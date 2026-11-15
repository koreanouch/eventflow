package amqp

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

// PublishResult represents the outcome of a publisher confirmation
type PublishResult struct {
	SeqNo uint64
	Ack   bool
	Nack  bool
}

// ConfirmationWaiter waits for a publisher confirmation from RabbitMQ
type ConfirmationWaiter struct {
	done   chan *PublishResult
	result *PublishResult
}

// Client represents an AMQP connection and channel
type Client struct {
	conn                 *amqp.Connection
	channel              *amqp.Channel
	log                  *zap.Logger
	closeDone            chan struct{}
	pendingConfirmations map[uint64]*ConfirmationWaiter
	confirmMutex         sync.RWMutex
	confirmationsStarted atomic.Bool
	handlersStarted      atomic.Bool
}

func NewClient(host string, port int, user, password string, log *zap.Logger) (*Client, error) {
	url := fmt.Sprintf("amqp://%s:%s@%s:%d/", user, password, host, port)

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	client := &Client{
		conn:                 conn,
		channel:              ch,
		log:                  log,
		closeDone:            make(chan struct{}),
		pendingConfirmations: make(map[uint64]*ConfirmationWaiter),
	}

	log.Info("Connected to RabbitMQ", zap.String("host", host), zap.Int("port", port))

	return client, nil
}

func (c *Client) Channel() *amqp.Channel {
	return c.channel
}

func (c *Client) Close() error {
	var errCh, errConn error

	close(c.closeDone)

	if c.channel != nil {
		errCh = c.channel.Close()
	}

	if c.conn != nil {
		errConn = c.conn.Close()
	}

	if errCh != nil {
		return fmt.Errorf("failed to close channel: %w", errCh)
	}
	if errConn != nil {
		return fmt.Errorf("failed to close connection: %w", errConn)
	}

	c.log.Info("Disconnected from RabbitMQ")
	return nil
}

func (c *Client) DeclareExchange(name, kind string, durable bool) error {
	err := c.channel.ExchangeDeclare(
		name,
		kind,
		durable,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return fmt.Errorf("failed to declare exchange %s: %w", name, err)
	}

	c.log.Debug("Exchange declared", zap.String("exchange", name))
	return nil
}

func (c *Client) DeclareQueue(name string, durable bool, args amqp.Table) (amqp.Queue, error) {
	q, err := c.channel.QueueDeclare(
		name,
		durable,
		false,
		false,
		false,
		args,
	)

	if err != nil {
		return amqp.Queue{}, fmt.Errorf("failed to declare queue %s: %w", name, err)
	}

	c.log.Debug("Queue declared", zap.String("queue", name))
	return q, nil
}

func (c *Client) BindQueue(queue, exchange, routingKey string) error {
	err := c.channel.QueueBind(
		queue,
		routingKey,
		exchange,
		false,
		nil,
	)

	if err != nil {
		return fmt.Errorf("failed to bind queue %s to exchange %s: %w", queue, exchange, err)
	}

	c.log.Debug("Queue bound", zap.String("queue", queue), zap.String("exchange", exchange), zap.String("routingKey", routingKey))
	return nil
}

func (c *Client) Consume(queue string, consumerTag string, autoAck bool, exclusive bool, noLocal bool, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	deliveries, err := c.channel.Consume(
		queue,
		consumerTag,
		autoAck,
		exclusive,
		noLocal,
		noWait,
		args,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to start consuming from queue %s: %w", queue, err)
	}

	c.log.Debug("Started consuming from queue", zap.String("queue", queue))
	return deliveries, nil
}

func (c *Client) Qos(prefetchCount, prefetchSize int) error {
	err := c.channel.Qos(prefetchCount, prefetchSize, false)
	if err != nil {
		return fmt.Errorf("failed to set QoS: %w", err)
	}
	return nil
}

func (c *Client) EnablePublisherConfirms() error {
	if c.confirmationsStarted.Load() {
		return nil
	}

	err := c.channel.Confirm(false)
	if err != nil {
		return fmt.Errorf("failed to enable publisher confirms: %w", err)
	}

	c.confirmationsStarted.Store(true)

	if !c.handlersStarted.Load() {
		go c.handleConfirmations()
		c.handlersStarted.Store(true)
	}

	return nil
}

func (c *Client) handleConfirmations() {
	confirms := c.channel.NotifyPublish(make(chan amqp.Confirmation, 100))
	returns := c.channel.NotifyReturn(make(chan amqp.Return, 100))

	for {
		select {
		case <-c.closeDone:
			// Drain remaining confirmations to avoid goroutine leak
			for {
				select {
				case <-confirms:
				case <-returns:
				default:
					return
				}
			}

		case ret, ok := <-returns:
			if !ok {
				return
			}
			// Log the returned message
			// Note: basicReturn in amqp091-go doesn't include delivery tag,
			// so we can't correlate directly with a pending confirmation.
			// The publisher should expect a NACK to follow if mandatory=true.
			c.log.Warn("Message returned by RabbitMQ (not routable)",
				zap.Uint16("code", ret.ReplyCode),
				zap.String("reason", ret.ReplyText),
				zap.String("exchange", ret.Exchange),
				zap.String("routing_key", ret.RoutingKey))

		case conf, ok := <-confirms:
			if !ok {
				return
			}

			c.confirmMutex.Lock()
			waiter, exists := c.pendingConfirmations[conf.DeliveryTag]
			if exists && waiter.result != nil {
				waiter.result.Ack = conf.Ack
				waiter.result.Nack = !conf.Ack
				close(waiter.done)
			}
			c.confirmMutex.Unlock()
		}
	}
}

// PublishWithConfirm publishes a message and waits for RabbitMQ confirmation.
// The correlation is done using GetNextPublishSeqNo() to obtain the real delivery tag from RabbitMQ.
// Returns nil on ACK or an error on NACK, timeout, or publish failure.
// Note: If mandatory=true and the message is not routable, RabbitMQ will send a basic.return
// (logged as a warning) followed by a NACK, which will be returned as an error.
func (c *Client) PublishWithConfirm(ctx context.Context, exchange, routingKey string, mandatory bool, msg amqp.Publishing) error {
	if !c.confirmationsStarted.Load() {
		return fmt.Errorf("publisher confirms not enabled")
	}

	// Get the next publish sequence number from RabbitMQ
	// This is the actual delivery tag that will appear in confirmations
	seqNo := c.channel.GetNextPublishSeqNo()

	// Create a waiter for this specific publish
	waiter := &ConfirmationWaiter{
		done: make(chan *PublishResult, 1),
		result: &PublishResult{
			SeqNo: seqNo,
		},
	}

	c.confirmMutex.Lock()
	c.pendingConfirmations[seqNo] = waiter
	c.confirmMutex.Unlock()

	// Publish the message
	err := c.channel.PublishWithContext(ctx, exchange, routingKey, mandatory, false, msg)
	if err != nil {
		c.confirmMutex.Lock()
		delete(c.pendingConfirmations, seqNo)
		c.confirmMutex.Unlock()
		return fmt.Errorf("failed to publish message: %w", err)
	}

	// Wait for confirmation or context cancellation
	select {
	case <-waiter.done:
		// Got confirmation (ACK or NACK)
		result := waiter.result

		c.confirmMutex.Lock()
		delete(c.pendingConfirmations, seqNo)
		c.confirmMutex.Unlock()

		if result.Nack {
			return fmt.Errorf("message not acknowledged by RabbitMQ (NACK)")
		}

		if result.Ack {
			return nil
		}

		// Should not reach here, but safeguard
		return fmt.Errorf("unexpected confirmation state for delivery tag %d", seqNo)

	case <-ctx.Done():
		c.confirmMutex.Lock()
		delete(c.pendingConfirmations, seqNo)
		c.confirmMutex.Unlock()
		return fmt.Errorf("publisher confirmation timeout: %w", ctx.Err())
	}
}

func (c *Client) PublishWithContextBasic(exchange, routingKey string, mandatory bool, msg amqp.Publishing) error {
	err := c.channel.Publish(
		exchange,
		routingKey,
		mandatory,
		false,
		msg,
	)

	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}
