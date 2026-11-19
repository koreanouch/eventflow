package amqp

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

// Client represents an AMQP connection and channel
type Client struct {
	conn                 *amqp.Connection
	channel              *amqp.Channel
	log                  *zap.Logger
	returns              <-chan amqp.Return
	returnedMu           sync.Mutex
	returnedIDs          map[string]amqp.Return
	confirmationsStarted atomic.Bool
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
		conn:        conn,
		channel:     ch,
		log:         log,
		returnedIDs: make(map[string]amqp.Return),
	}

	log.Info("Connected to RabbitMQ", zap.String("host", host), zap.Int("port", port))

	return client, nil
}

func (c *Client) Channel() *amqp.Channel {
	return c.channel
}

func (c *Client) Close() error {
	var errCh, errConn error

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

	// basic.return notifications are drained synchronously by PublishWithConfirm
	// so that unroutable mandatory messages can be reported to the caller.
	c.returns = c.channel.NotifyReturn(make(chan amqp.Return, 128))
	c.confirmationsStarted.Store(true)

	return nil
}

// PublishWithConfirm publishes a message and waits for RabbitMQ confirmation.
// The confirmation is tracked by the amqp091-go deferred confirmation API, which
// allocates the delivery tag under a lock and is therefore safe for concurrent
// publishers sharing the same channel.
//
// Returns nil on ACK. Returns an error on NACK, timeout, publish failure or when
// a mandatory message cannot be routed (basic.return).
func (c *Client) PublishWithConfirm(ctx context.Context, exchange, routingKey string, mandatory bool, msg amqp.Publishing) error {
	if !c.confirmationsStarted.Load() {
		return fmt.Errorf("publisher confirms not enabled")
	}

	// MessageId is required to correlate basic.return notifications.
	if msg.MessageId == "" {
		msg.MessageId = uuid.NewString()
	}

	conf, err := c.channel.PublishWithDeferredConfirmWithContext(ctx, exchange, routingKey, mandatory, false, msg)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	acked, err := conf.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("publisher confirmation timeout: %w", err)
	}
	if !acked {
		return fmt.Errorf("message not acknowledged by RabbitMQ (NACK)")
	}

	// basic.return is emitted before the confirmation on the same channel, so
	// the return is already queued once WaitContext finishes.
	if ret, returned := c.takeReturned(msg.MessageId); returned {
		return fmt.Errorf("message returned by RabbitMQ as unroutable (code=%d, reason=%q, exchange=%s, routing_key=%s)",
			ret.ReplyCode, ret.ReplyText, ret.Exchange, ret.RoutingKey)
	}

	return nil
}

// takeReturned drains pending basic.return notifications and reports whether the
// message with the given ID was returned. Drain and lookup happen under the same
// lock so concurrent publishers cannot miss a return handled by another goroutine.
func (c *Client) takeReturned(messageID string) (amqp.Return, bool) {
	c.returnedMu.Lock()
	defer c.returnedMu.Unlock()

	for {
		select {
		case ret, ok := <-c.returns:
			if !ok {
				return amqp.Return{}, false
			}
			c.log.Warn("Message returned by RabbitMQ (not routable)",
				zap.Uint16("code", ret.ReplyCode),
				zap.String("reason", ret.ReplyText),
				zap.String("exchange", ret.Exchange),
				zap.String("routing_key", ret.RoutingKey),
				zap.String("message_id", ret.MessageId))
			if ret.MessageId != "" {
				c.returnedIDs[ret.MessageId] = ret
			}
		default:
			ret, returned := c.returnedIDs[messageID]
			if returned {
				delete(c.returnedIDs, messageID)
			}
			return ret, returned
		}
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
