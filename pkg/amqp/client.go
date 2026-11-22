package amqp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

// ErrClientClosed se devuelve cuando se intenta usar un cliente ya cerrado.
var ErrClientClosed = errors.New("rabbitmq client is closed")

const defaultDialTimeout = 10 * time.Second

// Client representa una conexión y un canal AMQP con recuperación automática.
//
// Si el broker se reinicia o la conexión se cae, el cliente vuelve a conectar de
// forma perezosa (al publicar, consumir o comprobar la salud) y reejecuta los
// hooks registrados con OnReconnect para restaurar la topología y el QoS.
type Client struct {
	// mu serializa los cambios de conexión y protege el canal canal durante la
	// publicación, de modo que una reconexión no cierre el canal en mitad de un
	// publish.
	mu sync.Mutex

	url     string
	host    string
	port    int
	log     *zap.Logger
	conn    *amqp.Connection
	channel *amqp.Channel

	returns              <-chan amqp.Return
	returnedMu           sync.Mutex
	returnedIDs          map[string]amqp.Return
	confirmationsStarted atomic.Bool
	confirmsDesired      bool
	setupHooks           []func(*amqp.Channel) error
	closed               bool
}

// NewClient establece la conexión inicial. Devuelve error si RabbitMQ no está
// disponible en ese momento.
func NewClient(host string, port int, user, password string, log *zap.Logger) (*Client, error) {
	client := NewLazyClient(host, port, user, password, log)

	ctx, cancel := context.WithTimeout(context.Background(), defaultDialTimeout)
	defer cancel()

	if err := client.EnsureConnected(ctx); err != nil {
		return nil, err
	}

	return client, nil
}

// NewLazyClient crea el cliente sin conectar. La primera conexión se realiza al
// llamar a EnsureConnected, publicar, consumir o comprobar la salud. Esto permite
// arrancar la API aunque RabbitMQ todavía no esté disponible y recuperarse solo.
func NewLazyClient(host string, port int, user, password string, log *zap.Logger) *Client {
	if log == nil {
		log = zap.NewNop()
	}

	return &Client{
		url:         fmt.Sprintf("amqp://%s:%s@%s:%d/", user, password, host, port),
		host:        host,
		port:        port,
		log:         log,
		returnedIDs: make(map[string]amqp.Return),
	}
}

// OnReconnect registra una función que se ejecuta cada vez que se establece una
// conexión (inicial o posterior a una caída). Se usa para redeclarar exchanges y
// colas, reaplicar QoS o cualquier otro estado ligado al canal.
func (c *Client) OnReconnect(fn func(*amqp.Channel) error) {
	if fn == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.setupHooks = append(c.setupHooks, fn)
}

// EnsureConnected garantiza que hay una conexión y un canal abiertos,
// reconectando si es necesario.
func (c *Client) EnsureConnected(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.ensureConnectedLocked(ctx)
}

func (c *Client) ensureConnectedLocked(ctx context.Context) error {
	if c.closed {
		return ErrClientClosed
	}

	if c.conn != nil && !c.conn.IsClosed() && c.channel != nil && !c.channel.IsClosed() {
		return nil
	}

	return c.connectLocked(ctx)
}

func (c *Client) connectLocked(ctx context.Context) error {
	if c.closed {
		return ErrClientClosed
	}

	// Descartar una conexión rota antes de reemplazarla.
	if c.channel != nil {
		_ = c.channel.Close()
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
	c.channel = nil
	c.conn = nil
	c.confirmationsStarted.Store(false)

	dialer := &net.Dialer{}
	conn, err := amqp.DialConfig(c.url, amqp.Config{
		Dial: func(network, addr string) (net.Conn, error) {
			return dialer.DialContext(ctx, network, addr)
		},
	})
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("failed to open channel: %w", err)
	}

	c.conn = conn
	c.channel = ch

	// Restaurar publisher confirms antes de los hooks, porque algunos hooks
	// (por ejemplo la redeclaración de la topología) usan el canal directamente.
	if c.confirmsDesired {
		if err := c.startConfirmsLocked(); err != nil {
			_ = ch.Close()
			_ = conn.Close()
			c.channel = nil
			c.conn = nil
			return err
		}
	}

	for _, hook := range c.setupHooks {
		if err := hook(ch); err != nil {
			_ = ch.Close()
			_ = conn.Close()
			c.channel = nil
			c.conn = nil
			return fmt.Errorf("failed to restore RabbitMQ topology: %w", err)
		}
	}

	c.log.Info("Connected to RabbitMQ", zap.String("host", c.host), zap.Int("port", c.port))
	return nil
}

func (c *Client) startConfirmsLocked() error {
	if c.channel == nil || c.channel.IsClosed() {
		return fmt.Errorf("rabbitmq channel is closed")
	}

	if err := c.channel.Confirm(false); err != nil {
		return fmt.Errorf("failed to enable publisher confirms: %w", err)
	}

	// basic.return notifications are drained synchronously by PublishWithConfirm
	// so that unroutable mandatory messages can be reported to the caller.
	c.returns = c.channel.NotifyReturn(make(chan amqp.Return, 128))
	c.confirmationsStarted.Store(true)

	return nil
}

func (c *Client) Channel() *amqp.Channel {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.channel
}

// Ping comprueba el estado actual sin intentar reconectar.
func (c *Client) Ping() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.pingLocked()
}

// PingContext comprueba la salud intentando reconectar si la conexión se perdió.
// Es lo que usa el readiness para que el servicio vuelva a "ok" cuando el broker
// regresa, sin esperar a la siguiente publicación.
func (c *Client) PingContext(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureConnectedLocked(ctx); err != nil {
		return err
	}

	return c.pingLocked()
}

func (c *Client) pingLocked() error {
	if c.conn == nil || c.conn.IsClosed() {
		return fmt.Errorf("rabbitmq connection is closed")
	}
	if c.channel == nil || c.channel.IsClosed() {
		return fmt.Errorf("rabbitmq channel is closed")
	}
	return nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

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
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.channel == nil || c.channel.IsClosed() {
		return fmt.Errorf("rabbitmq channel is closed")
	}

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
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.channel == nil || c.channel.IsClosed() {
		return amqp.Queue{}, fmt.Errorf("rabbitmq channel is closed")
	}

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
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.channel == nil || c.channel.IsClosed() {
		return fmt.Errorf("rabbitmq channel is closed")
	}

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
	ctx, cancel := context.WithTimeout(context.Background(), defaultDialTimeout)
	defer cancel()

	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureConnectedLocked(ctx); err != nil {
		return nil, err
	}

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
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.channel == nil || c.channel.IsClosed() {
		return fmt.Errorf("rabbitmq channel is closed")
	}

	err := c.channel.Qos(prefetchCount, prefetchSize, false)
	if err != nil {
		return fmt.Errorf("failed to set QoS: %w", err)
	}
	return nil
}

// EnablePublisherConfirms activa los confirms y los reaplica automáticamente en
// cada reconexión. Puede llamarse antes de que exista conexión (cliente lazy).
func (c *Client) EnablePublisherConfirms() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.confirmsDesired = true

	if c.channel == nil || c.channel.IsClosed() {
		return nil
	}
	if c.confirmationsStarted.Load() {
		return nil
	}

	return c.startConfirmsLocked()
}

// PublishWithConfirm publishes a message and waits for RabbitMQ confirmation.
// The confirmation is tracked by the amqp091-go deferred confirmation API, which
// allocates the delivery tag under a lock and is therefore safe for concurrent
// publishers sharing the same channel.
//
// Si la conexión está caída intenta reconectar antes de publicar, y si falla a
// mitad del envío reintenta una vez tras restaurar la conexión.
//
// Returns nil on ACK. Returns an error on NACK, timeout, publish failure or when
// a mandatory message cannot be routed (basic.return).
func (c *Client) PublishWithConfirm(ctx context.Context, exchange, routingKey string, mandatory bool, msg amqp.Publishing) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.confirmsDesired {
		return fmt.Errorf("publisher confirms not enabled")
	}

	if err := c.ensureConnectedLocked(ctx); err != nil {
		return err
	}

	err := c.publishLocked(ctx, exchange, routingKey, mandatory, msg)
	if err == nil {
		return nil
	}

	// Si el canal murió a mitad del publish, la conexión está rota y conviene un
	// único reintento tras reconectar. Cualquier otro error (NACK, timeout,
	// mensaje no ruteable) se devuelve tal cual.
	if ctx.Err() != nil || !c.connectionBrokenLocked() {
		return err
	}

	c.log.Warn("RabbitMQ connection lost during publish; reconnecting and retrying",
		zap.String("exchange", exchange),
		zap.String("routing_key", routingKey),
		zap.Error(err),
	)

	if reconnectErr := c.connectLocked(ctx); reconnectErr != nil {
		return err
	}

	return c.publishLocked(ctx, exchange, routingKey, mandatory, msg)
}

func (c *Client) connectionBrokenLocked() bool {
	return c.conn == nil || c.conn.IsClosed() || c.channel == nil || c.channel.IsClosed()
}

func (c *Client) publishLocked(ctx context.Context, exchange, routingKey string, mandatory bool, msg amqp.Publishing) error {
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
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.channel == nil || c.channel.IsClosed() {
		return fmt.Errorf("rabbitmq channel is closed")
	}

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
