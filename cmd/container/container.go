package container

import (
	"context"
	"microservice/cmd/config"
	healthUseCase "microservice/internal/health/application/usecase"
	healthDomain "microservice/internal/health/domain/repository"
	healthInfra "microservice/internal/health/infrastructure"
	"microservice/internal/health/presentation/handler"
	"microservice/internal/order/application/port"
	orderUseCase "microservice/internal/order/application/usecase"
	orderInfra "microservice/internal/order/infrastructure"
	"microservice/internal/order/infrastructure/eventpublisher"
	"microservice/internal/order/infrastructure/queue"
	orderHandler "microservice/internal/order/presentation/handler"
	"microservice/pkg/amqp"
	"microservice/pkg/database"
	"microservice/pkg/logger"
	"time"

	amqplib "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type Container struct {
	DB                  database.Database
	HealthHandler       *handler.HealthHandler
	OrderHandler        *orderHandler.OrderHandler
	CreateOrderUseCase  *orderUseCase.CreateOrderUseCase
	GetOrderUseCase     *orderUseCase.GetOrderUseCase
	AMQPClient          *amqp.Client
	OrderEventPublisher *eventpublisher.OrderEventPublisher
	QueueManager        *queue.QueueManager
	Log                 *zap.Logger
}

func NewContainer(cfg *config.Configuration, db database.Database, log *zap.Logger) (*Container, error) {

	// Health dependencies
	healthRepo := healthInfra.NewPostgresHealthRepository(db)

	// Order repository
	orderRepo := orderInfra.NewPostgresOrderRepository(db)

	// AMQP client and publisher. A broker outage must not prevent the API from
	// starting: orders are still persisted and reads keep working. El cliente es
	// perezoso y se reconecta solo, de modo que una caída temporal del broker no
	// deja la publicación deshabilitada para siempre.
	var (
		amqpClient     *amqp.Client
		orderPublisher port.OrderCreatedPublisher
		queueMgr       *queue.QueueManager
		orderEvtPub    *eventpublisher.OrderEventPublisher
	)

	client := amqp.NewLazyClient(cfg.RabbitMQHost, cfg.RabbitMQPort, cfg.RabbitMQUser, cfg.RabbitMQPassword, log)
	if err := client.EnablePublisherConfirms(); err != nil {
		log.Error("failed to configure publisher confirms", zap.Error(err))
		_ = client.Close()
		orderPublisher = eventpublisher.NewUnavailablePublisher(log)
	} else {
		// Al reconectar hay que restaurar la topología (exchanges, colas,
		// reintentos y DLQ) porque el canal nuevo no conoce lo declarado antes.
		client.OnReconnect(func(ch *amqplib.Channel) error {
			return queue.SetupOrderQueues(ch, cfg, log)
		})

		setupCtx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.RequestTimeoutSeconds)*time.Second)
		err := client.EnsureConnected(setupCtx)
		cancel()
		if err != nil {
			log.Error("RabbitMQ unavailable at startup; will retry on demand", zap.Error(err))
		}

		amqpClient = client
		queueMgr = queue.NewQueueManager(client.Channel(), cfg, log)
		orderEvtPub = eventpublisher.NewOrderEventPublisher(client, cfg, log)
		orderPublisher = orderEvtPub
	}

	createOrderUC := orderUseCase.NewCreateOrderUseCase(orderRepo, orderPublisher, log)
	getOrderUC := orderUseCase.NewGetOrderUseCase(orderRepo)
	orderHdlr := orderHandler.NewOrderHandler(createOrderUC, getOrderUC, log)

	var brokerChecker healthDomain.BrokerHealthChecker
	if amqpClient != nil {
		brokerChecker = healthInfra.NewAMQPHealthChecker(amqpClient)
	}
	healthUC := healthUseCase.NewHealthUseCase(healthRepo, brokerChecker)
	healthHdlr := handler.NewHealthHandler(healthUC)

	logger.Success("Container de dependencias inicializado...")

	return &Container{
		DB:                  db,
		HealthHandler:       healthHdlr,
		OrderHandler:        orderHdlr,
		CreateOrderUseCase:  createOrderUC,
		GetOrderUseCase:     getOrderUC,
		AMQPClient:          amqpClient,
		OrderEventPublisher: orderEvtPub,
		QueueManager:        queueMgr,
		Log:                 log,
	}, nil
}
