package container

import (
	"context"
	"microservice/cmd/config"
	healthUseCase "microservice/internal/health/application/usecase"
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
	healthUC := healthUseCase.NewHealthUseCase(healthRepo)
	healthHdlr := handler.NewHealthHandler(healthUC)

	// Order repository
	orderRepo := orderInfra.NewPostgresOrderRepository(db)

	// AMQP client and publisher. A broker outage must not prevent the API from
	// starting: orders are still persisted and reads keep working.
	var (
		amqpClient     *amqp.Client
		orderPublisher port.OrderCreatedPublisher
		queueMgr       *queue.QueueManager
		orderEvtPub    *eventpublisher.OrderEventPublisher
	)

	client, err := amqp.NewClient(cfg.RabbitMQHost, cfg.RabbitMQPort, cfg.RabbitMQUser, cfg.RabbitMQPassword, log)
	if err != nil {
		log.Error("RabbitMQ unavailable at startup; event publication is disabled", zap.Error(err))
		orderPublisher = eventpublisher.NewUnavailablePublisher(log)
	} else if err := client.EnablePublisherConfirms(); err != nil {
		log.Error("failed to enable publisher confirms; event publication is disabled", zap.Error(err))
		_ = client.Close()
		orderPublisher = eventpublisher.NewUnavailablePublisher(log)
	} else {
		setupCtx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.RequestTimeoutSeconds)*time.Second)
		mgr := queue.NewQueueManager(client.Channel(), cfg, log)
		if err := mgr.SetupOrderQueues(setupCtx); err != nil {
			cancel()
			log.Error("failed to setup RabbitMQ queues; event publication is disabled", zap.Error(err))
			_ = client.Close()
			orderPublisher = eventpublisher.NewUnavailablePublisher(log)
		} else {
			cancel()
			amqpClient = client
			queueMgr = mgr
			orderEvtPub = eventpublisher.NewOrderEventPublisher(client, cfg, log)
			orderPublisher = orderEvtPub
		}
	}

	createOrderUC := orderUseCase.NewCreateOrderUseCase(orderRepo, orderPublisher, log)
	getOrderUC := orderUseCase.NewGetOrderUseCase(orderRepo)
	orderHdlr := orderHandler.NewOrderHandler(createOrderUC, getOrderUC, log)

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
