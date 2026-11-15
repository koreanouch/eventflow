package container

import (
	"context"
	"microservice/cmd/config"
	"microservice/internal/order/infrastructure/eventpublisher"
	"microservice/internal/order/infrastructure/queue"
	healthUseCase "microservice/internal/health/application/usecase"
	healthInfra "microservice/internal/health/infrastructure"
	"microservice/internal/health/presentation/handler"
	orderUseCase "microservice/internal/order/application/usecase"
	orderInfra "microservice/internal/order/infrastructure"
	orderHandler "microservice/internal/order/presentation/handler"
	"microservice/pkg/amqp"
	"microservice/pkg/database"
	"microservice/pkg/logger"

	"go.uber.org/zap"
)

type Container struct {
	DB                    database.Database
	HealthHandler         *handler.HealthHandler
	OrderHandler          *orderHandler.OrderHandler
	CreateOrderUseCase    *orderUseCase.CreateOrderUseCase
	GetOrderUseCase       *orderUseCase.GetOrderUseCase
	AMQPClient            *amqp.Client
	OrderEventPublisher   *eventpublisher.OrderEventPublisher
	QueueManager          *queue.QueueManager
	Log                   *zap.Logger
}

func NewContainer(cfg *config.Configuration, db database.Database, log *zap.Logger) (*Container, error) {

	// Health dependencies
	healthRepo := healthInfra.NewPostgresHealthRepository(db)
	healthUC := healthUseCase.NewHealthUseCase(healthRepo)
	healthHdlr := handler.NewHealthHandler(healthUC)

	// Order dependencies
	orderRepo := orderInfra.NewPostgresOrderRepository(db)
	createOrderUC := orderUseCase.NewCreateOrderUseCase(orderRepo)
	getOrderUC := orderUseCase.NewGetOrderUseCase(orderRepo)

	// AMQP client and publisher
	amqpClient, err := amqp.NewClient(cfg.RabbitMQHost, cfg.RabbitMQPort, cfg.RabbitMQUser, cfg.RabbitMQPassword, log)
	if err != nil {
		return nil, err
	}

	if err := amqpClient.EnablePublisherConfirms(); err != nil {
		amqpClient.Close()
		return nil, err
	}

	orderPublisher := eventpublisher.NewOrderEventPublisher(amqpClient, cfg, log)

	// Queue manager
	queueMgr := queue.NewQueueManager(amqpClient.Channel(), cfg, log)

	// Setup queues
	if err := queueMgr.SetupOrderQueues(context.Background()); err != nil {
		amqpClient.Close()
		return nil, err
	}

	orderHdlr := orderHandler.NewOrderHandler(createOrderUC, getOrderUC, orderPublisher, log)

	logger.Success("Container de dependencias inicializado...")

	return &Container{
		DB:                  db,
		HealthHandler:       healthHdlr,
		OrderHandler:        orderHdlr,
		CreateOrderUseCase:  createOrderUC,
		GetOrderUseCase:     getOrderUC,
		AMQPClient:          amqpClient,
		OrderEventPublisher: orderPublisher,
		QueueManager:        queueMgr,
		Log:                 log,
	}, nil
}
