package config

import (
	"github.com/caarlos0/env/v6"
	"github.com/go-playground/validator/v10"
)

type Configuration struct {
	AppEnv                string `env:"ENVIRONMENT" envDefault:"dev" validate:"oneof= dev qa prod"`
	Port                  int    `env:"PORT" envDefault:"3200"`
	LogLevel              string `env:"LOG_LEVEL" envDefault:"info"`
	ShutDownTimeOut       int    `env:"SHUTDOWN_TIMEOUT" envDefault:"30"`
	RequestTimeoutSeconds int    `env:"REQUEST_TIMEOUT_SECONDS" envDefault:"10"`
	MaxBodyBytes          int64  `env:"MAX_BODY_BYTES" envDefault:"1048576"`
	DBHost                string `env:"DB_HOST" envDefault:"localhost"`
	DBPort                int    `env:"DB_PORT" envDefault:"5432"`
	DBUser                string `env:"DB_USER" envDefault:"postgres"`
	DBPassword            string `env:"DB_PASSWORD" envDefault:"postgres"`
	DBName                string `env:"DB_NAME" envDefault:"microservice"`
	DBSSLMode             string `env:"DB_SSL_MODE" envDefault:"disable"`
	RabbitMQHost          string `env:"RABBITMQ_HOST" envDefault:"localhost"`
	RabbitMQPort          int    `env:"RABBITMQ_PORT" envDefault:"5672"`
	RabbitMQUser          string `env:"RABBITMQ_USER" envDefault:"guest"`
	RabbitMQPassword      string `env:"RABBITMQ_PASSWORD" envDefault:"guest"`
	RabbitMQQueueOrders   string `env:"RABBITMQ_QUEUE_ORDERS" envDefault:"orders"`
	RabbitMQQueueRetry    string `env:"RABBITMQ_QUEUE_ORDERS_RETRY" envDefault:"orders.retry"`
	RabbitMQQueueDLQ      string `env:"RABBITMQ_QUEUE_ORDERS_DLQ" envDefault:"orders.dlq"`
	RabbitMQExchange      string `env:"RABBITMQ_EXCHANGE_ORDERS" envDefault:"orders.exchange"`
	RabbitMQRoutingKey    string `env:"RABBITMQ_ROUTING_KEY_ORDER_CREATED" envDefault:"order.created"`
	RabbitMQMaxRetries    int    `env:"RABBITMQ_MAX_RETRIES" envDefault:"3"`
	RabbitMQRetryDelayMs  int    `env:"RABBITMQ_RETRY_DELAY_MS" envDefault:"5000"`
}

func LoadVars() (*Configuration, error) {
	cfg := &Configuration{}

	if err := env.Parse(cfg); err != nil {
		return nil, err
	}

	v := validator.New()

	if err := v.Struct(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}
