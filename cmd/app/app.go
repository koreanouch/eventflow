package app

import (
	"context"
	"microservice/cmd/config"
	"microservice/cmd/container"
	"microservice/cmd/routes"
	"microservice/pkg/database"
	"microservice/pkg/logger"
	"microservice/pkg/metrics"
	"microservice/pkg/middleware"
	"os"
	"time"

	_ "microservice/docs"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"go.uber.org/zap"
)

type App struct {
	config    *config.Configuration
	container *container.Container
	router    *gin.Engine
	log       *zap.Logger
	db        database.Database
}

func NewApp() *App {
	return &App{}
}

func (a *App) Init() error {
	env := os.Getenv("ENVIRONMENT")

	if env == "" {
		env = "dev"
	}

	config.LoadEnv(env)

	logger.Init(env)
	a.log = logger.L()
	logger.General("==== Microservice Start ====")

	cfg, err := config.LoadVars()
	if err != nil {
		return err
	}

	a.config = cfg

	logger.Success("Configuración cargada correctamente")
	logger.General("Servidor Configurado")

	if a.config.AppEnv == "prod" {
		gin.SetMode(gin.ReleaseMode)
	}

	router := gin.New()
	if a.config.AppEnv != "prod" {
		router.GET("/docs/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	}

	// Métricas en formato Prometheus. Se registra antes de los middlewares de
	// negocio para que el scraping no se mezcle con el tráfico de la API ni
	// genere ruido en los logs de acceso.
	router.GET("/metrics", func(c *gin.Context) {
		c.Header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_ = metrics.Default().WritePrometheus(c.Writer)
	})

	// Aplicamos Middlewares
	router.Use(middleware.RequestID())
	router.Use(middleware.Metrics())
	router.Use(middleware.AccessLog(a.log))

	router.Use(middleware.ErrorHandlerMiddleware(a.log)) // Global Error Handler

	router.Use(gin.Recovery())

	// Middleware Cors
	router.Use(cors.Default())

	// Middleware Helmet
	router.Use(middleware.SecurityHeaders())

	// Límite de tamaño de body y deadline por petición
	router.Use(middleware.Limits(a.config.MaxBodyBytes, time.Duration(a.config.RequestTimeoutSeconds)*time.Second))

	// Otros middlewares
	//...

	// Base de datos
	db, err := database.NewPostgresDatabase(context.Background(), database.PostgresConfig{
		Host:     cfg.DBHost,
		Port:     cfg.DBPort,
		User:     cfg.DBUser,
		DBName:   cfg.DBName,
		Password: cfg.DBPassword,
		SSLMode:  cfg.DBSSLMode,
	})

	if err != nil {
		return err
	}

	a.db = db

	c, err := container.NewContainer(a.config, db, a.log)
	if err != nil {
		return err
	}
	a.container = c

	apiRouter := routes.NewAPIRouter(a.container)
	apiRouter.RegisterRoutes(router)

	a.router = router

	return nil
}
