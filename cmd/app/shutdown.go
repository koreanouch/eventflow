package app

import (
	"microservice/pkg/logger"
	"time"
)

func (a *App) Shutdown() {

	// Cerramos conexiones globales
	// Cierre DB, Colas, recursos , etc...

	logger.General("Cerrando conexiones y recursos...")

	if a.container.AMQPClient != nil {
		if err := a.container.AMQPClient.Close(); err != nil {
			logger.Error("Error al cerrar cliente AMQP: " + err.Error())
		}
	}

	if a.db != nil {
		a.db.Close()
	}

	time.Sleep(100 * time.Millisecond)

	logger.General("Recursos cerrados correctamente")
}
