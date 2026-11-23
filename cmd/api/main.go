// @title Microservice API
// @version 1.0
// @description API de ejemplo basada en Clean Architecture
// @host localhost:3200
// @BasePath /api/v1
package main

import (
	"fmt"
	"log"
	"microservice/cmd/app"
	"net/http"
	"os"
	"time"
)

func main() {
	// Subcomando usado por el healthcheck de Docker. La imagen distroless no
	// incluye shell ni curl, así que el propio binario comprueba el endpoint.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		if err := healthcheck(); err != nil {
			fmt.Fprintln(os.Stderr, "healthcheck failed:", err)
			os.Exit(1)
		}
		return
	}

	app := app.NewApp()

	if err := app.Init(); err != nil {
		log.Fatal("Error al iniciar: ", err)
	}

	if err := app.Run(); err != nil {
		log.Fatal("Error al ejecutar aplicativo: ", err)
	}
}

func healthcheck() error {
	port := os.Getenv("PORT")
	if port == "" {
		port = "3200"
	}

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%s/api/v1/health", port))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return nil
}
