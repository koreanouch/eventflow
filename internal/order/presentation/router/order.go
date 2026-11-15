package router

import (
	"microservice/internal/order/presentation/handler"

	"github.com/gin-gonic/gin"
)

func RegisterRoutes(api *gin.RouterGroup, h *handler.OrderHandler) {
	v1 := api.Group("/v1/orders")

	v1.POST("", h.CreateOrder)
	v1.GET("/:id", h.GetOrder)
}
