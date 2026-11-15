package handler

import (
	"errors"
	"fmt"
	"microservice/internal/order/application/dto"
	"microservice/internal/order/application/usecase"
	"microservice/internal/order/domain/event"
	"microservice/internal/order/domain/repository"
	"microservice/internal/order/infrastructure/eventpublisher"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type OrderHandler struct {
	createOrderUC    *usecase.CreateOrderUseCase
	getOrderUC       *usecase.GetOrderUseCase
	eventPublisher   eventpublisher.Publisher
	log              *zap.Logger
}

func NewOrderHandler(createOrderUC *usecase.CreateOrderUseCase, getOrderUC *usecase.GetOrderUseCase, eventPublisher eventpublisher.Publisher, log *zap.Logger) *OrderHandler {
	return &OrderHandler{
		createOrderUC:   createOrderUC,
		getOrderUC:      getOrderUC,
		eventPublisher:  eventPublisher,
		log:             log,
	}
}

// @Summary Create Order
// @Description Creates a new order with the provided customer ID and items
// @Tags Orders
// @Accept json
// @Produce json
// @Param request body dto.CreateOrderRequest true "Create Order Request"
// @Param Idempotency-Key header string true "Idempotency key (maximum 255 bytes)"
// @Success 201 {object} dto.CreateOrderResponse
// @Failure 400 {object} map[string]string
// @Failure 409 {object} map[string]string
// @Failure 500 {object} map[string]string
// @Router /orders [post]
func (h *OrderHandler) CreateOrder(c *gin.Context) {
	var req dto.CreateOrderRequest

	key := c.GetHeader("Idempotency-Key")
	if strings.TrimSpace(key) == "" || len(key) > 255 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "idempotency key must be nonempty and at most 255 bytes"})
		return
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if validationErr := validateCreateOrderRequest(&req); validationErr != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": validationErr.Error()})
		return
	}

	res, err := h.createOrderUC.Execute(c.Request.Context(), &req, key)
	if err != nil {
		if errors.Is(err, repository.ErrIdempotencyConflict) {
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Publish OrderCreated event after successful creation
	// Note: If this publication fails, the order is already persisted
	evt := event.NewOrderCreated(res.ID, req.CustomerID, calculateTotalAmount(&req))
	if err := h.eventPublisher.PublishOrderCreated(c.Request.Context(), evt); err != nil {
		h.log.Error("Failed to publish order created event, but order was persisted",
			zap.String("order_id", res.ID),
			zap.Error(err),
		)
		// Still return 201 because the order was successfully created
		// The event publication failure is logged for later investigation
	}

	c.JSON(http.StatusCreated, res)
}

func validateCreateOrderRequest(req *dto.CreateOrderRequest) error {
	if req.CustomerID == "" {
		return errors.New("customer_id is required")
	}

	if len(req.Items) == 0 {
		return errors.New("items is required and must contain at least one item")
	}

	for i, item := range req.Items {
		if item.ProductID == "" {
			return fmt.Errorf("items[%d].product_id is required", i)
		}

		if item.Quantity <= 0 {
			return fmt.Errorf("items[%d].quantity must be greater than 0", i)
		}

		if item.UnitPrice < 0 {
			return fmt.Errorf("items[%d].unit_price must be greater than or equal to 0", i)
		}
	}

	return nil
}

func calculateTotalAmount(req *dto.CreateOrderRequest) float64 {
	total := 0.0
	for _, item := range req.Items {
		total += float64(item.UnitPrice) * float64(item.Quantity)
	}
	return total
}

// @Summary Get Order
// @Description Retrieves an order by ID with its items, total, status, and creation date
// @Tags Orders
// @Accept json
// @Produce json
// @Param id path string true "Order ID"
// @Success 200 {object} dto.GetOrderResponse
// @Failure 404 {object} map[string]string
// @Failure 500 {object} map[string]string
// @Router /orders/{id} [get]
func (h *OrderHandler) GetOrder(c *gin.Context) {
	orderID := c.Param("id")

	res, err := h.getOrderUC.Execute(c.Request.Context(), orderID)
	if err != nil {
		if errors.Is(err, usecase.ErrOrderNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "order not found"})
			return
		}

		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, res)
}
