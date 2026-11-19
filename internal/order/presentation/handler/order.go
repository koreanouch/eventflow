package handler

import (
	"errors"
	"fmt"
	"microservice/internal/order/application/dto"
	"microservice/internal/order/application/usecase"
	"microservice/internal/order/domain/event"
	"microservice/internal/order/infrastructure/eventpublisher"
	"microservice/pkg/apperrors"
	"microservice/pkg/httputil"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type OrderHandler struct {
	createOrderUC  *usecase.CreateOrderUseCase
	getOrderUC     *usecase.GetOrderUseCase
	eventPublisher eventpublisher.Publisher
	log            *zap.Logger
}

func NewOrderHandler(createOrderUC *usecase.CreateOrderUseCase, getOrderUC *usecase.GetOrderUseCase, eventPublisher eventpublisher.Publisher, log *zap.Logger) *OrderHandler {
	return &OrderHandler{
		createOrderUC:  createOrderUC,
		getOrderUC:     getOrderUC,
		eventPublisher: eventPublisher,
		log:            log,
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
// @Failure 400 {object} httputil.ErrorResponse
// @Failure 409 {object} httputil.ErrorResponse
// @Failure 413 {object} httputil.ErrorResponse
// @Failure 500 {object} httputil.ErrorResponse
// @Router /orders [post]
func (h *OrderHandler) CreateOrder(c *gin.Context) {
	var req dto.CreateOrderRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		// El detalle del binding se queda en el log: nunca se devuelve al cliente.
		h.log.Warn("invalid create order payload", zap.Error(err), zap.String("path", c.FullPath()))

		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			h.respondError(c, apperrors.NewPayloadTooLargeError("REQUEST_TOO_LARGE", "request body is too large"))
			return
		}

		h.respondError(c, apperrors.NewValidationError("REQUEST_INVALID", "invalid request body"))
		return
	}

	if validationErr := validateCreateOrderRequest(&req); validationErr != nil {
		h.respondError(c, apperrors.NewValidationError("ORDER_INVALID", validationErr.Error()))
		return
	}

	res, err := h.createOrderUC.Execute(c.Request.Context(), &req, c.GetHeader("Idempotency-Key"))
	if err != nil {
		h.respondError(c, err)
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

// respondError traduce el error a una respuesta HTTP consistente.
// Los errores 5xx solo exponen un mensaje genérico; el detalle queda en los logs.
func (h *OrderHandler) respondError(c *gin.Context, err error) {
	status, body := httputil.MapErrorToHttp(err)

	if status >= http.StatusInternalServerError {
		h.log.Error("request failed",
			zap.String("method", c.Request.Method),
			zap.String("path", c.Request.URL.Path),
			zap.Error(err),
		)
	}

	c.JSON(status, body)
}

func validateCreateOrderRequest(req *dto.CreateOrderRequest) error {
	if strings.TrimSpace(req.CustomerID) == "" {
		return errors.New("customer_id is required")
	}

	if len(req.Items) == 0 {
		return errors.New("items is required and must contain at least one item")
	}

	for i, item := range req.Items {
		if strings.TrimSpace(item.ProductID) == "" {
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
// @Failure 404 {object} httputil.ErrorResponse
// @Failure 500 {object} httputil.ErrorResponse
// @Router /orders/{id} [get]
func (h *OrderHandler) GetOrder(c *gin.Context) {
	res, err := h.getOrderUC.Execute(c.Request.Context(), c.Param("id"))
	if err != nil {
		h.respondError(c, err)
		return
	}

	c.JSON(http.StatusOK, res)
}
