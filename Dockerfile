# ================================
# STAGE 1 : Build
# ================================
FROM golang:1.27-alpine as builder
WORKDIR /app

# Instalar swag para generar documentación
RUN go install github.com/swaggo/swag/cmd/swag@latest

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Generar documentación Swagger
RUN swag init -g cmd/api/main.go

RUN CGO_ENABLED=0 GOOS=linux go build -o /app/microservice ./cmd/api

# ===============================
# STAGE 2: RUNTIME
# ===============================
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /app/microservice /microservice

EXPOSE 3200
ENTRYPOINT [ "/microservice" ]
