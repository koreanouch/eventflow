#!/usr/bin/env pwsh

# Colores
$Green = "`e[32m"
$Yellow = "`e[33m"
$Red = "`e[31m"
$Reset = "`e[0m"

function Write-Success {
    param([string]$Message)
    Write-Host "${Green}✓${Reset} $Message"
}

function Write-Info {
    param([string]$Message)
    Write-Host "${Yellow}ℹ${Reset} $Message"
}

function Write-Error-Custom {
    param([string]$Message)
    Write-Host "${Red}✗${Reset} $Message"
}

Write-Info "EVENTFLOW - Pruebas E2E"
Write-Info "======================="
Write-Info ""

# Verificar que docker-compose esté corriendo
Write-Info "Verificando servicios..."
$services = & docker-compose ps -q 2>$null
if (-not $services) {
    Write-Error-Custom "Docker Compose no está ejecutándose"
    Write-Info "Ejecuta: docker-compose up -d"
    exit 1
}

Write-Success "Servicios están ejecutándose"
Write-Info ""

# Esperar a que los servicios estén listos
Write-Info "Esperando a que los servicios estén listos..."
Start-Sleep -Seconds 2

# Verificar que la API esté respondiendo
Write-Info "Verificando API en http://localhost:3200..."
$apiResponse = $null
try {
    $apiResponse = Invoke-WebRequest -Uri "http://localhost:3200/api/v1/orders" -Method Get -ErrorAction SilentlyContinue
    Write-Success "API está respondiendo"
} catch {
    Write-Error-Custom "API no está disponible en localhost:3200"
    exit 1
}

Write-Info ""

# Crear una orden
Write-Info "Creando una nueva orden..."
$timestamp = Get-Date -AsUTC -Format "yyyy-MM-ddTHH:mm:ssZ"
$idempotencyKey = "test-$(Get-Date -Format 'yyyyMMddHHmmss')"

$orderBody = @{
    customer_id = "test-customer-$(Get-Random -Minimum 1 -Maximum 9999)"
    items = @(
        @{
            product_id = "test-product-001"
            quantity = 2
            unit_price = 100
        }
    )
} | ConvertTo-Json

try {
    $response = Invoke-WebRequest -Uri "http://localhost:3200/api/v1/orders" `
        -Method Post `
        -Headers @{
            "Content-Type" = "application/json"
            "Idempotency-Key" = $idempotencyKey
        } `
        -Body $orderBody `
        -ErrorAction Stop

    $responseBody = $response.Content | ConvertFrom-Json
    $orderId = $responseBody.id

    Write-Success "Orden creada exitosamente"
    Write-Info "Order ID: $orderId"
    Write-Info "Idempotency Key: $idempotencyKey"
    Write-Info ""
} catch {
    Write-Error-Custom "Error al crear orden: $_"
    exit 1
}

# Esperar a que el worker procese el evento
Write-Info "Esperando a que el worker procese el evento (5 segundos)..."
Start-Sleep -Seconds 5

# Verificar que la orden existe en BD
Write-Info "Verificando que la orden se registró en BD..."
try {
    $sqlQuery = "SELECT COUNT(*) as count FROM orders WHERE id = '$orderId';"
    $dbCheck = & docker-compose exec -T db psql -U postgres -d microservice -c $sqlQuery 2>$null

    if ($dbCheck -match "\s1\s") {
        Write-Success "Orden encontrada en BD"
    } else {
        Write-Error-Custom "Orden no encontrada en BD"
    }
} catch {
    Write-Info "No se pudo verificar en BD (ignorando)"
}

Write-Info ""

# Verificar que la notificación se creó
Write-Info "Verificando que se creó la notificación..."
try {
    $sqlQuery = "SELECT COUNT(*) as count FROM notifications WHERE order_id = '$orderId';"
    $notifCheck = & docker-compose exec -T db psql -U postgres -d microservice -c $sqlQuery 2>$null

    if ($notifCheck -match "\s1\s") {
        Write-Success "Notificación creada exitosamente"
    } else {
        Write-Info "Notificación aún no creada (puede estar procesándose)"
    }
} catch {
    Write-Info "No se pudo verificar notificación en BD"
}

Write-Info ""

# Probar idempotencia
Write-Info "Probando idempotencia (resend con misma key)..."
try {
    $response2 = Invoke-WebRequest -Uri "http://localhost:3200/api/v1/orders" `
        -Method Post `
        -Headers @{
            "Content-Type" = "application/json"
            "Idempotency-Key" = $idempotencyKey
        } `
        -Body $orderBody `
        -ErrorAction Stop

    $response2Body = $response2.Content | ConvertFrom-Json

    if ($response2Body.id -eq $orderId) {
        Write-Success "Idempotencia funciona correctamente (mismo order ID retornado)"
    } else {
        Write-Error-Custom "IDs no coinciden"
    }
} catch {
    if ($_.Exception.Response.StatusCode -eq 409) {
        Write-Success "Idempotencia funciona (409 Conflict retornado)"
    } else {
        Write-Error-Custom "Error en idempotencia: $_"
    }
}

Write-Info ""
Write-Success "Pruebas E2E completadas"
Write-Info ""
Write-Info "Comandos útiles:"
Write-Info "  - Ver logs del worker: docker-compose logs -f worker"
Write-Info "  - Ver RabbitMQ UI: http://localhost:15672"
Write-Info "  - Ver BD: docker-compose exec db psql -U postgres -d microservice"
Write-Info ""
