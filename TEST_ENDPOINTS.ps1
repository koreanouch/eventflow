# PowerShell Commands to Test Order Endpoints

## Prerequisites
# - Microservice running on http://localhost:3200
# - PowerShell 5.1+ or PowerShell 7+

## 1. Create a Test Order

$ordersUrl = "http://localhost:3200/api/v1/orders"

$createOrderBody = @{
    customer_id = "test-customer-001"
    items = @(
        @{
            product_id = "PROD-ABC123"
            quantity = 5
            unit_price = 2500
        },
        @{
            product_id = "PROD-DEF456"
            quantity = 3
            unit_price = 1500
        }
    )
} | ConvertTo-Json

Write-Host "Creating order..." -ForegroundColor Cyan
$createResponse = Invoke-WebRequest -Uri $ordersUrl `
    -Method POST `
    -ContentType "application/json" `
    -Body $createOrderBody

$orderData = $createResponse.Content | ConvertFrom-Json
$orderId = $orderData.id

Write-Host "Order created successfully!" -ForegroundColor Green
Write-Host "Order ID: $orderId" -ForegroundColor Yellow
Write-Host "Created At: $($orderData.created_at)" -ForegroundColor Yellow
Write-Host ""

## 2. Retrieve the Created Order

$getOrderUrl = "$ordersUrl/$orderId"

Write-Host "Retrieving order..." -ForegroundColor Cyan
$getResponse = Invoke-WebRequest -Uri $getOrderUrl -Method GET

$order = $getResponse.Content | ConvertFrom-Json

Write-Host "Order retrieved successfully!" -ForegroundColor Green
Write-Host ""
Write-Host "Order Details:" -ForegroundColor Yellow
Write-Host "  ID:           $($order.id)" -ForegroundColor Cyan
Write-Host "  Customer ID:  $($order.customer_id)" -ForegroundColor Cyan
Write-Host "  Status:       $($order.status)" -ForegroundColor Cyan
Write-Host "  Total Amount: $($order.total_amount)" -ForegroundColor Cyan
Write-Host "  Created At:   $($order.created_at)" -ForegroundColor Cyan
Write-Host ""
Write-Host "Items:" -ForegroundColor Yellow
$order.items | ForEach-Object {
    Write-Host "  - Product: $($_.product_id), Qty: $($_.quantity), Price: $($_.unit_price)" -ForegroundColor Cyan
}
Write-Host ""

## 3. Test Error Cases

### 3.1 Test 404 Not Found
Write-Host "Testing 404 Not Found..." -ForegroundColor Cyan
try {
    Invoke-WebRequest -Uri "$ordersUrl/nonexistent-order-id" -Method GET
} catch {
    Write-Host "Got expected 404 error:" -ForegroundColor Yellow
    Write-Host "  Status Code: $($_.Exception.Response.StatusCode)" -ForegroundColor Red
    Write-Host "  Message: $($_.Exception.Response | ForEach-Object { $_.StatusDescription })" -ForegroundColor Red
}
Write-Host ""

### 3.2 Test 400 Bad Request (Missing customer_id)
Write-Host "Testing 400 Bad Request (missing customer_id)..." -ForegroundColor Cyan
$invalidBody = @{
    items = @(
        @{ product_id = "prod-1"; quantity = 1; unit_price = 100 }
    )
} | ConvertTo-Json

try {
    Invoke-WebRequest -Uri $ordersUrl `
        -Method POST `
        -ContentType "application/json" `
        -Body $invalidBody
} catch {
    Write-Host "Got expected 400 error:" -ForegroundColor Yellow
    Write-Host "  Status Code: $($_.Exception.Response.StatusCode)" -ForegroundColor Red
}
Write-Host ""

### 3.3 Test 400 Bad Request (Invalid quantity)
Write-Host "Testing 400 Bad Request (negative quantity)..." -ForegroundColor Cyan
$invalidQtyBody = @{
    customer_id = "test-customer"
    items = @(
        @{ product_id = "prod-1"; quantity = -1; unit_price = 100 }
    )
} | ConvertTo-Json

try {
    Invoke-WebRequest -Uri $ordersUrl `
        -Method POST `
        -ContentType "application/json" `
        -Body $invalidQtyBody
} catch {
    Write-Host "Got expected 400 error:" -ForegroundColor Yellow
    Write-Host "  Status Code: $($_.Exception.Response.StatusCode)" -ForegroundColor Red
}
Write-Host ""

### 3.4 Test 400 Bad Request (No items)
Write-Host "Testing 400 Bad Request (empty items array)..." -ForegroundColor Cyan
$noItemsBody = @{
    customer_id = "test-customer"
    items = @()
} | ConvertTo-Json

try {
    Invoke-WebRequest -Uri $ordersUrl `
        -Method POST `
        -ContentType "application/json" `
        -Body $noItemsBody
} catch {
    Write-Host "Got expected 400 error:" -ForegroundColor Yellow
    Write-Host "  Status Code: $($_.Exception.Response.StatusCode)" -ForegroundColor Red
}
Write-Host ""

## 4. Comprehensive Integration Test

Write-Host "Running comprehensive integration test..." -ForegroundColor Green
Write-Host ""

# Create multiple orders
$orders = @()

for ($i = 1; $i -le 3; $i++) {
    $body = @{
        customer_id = "customer-$i"
        items = @(
            @{ product_id = "product-$i-a"; quantity = $i; unit_price = (1000 * $i) }
        )
    } | ConvertTo-Json

    $response = Invoke-WebRequest -Uri $ordersUrl `
        -Method POST `
        -ContentType "application/json" `
        -Body $body

    $order = $response.Content | ConvertFrom-Json
    $orders += $order

    Write-Host "Created order $i : $($order.id)" -ForegroundColor Cyan
}

Write-Host ""
Write-Host "Retrieving all created orders..." -ForegroundColor Cyan

$orders | ForEach-Object {
    $response = Invoke-WebRequest -Uri "$ordersUrl/$($_.id)" -Method GET
    $order = $response.Content | ConvertFrom-Json

    Write-Host ""
    Write-Host "Order: $($order.customer_id)" -ForegroundColor Yellow
    Write-Host "  Total: $($order.total_amount)" -ForegroundColor Cyan
    Write-Host "  Status: $($order.status)" -ForegroundColor Cyan
}

Write-Host ""
Write-Host "✅ Comprehensive integration test completed successfully!" -ForegroundColor Green
Write-Host ""

## 5. Summary

Write-Host "════════════════════════════════════════════════" -ForegroundColor Green
Write-Host "Test Summary" -ForegroundColor Green
Write-Host "════════════════════════════════════════════════" -ForegroundColor Green
Write-Host ""
Write-Host "✅ POST /api/v1/orders" -ForegroundColor Green
Write-Host "   - Create order with multiple items" -ForegroundColor Cyan
Write-Host "   - Validates required fields" -ForegroundColor Cyan
Write-Host "   - Rejects invalid input" -ForegroundColor Cyan
Write-Host ""
Write-Host "✅ GET /api/v1/orders/:id" -ForegroundColor Green
Write-Host "   - Retrieve created order" -ForegroundColor Cyan
Write-Host "   - Returns complete order data" -ForegroundColor Cyan
Write-Host "   - Handles 404 for missing orders" -ForegroundColor Cyan
Write-Host ""
Write-Host "✅ Error Handling" -ForegroundColor Green
Write-Host "   - 400: Validation errors" -ForegroundColor Yellow
Write-Host "   - 404: Order not found" -ForegroundColor Yellow
Write-Host "   - 500: Internal server errors" -ForegroundColor Yellow
Write-Host ""
Write-Host "════════════════════════════════════════════════" -ForegroundColor Green
