CREATE TABLE IF NOT EXISTS processed_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id VARCHAR(255) NOT NULL UNIQUE,
    event_type VARCHAR(100) NOT NULL,
    order_id VARCHAR(100) NOT NULL,
    consumer_name VARCHAR(100) NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uc_event_consumer UNIQUE(event_id, consumer_name)
);

CREATE INDEX IF NOT EXISTS idx_order_id_events ON processed_events(order_id);
CREATE INDEX IF NOT EXISTS idx_consumer_name ON processed_events(consumer_name);
CREATE INDEX IF NOT EXISTS idx_processed_at ON processed_events(processed_at);

CREATE TABLE IF NOT EXISTS notifications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id VARCHAR(255) NOT NULL,
    order_id VARCHAR(100) NOT NULL,
    customer_id VARCHAR(100) NOT NULL,
    message TEXT NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'created',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_order_id_notif ON notifications(order_id);
CREATE INDEX IF NOT EXISTS idx_customer_id ON notifications(customer_id);
CREATE INDEX IF NOT EXISTS idx_status ON notifications(status);
CREATE INDEX IF NOT EXISTS idx_created_at ON notifications(created_at);
