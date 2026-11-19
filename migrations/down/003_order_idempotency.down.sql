ALTER TABLE orders
    DROP CONSTRAINT IF EXISTS orders_request_hash_length,
    DROP CONSTRAINT IF EXISTS orders_idempotency_fields_paired,
    DROP CONSTRAINT IF EXISTS orders_idempotency_key_unique,
    DROP COLUMN IF EXISTS request_hash,
    DROP COLUMN IF EXISTS idempotency_key;
