ALTER TABLE orders
    ADD COLUMN idempotency_key VARCHAR(255),
    ADD COLUMN request_hash BYTEA,
    ADD CONSTRAINT orders_idempotency_key_unique UNIQUE (idempotency_key),
    ADD CONSTRAINT orders_idempotency_fields_paired CHECK (
        (idempotency_key IS NULL AND request_hash IS NULL) OR
        (idempotency_key IS NOT NULL AND request_hash IS NOT NULL)
    ),
    ADD CONSTRAINT orders_request_hash_length CHECK (
        request_hash IS NULL OR octet_length(request_hash) = 32
    );
