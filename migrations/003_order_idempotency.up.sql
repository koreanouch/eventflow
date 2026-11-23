-- La migración es idempotente para permitir baselinar bases de datos creadas
-- antes de adoptar golang-migrate (initdb aplicaba los scripts sin registrar
-- versión). En una base nueva se ejecuta una sola vez.
ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(255),
    ADD COLUMN IF NOT EXISTS request_hash BYTEA;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'orders_idempotency_key_unique'
    ) THEN
        ALTER TABLE orders
            ADD CONSTRAINT orders_idempotency_key_unique UNIQUE (idempotency_key);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'orders_idempotency_fields_paired'
    ) THEN
        ALTER TABLE orders
            ADD CONSTRAINT orders_idempotency_fields_paired CHECK (
                (idempotency_key IS NULL AND request_hash IS NULL) OR
                (idempotency_key IS NOT NULL AND request_hash IS NOT NULL)
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'orders_request_hash_length'
    ) THEN
        ALTER TABLE orders
            ADD CONSTRAINT orders_request_hash_length CHECK (
                request_hash IS NULL OR octet_length(request_hash) = 32
            );
    END IF;
END $$;
