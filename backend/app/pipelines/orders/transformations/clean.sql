-- Streaming table defined in SQL: valid orders only, still incremental
CREATE STREAMING TABLE orders_clean
COMMENT 'Orders with a positive amount'
AS SELECT order_id, customer, store, amount, ingested_at
FROM STREAM(orders_raw)
WHERE amount > 0;
