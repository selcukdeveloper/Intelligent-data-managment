ALTER TABLE fact_sales
    ADD COLUMN IF NOT EXISTS order_id TEXT,
    ADD COLUMN IF NOT EXISTS line_no  INT;

CREATE INDEX IF NOT EXISTS idx_fact_sales_order_line
    ON fact_sales (order_id, line_no);
