-- =============================================================================
-- ksqlDB initialization script for the e-commerce DW umbrella.
--
-- Two independent streaming pipelines are defined here:
--
--   Pipeline A — query-execution observability
--     Topic     : dw.query.executions  (JSON, populated by the Flask app)
--     Stream    : QUERY_EXECUTIONS_RAW
--     Aggregates: QUERY_EXECUTION_METRICS  (5-min tumbling window)
--                 FAILED_QUERY_EXECUTIONS  (filtered stream)
--
--   Pipeline B — streaming sales -> fact table   (Lab 9 pattern)
--     Topic     : dw.sales.transactions  (DELIMITED, populated by data_producer.py)
--     Stream    : SALES_TRANSACTIONS
--     Aggregates: FACT_SALES_BY_PRODUCT_LOCATION
--                 FACT_SALES_BY_CUSTOMER
--                 FACT_SALES_WINDOWED  (1-min tumbling window)
-- =============================================================================

SET 'auto.offset.reset' = 'earliest';

-- -----------------------------------------------------------------------------
-- Pipeline A: query-execution observability (JSON events from Flask)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE STREAM QUERY_EXECUTIONS_RAW (
  event_id     VARCHAR,
  db_name      VARCHAR,
  query_type   VARCHAR,
  query_key    VARCHAR,
  success      BOOLEAN,
  row_count    INTEGER,
  elapsed_ms   DOUBLE,
  executed_at  VARCHAR
) WITH (
  KAFKA_TOPIC  = 'dw.query.executions',
  VALUE_FORMAT = 'JSON',
  PARTITIONS   = 1
);

CREATE OR REPLACE TABLE QUERY_EXECUTION_METRICS
WITH (KEY_FORMAT = 'JSON') AS
SELECT
  db_name,
  query_type,
  COUNT(*)                      AS execution_count,
  ROUND(AVG(elapsed_ms), 2)     AS avg_elapsed_ms,
  MAX(executed_at)              AS last_executed_at
FROM QUERY_EXECUTIONS_RAW
WINDOW TUMBLING (SIZE 5 MINUTES)
GROUP BY db_name, query_type
EMIT CHANGES;

CREATE OR REPLACE STREAM FAILED_QUERY_EXECUTIONS AS
SELECT
  event_id,
  db_name,
  query_type,
  query_key,
  elapsed_ms,
  executed_at
FROM QUERY_EXECUTIONS_RAW
WHERE success = FALSE
EMIT CHANGES;


-- -----------------------------------------------------------------------------
-- Pipeline B: streaming sales transactions -> fact table (Lab 9 pattern)
--
-- The Python data_producer.py reads kafka/data/sales_transactions.txt and
-- pushes one CSV record per message:
--
--     CUST001,Oslo,Laptop,1200.00
--
-- ksqlDB parses each row using the DELIMITED value format and the schema
-- declared on the stream below.  Continuous CREATE TABLE ... AS SELECT
-- queries then keep aggregated fact tables up-to-date in real time.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE STREAM SALES_TRANSACTIONS (
  cust_id   VARCHAR,
  location  VARCHAR,
  product   VARCHAR,
  sale      DOUBLE
) WITH (
  KAFKA_TOPIC  = 'dw.sales.transactions',
  VALUE_FORMAT = 'DELIMITED',
  PARTITIONS   = 1
);

-- Star-schema-style fact aggregated by (product, location).
-- AVG() is intentionally omitted: ksqlDB stores AVG's intermediate state as
-- a STRUCT, which the source stream's DELIMITED format cannot serialise.
-- COUNT and SUM use simple types and work fine.
CREATE OR REPLACE TABLE FACT_SALES_BY_PRODUCT_LOCATION
WITH (KEY_FORMAT = 'JSON') AS
SELECT
  product,
  location,
  COUNT(*)  AS order_count,
  SUM(sale) AS total_sale
FROM SALES_TRANSACTIONS
GROUP BY product, location
EMIT CHANGES;

-- Customer-grain aggregate (top-spender lookup)
CREATE OR REPLACE TABLE FACT_SALES_BY_CUSTOMER
WITH (KEY_FORMAT = 'JSON') AS
SELECT
  cust_id,
  COUNT(*)  AS order_count,
  SUM(sale) AS total_spent
FROM SALES_TRANSACTIONS
GROUP BY cust_id
EMIT CHANGES;

-- Time-windowed revenue (1-minute tumbling) for trend monitoring
CREATE OR REPLACE TABLE FACT_SALES_WINDOWED
WITH (KEY_FORMAT = 'JSON') AS
SELECT
  product,
  COUNT(*)  AS order_count,
  SUM(sale) AS total_sale
FROM SALES_TRANSACTIONS
WINDOW TUMBLING (SIZE 1 MINUTE)
GROUP BY product
EMIT CHANGES;
