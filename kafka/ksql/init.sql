SET 'auto.offset.reset' = 'earliest';

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

CREATE OR REPLACE TABLE FACT_SALES_BY_CUSTOMER
WITH (KEY_FORMAT = 'JSON') AS
SELECT
  cust_id,
  COUNT(*)  AS order_count,
  SUM(sale) AS total_spent
FROM SALES_TRANSACTIONS
GROUP BY cust_id
EMIT CHANGES;

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
