# Kafka + ksqlDB Streaming

This document explains the streaming layer of the e-commerce DW umbrella.
The implementation follows the patterns introduced in the IKT453 lab
tutorials:

- **Lab 8 — kSQL.** Stand up Kafka + ksqlDB in Docker, declare a stream
  over a topic, run a continuous push query (`EMIT CHANGES`).
- **Lab 9 — kSQL with a transaction stream.** A Python `KafkaProducer`
  reads CSV records from a file, ksqlDB exposes them as a `transactions`
  stream, and a continuous `CREATE TABLE … AS SELECT … GROUP BY`
  materialises a live **fact table**.

Lab 9 is the central pattern in this project: it shows how a star-schema
fact table can be kept up-to-date by a streaming pipeline instead of a
nightly batch job. The umbrella reuses that pattern for sales
transactions.

---

## TL;DR

Two independent pipelines run inside the same Kafka cluster.

| Pipeline | Topic | Source | ksqlDB stream | Materialised tables |
| --- | --- | --- | --- | --- |
| **A — Query observability** | `dw.query.executions` (JSON) | Flask app, every `/api/run` and `/api/compare` | `QUERY_EXECUTIONS_RAW` | `QUERY_EXECUTION_METRICS`, `FAILED_QUERY_EXECUTIONS` |
| **B — Sales fact table** | `dw.sales.transactions` (DELIMITED) | `data_producer.py` | `SALES_TRANSACTIONS` | `FACT_SALES_BY_PRODUCT_LOCATION`, `FACT_SALES_BY_CUSTOMER`, `FACT_SALES_WINDOWED` |

Pipeline A keeps the existing UI observable. Pipeline B is the new
streaming-DW pipeline modelled after Lab 9.

---

## Architecture

```
                         ┌──────────────────────────────────────────┐
                         │                Docker network             │
                         │                                            │
  Flask /api/run ───────►│  Kafka broker  ────────────────────────►  │
                         │   (topic: dw.query.executions, JSON)      │
                         │                                            │
  data_producer.py ─────►│  Kafka broker  ────────────────────────►  │
   (file → producer)      │   (topic: dw.sales.transactions, CSV)    │
                         │                                            │
                         │  ksqlDB server  ─── streams + tables ───► │
                         │   (init.sql applied by ksqldb-init)       │
                         │                                            │
                         │  ksqlDB CLI  (interactive ad-hoc queries) │
                         └──────────────────────────────────────────┘
```

All Kafka services use the standard Confluent images
(`confluentinc/cp-kafka:7.6.1`, `cp-ksqldb-server:7.6.1`,
`cp-ksqldb-cli:7.6.1`), declared in [docker-compose.yml](docker-compose.yml)
and on the same `app-network`. The same broker exposes two listeners:

- `kafka:29092` for in-network clients (Flask, ksqlDB, the producer)
- `localhost:9092` for clients on the host machine

---

## Pipeline A — Query observability (JSON)

Implemented in [app/streaming/kafka_producer.py](app/streaming/kafka_producer.py)
and used from [app/app.py](app/app.py). On every query execution the
Flask app constructs an event:

```json
{
  "event_id": "…uuid…",
  "db_name": "postgres",
  "query_type": "predefined",
  "query_key": "top_products_by_revenue",
  "success": true,
  "row_count": 10,
  "elapsed_ms": 42.7,
  "executed_at": "2026-04-19T09:45:00+00:00"
}
```

The matching kSQL declarations live in [kafka/ksql/init.sql](kafka/ksql/init.sql):

- `QUERY_EXECUTIONS_RAW` — schema over the JSON topic.
- `QUERY_EXECUTION_METRICS` — 5-minute tumbling window aggregating
  `COUNT(*)`, `AVG(elapsed_ms)`, `MAX(executed_at)` per
  `(db_name, query_type)`.
- `FAILED_QUERY_EXECUTIONS` — push stream filtering `success = FALSE`.

This pipeline is fully automatic: nothing extra has to be run. The Flask
app emits events whenever the umbrella UI is used.

---

## Pipeline B — Sales fact table (Lab 9 pattern)

This is the new pipeline that mirrors Lab 9 step-for-step but adapted to
the project's e-commerce domain.

### 1. Source data

Sample records live in
[kafka/data/sales_transactions.txt](kafka/data/sales_transactions.txt).
Each line is one CSV transaction:

```
CUST001,Oslo,Laptop,1200.00
CUST002,Bergen,Phone,800.00
CUST003,Trondheim,Headphones,150.00
...
```

The columns mirror the Lab 9 schema (`custID, location, product, sale`)
and align with the dimensions of the project's relational warehouse
(`dim_customer`, `dim_location`, `dim_product`, `fact_sales`).

### 2. Producer

[app/streaming/data_producer.py](app/streaming/data_producer.py) is a
direct adaptation of the `data_producer.py` shown on slide 5 of Lab 9:

```python
producer = KafkaProducer(bootstrap_servers=broker, api_version=(2,0,2))
for line in data_file:
    producer.send(topic, line.encode("utf-8"))
    time.sleep(interval)
producer.flush()
```

Differences from the tutorial version:

- Topic, broker, file path and interval are configurable via CLI flags
  and environment variables (see below).
- The default broker auto-switches between `kafka:29092` (when running
  inside the Docker network) and `localhost:9092` (when running on the
  host).
- Comments and blank lines in the data file are skipped.

### 3. ksqlDB stream + fact tables

Defined in the second half of [kafka/ksql/init.sql](kafka/ksql/init.sql).
Reading directly from Lab 9 (slide 7):

```sql
CREATE STREAM transactions (custID VARCHAR, location VARCHAR,
                            product VARCHAR, sale DOUBLE)
WITH (kafka_topic='Data', value_format='delimited', partitions=1);

CREATE TABLE fact AS
SELECT product, location, sum(sale) AS total_sale
FROM transactions
GROUP BY product, location;
```

The umbrella's version follows the same shape but uses the project's
naming convention:

```sql
CREATE OR REPLACE STREAM SALES_TRANSACTIONS (
  cust_id  VARCHAR,
  location VARCHAR,
  product  VARCHAR,
  sale     DOUBLE
) WITH (
  KAFKA_TOPIC  = 'dw.sales.transactions',
  VALUE_FORMAT = 'DELIMITED',
  PARTITIONS   = 1
);

CREATE OR REPLACE TABLE FACT_SALES_BY_PRODUCT_LOCATION
WITH (KEY_FORMAT = 'JSON') AS
SELECT product, location,
       COUNT(*) AS order_count,
       SUM(sale) AS total_sale,
       ROUND(AVG(sale), 2) AS avg_sale
FROM SALES_TRANSACTIONS
GROUP BY product, location
EMIT CHANGES;
```

Two extra fact tables extend the example:

- `FACT_SALES_BY_CUSTOMER` — total spend and order count per customer.
- `FACT_SALES_WINDOWED` — 1-minute tumbling revenue per product, useful
  for live dashboards / trend monitoring.

All three are continuous: every new transaction message updates them
incrementally — exactly the behaviour described on slide 10 of Lab 9
("Run the data-producer.py several times and observe changes in
transactions stream and in fact table").

---

## How to run it

### Start the stack

```bash
docker compose up -d
```

This starts Postgres, Mongo, Neo4j, Zookeeper, the Kafka broker, the
Schema Registry, the ksqlDB server and CLI, the umbrella Flask app, and
the one-shot `ksqldb-init` container which applies
`kafka/ksql/init.sql` to create both pipelines.

### Open a ksqlDB CLI session (Lab 8 / Lab 9)

```bash
docker compose exec ksqldb-cli ksql http://ksqldb-server:8088
```

Inside the CLI, list everything:

```sql
SHOW STREAMS;
SHOW TABLES;
```

### Run a continuous push query (Lab 8 style)

```sql
SET 'auto.offset.reset' = 'earliest';
SELECT * FROM SALES_TRANSACTIONS EMIT CHANGES;
```

Leave it running. In a *second* CLI session, query the materialised
fact table:

```sql
SELECT * FROM FACT_SALES_BY_PRODUCT_LOCATION EMIT CHANGES;
```

### Produce sales transactions

Three equivalent options.

**Option A — one-shot via the dedicated compose service (recommended):**

```bash
docker compose run --rm data-producer
```

The `data-producer` service is gated behind the `producer` profile so
it does not start automatically — it only runs when invoked.

**Option B — exec inside the umbrella container:**

```bash
docker compose exec umbrella python -m streaming.data_producer
```

**Option C — directly from the host (uses `localhost:9092`):**

```bash
python app/streaming/data_producer.py
```

In every case you should see output similar to:

```
Produced tuple 1: CUST001,Oslo,Laptop,1200.00
Produced tuple 2: CUST002,Bergen,Phone,800.00
...
Done. Produced 20 records to topic 'dw.sales.transactions' on kafka:29092.
```

…and the continuous query in the ksqlDB CLI will start emitting rows as
they arrive, just like slide 10 of Lab 9.

### Inspect aggregations with pull queries

```sql
SELECT * FROM FACT_SALES_BY_PRODUCT_LOCATION;
SELECT * FROM FACT_SALES_BY_CUSTOMER WHERE cust_id = 'CUST001';
```

### Trigger query-observability events (Pipeline A)

Open the umbrella UI at <http://localhost:5000>, run any query, and
watch:

```sql
SELECT * FROM QUERY_EXECUTIONS_RAW EMIT CHANGES;
SELECT * FROM QUERY_EXECUTION_METRICS;
```

---

## Configuration reference

| Variable | Default | Purpose |
| --- | --- | --- |
| `KAFKA_ENABLED` | `true` | Turn off Pipeline-A publishing without removing the producer |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Broker address (override to `localhost:9092` from host) |
| `KAFKA_TOPIC_QUERY_EXECUTIONS` | `dw.query.executions` | Pipeline-A topic |
| `KAFKA_TOPIC_SALES` | `dw.sales.transactions` | Pipeline-B topic |
| `KAFKA_DATA_FILE` | `/data/sales_transactions.txt` | File path read by `data_producer.py` |
| `KAFKA_SEND_TIMEOUT_SECONDS` | `1.5` | Per-message ack timeout in `QueryEventProducer` |

CLI flags on `data_producer.py` (`--topic`, `--broker`, `--file`,
`--interval`) override the corresponding env vars.

---

## Mapping back to the tutorials

| Tutorial step | Tutorial artefact | Project artefact |
| --- | --- | --- |
| Lab 8: docker-compose with Zookeeper, broker, ksqlDB server, ksqlDB CLI | `docker-compose.yml` in `~/ksql` | `docker-compose.yml` (services `zookeeper`, `kafka`, `ksqldb-server`, `ksqldb-cli`) |
| Lab 8: `CREATE STREAM riderLocations` | inline at the CLI | declared in `kafka/ksql/init.sql` (`SALES_TRANSACTIONS`) |
| Lab 8: continuous push query `SELECT … EMIT CHANGES` | inline at the CLI | same idiom used to inspect `SALES_TRANSACTIONS` and the fact tables |
| Lab 9: `Data.txt` in shared folder | `~/ksql-data/Data.txt` | `kafka/data/sales_transactions.txt` (mounted to `/data` in containers) |
| Lab 9: `data_producer.py` (KafkaProducer reading the file) | host script | `app/streaming/data_producer.py` (CLI flags + dual host/container broker) |
| Lab 9: `CREATE STREAM transactions … value_format='delimited'` | inline at the CLI | declared in `init.sql` |
| Lab 9: `CREATE TABLE fact AS SELECT … GROUP BY product, location` | inline at the CLI | `FACT_SALES_BY_PRODUCT_LOCATION` (+ two extra fact tables) |
| Lab 9: cleanup with `DROP TABLE`, `DROP STREAM`, `docker-compose down -v` | manual | `docker compose down -v` |

---

## Why this matters for the DW project

The course brief asks for a **data streaming functionality** alongside
the relational/NoSQL warehouses. With this setup:

- The streaming sales pipeline shows how a star-schema fact table
  (`fact_sales` in PostgreSQL) can be **maintained continuously** rather
  than rebuilt by a nightly batch — directly addressing the
  "Pre-Aggregated Summary Tables" requirement using ksqlDB.
- The query-observability pipeline shows a second realistic use of
  Kafka — auditing every query the umbrella runs across PostgreSQL,
  Neo4j and MongoDB.
- Both pipelines reuse the same broker, so the demo only needs one
  `docker compose up -d`.
