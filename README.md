# E-commerce Data Warehouse — three databases, one UI

This project, built for the IKT553 Intelligent Data Management data-warehousing
course, runs the same set of business questions on three different databases and
compares the results.

The three backends are:

- **PostgreSQL** on Supabase, organised as a star schema with one
  fact table and five dimensions.
- **MongoDB** on Atlas, where each sale is a single document with
  its customer, product, date, and location embedded inside it.
- **Neo4j** on Aura, where sales, products, customers, dates, and
  locations are nodes connected by typed relationships.

A small Flask web application sits on top. The user picks a
database, picks (or writes) a query, and receives a table and a
chart. A "compare" page runs the same question on all three
databases and shows the three answers side by side, which makes the
differences between the three models easy to see.

A Kafka and ksqlDB pipeline runs alongside the application. Every
query executed through the UI produces a JSON event on a Kafka
topic. ksqlDB reads from that topic and keeps a rolling metrics
table — number of queries, average elapsed time, number of
failures. A second pipeline streams sales rows from a CSV file
into ksqlDB and materialises them into a live fact table.

The three databases are managed cloud services. The Flask
application and the Kafka stack run locally under Docker Compose.

---

## Architecture

```
┌────────────────┐  queries   ┌──────────────┐
│ Browser (5000) │──────────▶ │  Flask app   │
└────────────────┘            │  umbrella    │
                              └──────┬───────┘
                  ┌──────────────────┼──────────────────┐
                  ▼                  ▼                  ▼
            ┌──────────┐      ┌──────────┐       ┌──────────┐
            │ Postgres │      │ MongoDB  │       │ Neo4j    │
            │ Supabase │      │ Atlas    │       │ Aura     │
            └──────────┘      └──────────┘       └──────────┘
                              (cloud, TLS)

            (same Flask app, one event per query)
                                 │
                                 ▼
                       dw.query.executions (JSON)
                                 │
                                 ▼
                          ┌──────────────┐
                          │  ksqlDB      │── QUERY_EXECUTION_METRICS (5 min tumbling)
                          │  streams &   │── FAILED_QUERY_EXECUTIONS
                          │  tables      │── FACT_SALES_BY_PRODUCT_LOCATION
                          └──────────────┘       (from dw.sales.transactions)
```

---

## Getting started

Docker is required, together with accounts on
Supabase, MongoDB Atlas, and Neo4j Aura.
The free tiers are sufficient.

### Step 1 — configure credentials

Copy the example environment file and open it:

```bash
cp .env.example .env
```

Fill in the Postgres, MongoDB, and Neo4j connection details. The
Kafka section is already set up to talk to the local stack and can
be left unchanged.

### Step 2 — load the data (one-time)

The three loader scripts read the Superstore CSV from `dataset.csv`,
apply the same cleaning rules (date parsing, deduplication of
dimension values, surrogate-key generation), and populate each
backend:

```bash
python etl/load_postgres.py --csv dataset.csv     # fills dim_* and fact_sales
python etl/load_mongo.py    --csv dataset.csv     # fills the `sales` collection
python etl/load_neo4j.py    --csv dataset.csv     # creates Sale/Product/Customer/... nodes
```

Before running `load_postgres.py`, execute the DDL files in
`ddl/postgres/` against the Supabase database (using the Supabase
SQL editor or `psql`). The scripts create the star schema, the
summary tables, and the batch-job specification. The loaders assume
these tables already exist.

### Step 3 — start the stack

```bash
docker compose up --build -d
```

The following containers are started:

| Service           | Port            | Purpose                                      |
|-------------------|-----------------|----------------------------------------------|
| `umbrella`        | 5000            | Flask web UI                                 |
| `kafka`           | 9092 / 29092    | Broker                                       |
| `zookeeper`       | 2181            | Kafka coordinator                            |
| `schema-registry` | 8081            | Reserved for schema-bound pipelines          |
| `ksqldb-server`   | 8088            | Stream-processing engine                     |
| `ksqldb-cli`      | —               | CLI container (entered with `docker compose exec`) |
| `ksqldb-init`     | —               | One-shot: runs `kafka/ksql/init.sql` on boot |

Open **<http://localhost:5000>** in a browser.

To stop the stack:

```bash
docker compose down
```

---

## Repository layout

| Path                                    | Contents                                                 |
|-----------------------------------------|----------------------------------------------------------|
| `app/app.py`                            | Flask routes and Kafka event emission                    |
| `app/db/*.py`                           | One client wrapper per backend                           |
| `app/queries/{postgres,neo4j,mongo}.py` | The ten predefined queries, one file per language        |
| `app/queries/charts.py`                 | Chart.js presets, keyed by query name                    |
| `app/streaming/`                        | Kafka producer and the sales producer              |
| `app/templates/`, `app/static/`         | The UI (Jinja2 templates, a small amount of JavaScript)  |
| `ddl/postgres/`                         | Star schema DDL, summary tables, batch-job specification |
| `ddl/mongo/summary_collections.md`      | MongoDB summary collections and their refresh pipelines  |
| `ddl/neo4j/summary_subgraph.cypher`     | Neo4j aggregate sub-graph and its refresh Cypher         |
| `etl/load_{postgres,mongo,neo4j}.py`    | The data loaders                                         |
| `kafka/ksql/init.sql`                   | All ksqlDB streams, tables, and windowed aggregates      |
| `kafka/data/sales_transactions.txt`     | The CSV consumed by the producer                   |

---

## The ten predefined queries

Each query has three implementations — one per backend — stored under
the same key, so the comparison page can line them up:

1. `top_products_by_revenue` — the ten products with the highest revenue
2. `monthly_revenue_trend` — revenue grouped by year and month
3. `top_customers` — the ten customers with the highest spend
4. `revenue_by_category` — revenue per product category
5. `profit_by_region_state` — profit grouped by region and state
6. `products_high_sales_low_profit` — high-revenue but low-margin items
7. `products_low_sales_high_profit` — low-revenue but high-margin items
8. `profit_ratio_by_ship_mode` — profit margin per shipping mode
9. `most_frequent_customers` — the ten customers with the most orders
10. `lowest_profit_months` — the least profitable months

## Writing a custom query

Each explorer page provides a text area for ad-hoc queries. SQL and
Cypher are used directly. MongoDB has no single query string
language, so the text area expects a small JSON specification:

```json
{
  "collection": "sales",
  "operation": "aggregate",
  "pipeline": [
    {"$group": {"_id": "$product.category", "revenue": {"$sum": "$sales_amount"}}},
    {"$sort":  {"revenue": -1}}
  ]
}
```

The supported operations are `"find"` and `"aggregate"`.

---

## The streaming pipeline

Kafka is enabled by default. As soon as queries are executed in the
UI, events are published. To observe the rolling metrics, enter the
ksqlDB CLI:

```bash
docker compose exec ksqldb-cli ksql http://ksqldb-server:8088
```

Then:

```sql
SET 'auto.offset.reset' = 'earliest';
SELECT * FROM QUERY_EXECUTION_METRICS EMIT CHANGES;
```

To run the producer (CSV → Kafka → ksqlDB fact table):

```bash
docker compose --profile producer up data-producer
```

---

## Notes

- `docker-compose.yml` mounts `./app` into the umbrella container,
  so edits to local Python files trigger an automatic Flask reload.
- Postgres connections use `sslmode=require`, which Supabase requires.
- The elapsed-time values shown in the UI and published to Kafka are
  **operational signals, not a benchmark**, since all three databases are
  hosted in different cloud regions and reached over the open
  internet. The values are useful for spotting slow queries and
  outliers; they are not a fair ranking of the three engines.
- MongoDB Atlas (and, if required, Supabase and Neo4j Aura) requires
  the client IP address to be whitelisted before any connection will
  succeed.
