# DW Umbrella Frontend + Kafka Streaming

This Flask app runs analytics queries against:

- PostgreSQL (RDBMS)
- Neo4j (graph)
- MongoDB (NoSQL)

It now also streams query execution metadata to Kafka and applies kSQL on top of that stream.

## Current workflow

1. A query is triggered from `/explorer/<db>` or `/compare`.
2. Flask executes the query on the selected data store.
3. The result is returned immediately to the UI (rows + latency).
4. In parallel, Flask publishes an execution event to Kafka topic `dw.query.executions`.
5. ksqlDB materializes streaming views:
   - `QUERY_EXECUTIONS_RAW` (raw stream)
   - `QUERY_EXECUTION_METRICS` (5-minute windowed aggregates by DB and query type)
   - `FAILED_QUERY_EXECUTIONS` (filtered stream of failed executions)

## Docker stack

`docker-compose.yml` includes containers for:

- `umbrella` (Flask app)
- `postgres` (RDBMS)
- `mongo` (NoSQL)
- `neo4j` (graph DB)
- `zookeeper`, `kafka`, `schema-registry`, `ksqldb-server`, `ksqldb-cli`
- `ksqldb-init` (applies `kafka/ksql/init.sql` automatically)

## How to start

1. Copy env template:

```bash
cp .env.example .env
```

2. Build and run:

```bash
docker compose up -d --build
```

If you want to watch logs without stopping containers:

```bash
docker compose logs -f
```

3. Open:

- UI: http://localhost:5000
- ksqlDB API: http://localhost:8088
- Schema Registry: http://localhost:8081

To stop:

```bash
docker compose down
```

## ksqlDB quick checks

Open ksqlDB CLI:

```bash
docker compose exec -T ksqldb-server bash -lc 'until curl -fsS http://localhost:8088/info >/dev/null; do echo "waiting for ksqlDB..."; sleep 2; done'
docker compose exec ksqldb-cli ksql http://ksqldb-server:8088
```

`ksqldb-init` is a one-shot container; `Exited (0)` is expected after startup.

Then run:

```sql
SHOW STREAMS;
SHOW TABLES;
SELECT * FROM QUERY_EXECUTIONS_RAW EMIT CHANGES LIMIT 5;
SELECT * FROM QUERY_EXECUTION_METRICS EMIT CHANGES LIMIT 5;
SELECT * FROM FAILED_QUERY_EXECUTIONS EMIT CHANGES LIMIT 5;
```

## Where predefined queries live

- `app/queries/postgres.py`
- `app/queries/neo4j.py`
- `app/queries/mongo.py`

Use the same query key across all three files for `/compare`.

## MongoDB custom query format

Mongo custom query expects JSON (operation: `find` or `aggregate`), for example:

```json
{
  "collection": "sales_fact",
  "operation": "aggregate",
  "pipeline": [
    {"$group": {"_id": "$product.category", "revenue": {"$sum": "$total_amount"}}},
    {"$sort": {"revenue": -1}}
  ]
}
```
