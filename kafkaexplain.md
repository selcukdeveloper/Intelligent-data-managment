# Kafka Streaming — Demo Walkthrough (Plain English)

This file is written for **you, the presenter**. It assumes you already
know Postgres / Mongo / Neo4j and the Flask umbrella, but have not used
Kafka before. By the end you should be able to:

1. Explain what Kafka and ksqlDB *are* in everyday language.
2. Explain *why* this project uses them and how they relate to your
   star schema in Supabase.
3. Run a live demo with three terminals + the umbrella UI without
   getting stuck.

---

## 1. The 5-minute mental model

### Kafka, in one analogy

Think of **Kafka** as a **post office for events**.

- Different applications drop **messages** ("Customer CUST001 bought a
  Laptop for 1200") into named **topics** (think: PO boxes).
- Other applications subscribe to a topic and read messages out of it,
  in the order they arrived, at their own pace.
- Kafka itself doesn't care what's in a message. It just stores them
  durably and lets many consumers read the same messages independently.

The component that runs the post office is the **Kafka broker**. In
this project that's the `kafka` service in `docker-compose.yml`. The
auxiliary `zookeeper` service is just bookkeeping for the broker — you
can ignore it during the demo.

### Producers and consumers

- A **producer** is any program that *writes* messages to a topic. In
  this project there are two producers:
  - The Flask app, every time someone runs a query in the UI.
  - `data_producer.py`, when you (or the `data-producer` compose
    service) run it on demand.
- A **consumer** is any program that *reads* from a topic. In this
  project the only consumer is **ksqlDB**, which reads topics and
  treats them as SQL streams.

### What is ksqlDB?

ksqlDB is **SQL on top of Kafka**. Instead of having to write Java code
to consume topics and aggregate them, you write SQL like:

```sql
SELECT product, SUM(sale) FROM SALES_TRANSACTIONS GROUP BY product;
```

…and ksqlDB keeps the result up-to-date in real time as new messages
arrive. That is the whole point of the project's streaming layer.

ksqlDB has two key concepts:

- **STREAM** — an ever-growing log of events. Like the topic itself,
  but with a schema. Think *every individual sale*.
- **TABLE** — the latest aggregated value per key. Think
  *"current total sales for Laptop in Oslo"*. The table updates
  whenever a new event arrives in the underlying stream.

A bumper-sticker version: **stream = INSERT log, table = the GROUP BY
result that keeps recalculating itself**.

### What does `EMIT CHANGES` mean?

Normal SQL: "give me the answer once, then stop."
ksqlDB: "give me the answer *and* keep pushing me updates as new data
arrives — forever, until I press Ctrl+C."

The `EMIT CHANGES` keyword is what turns a normal SQL query into a
**continuous push query**. Whenever you see it in a demo, the row is
streaming live; the cursor never finishes.

---

## 2. What's running in your stack

When you run `docker compose up -d`, these are the services involved in
streaming:

| Service | Role | Talk to it via |
| --- | --- | --- |
| `zookeeper` | Coordinator for Kafka. Background only. | nothing — just must be running |
| `kafka` | The broker (the post office). | port `9092` from host, `kafka:29092` inside docker |
| `schema-registry` | Optional store for message schemas. Not required for the demo. | port `8081` |
| `ksqldb-server` | The SQL engine that consumes Kafka topics. | port `8088` |
| `ksqldb-cli` | Interactive shell for `ksqldb-server`. | `docker compose exec ksqldb-cli ksql http://ksqldb-server:8088` |
| `ksqldb-init` | One-shot container that runs on startup and applies `kafka/ksql/init.sql` to create the streams and tables. | runs once and exits |
| `data-producer` | One-shot container that runs `data_producer.py` to push the sample CSV file into Kafka. Started manually. | `docker compose run --rm data-producer` |
| `umbrella` | Your Flask app. Also acts as a Kafka producer for query-execution events. | port `5000` |

The two `init` / `data-producer` containers exit after they finish their
one job — that's normal. Everything else stays running.

---

## 3. The two pipelines, in plain English

You built two completely independent pipelines that share the same
Kafka broker. It's important to keep them separate in your head when
explaining.

### Pipeline A — Query observability (the audit trail)

**What's happening:**

Every time someone clicks "Run" on the umbrella UI, the Flask app does
two things:

1. Executes the SQL/Cypher/Mongo query against the chosen database and
   returns the result to the browser (the user sees no change).
2. *Also* publishes a small JSON event to the Kafka topic
   `dw.query.executions`:

   ```json
   {"db_name":"postgres","query_type":"predefined","query_key":"top_products_by_revenue","success":true,"row_count":10,"elapsed_ms":42.7,"executed_at":"2026-04-19T..."}
   ```

ksqlDB is subscribed to that topic via the `QUERY_EXECUTIONS_RAW`
stream and keeps two derived objects up-to-date:

- `QUERY_EXECUTION_METRICS` — a table that, every 5 minutes,
  aggregates "how many queries hit Postgres? what's the average
  latency? when was the last one?" Useful for monitoring.
- `FAILED_QUERY_EXECUTIONS` — a stream that only contains the events
  where `success = false`. Useful for alerting.

**Why it matters:** It demonstrates a classic Kafka use case —
**decoupling event production from analytics**. The UI doesn't have to
know anything about metrics; it just emits events. The metrics layer
can evolve independently.

**Where to find the code:**
- Producer: [app/streaming/kafka_producer.py](app/streaming/kafka_producer.py)
- Wired into the Flask app: [app/app.py:135-146](app/app.py)
- ksqlDB definitions: top half of [kafka/ksql/init.sql](kafka/ksql/init.sql)

### Pipeline B — Streaming sales fact table (the headline pipeline)

**This is the one to spend the most time on in your demo.** It's the
direct adaptation of Lab 9 and it's what answers the course brief's
"Data Streaming Functionality" requirement.

**What's happening:**

1. There's a sample text file [kafka/data/sales_transactions.txt](kafka/data/sales_transactions.txt)
   containing CSV lines like:

   ```
   CUST001,Oslo,Laptop,1200.00
   CUST002,Bergen,Phone,800.00
   ```

2. [app/streaming/data_producer.py](app/streaming/data_producer.py) is
   a tiny Python script that reads the file line by line and pushes
   each line as a message to the Kafka topic
   `dw.sales.transactions`. It sleeps 1 second between lines on purpose
   so you can *see* records arriving live during the demo.

3. ksqlDB is subscribed to that topic via the `SALES_TRANSACTIONS`
   stream and keeps **three live fact tables** up-to-date:

   - `FACT_SALES_BY_PRODUCT_LOCATION` — total / average sale per
     `(product, location)` combination.
   - `FACT_SALES_BY_CUSTOMER` — total spend and order count per
     customer.
   - `FACT_SALES_WINDOWED` — total revenue per product in 1-minute
     buckets.

The key sentence to memorise:

> "Every time a new sale event lands in the topic, ksqlDB
> *automatically* updates the relevant rows in those fact tables. There
> is no batch job. There is no cron. The aggregations are always
> current."

---

## 4. How this connects to your Supabase star schema

Look at your star schema diagram. The Postgres warehouse has:

- `fact_sales` — one row per sale, with foreign keys to the dimensions
  and three measures (`sales_amount`, `quantity`, `profit_amount`).
- Five dimensions: `dim_date`, `dim_product`, `dim_customer`,
  `dim_location`, `dim_ship_mode`.

In a traditional warehouse you fill `fact_sales` with a **nightly batch
ETL job** that pulls raw transactions from the operational database,
joins them to the dimensions to look up surrogate keys, and inserts the
result.

The Kafka pipeline is the **real-time alternative**. Compare:

| Concept | Postgres (batch) | ksqlDB (streaming) |
| --- | --- | --- |
| Source | OLTP database, queried once per night | Kafka topic, read continuously |
| Fact table | `fact_sales` rows inserted by ETL | `FACT_SALES_BY_PRODUCT_LOCATION` updated per event |
| Latency | hours to a day | milliseconds |
| Aggregations | summary tables built later | summary tables *are* the streaming tables |
| When you query it | the data is from "last night" | the data is "right now" |

**This is the punchline of your Kafka demo:** the streaming fact table
is doing in real time what your Postgres `fact_sales` does in batch.
Same logical model — different update model.

(In a real production system, you'd often use *both*: ksqlDB for live
dashboards and recent activity, Postgres for historical queries and BI
reporting. A Kafka Connect "JDBC sink" can copy the streaming table
into Postgres if you want a single source of truth — that's the
production pattern, but out of scope for this demo.)

---

## 5. Your demo script

Open **three terminals** in `~/Desktop/idmfront`. Make them visible
side-by-side. Also open the umbrella UI in a browser tab at
<http://localhost:5000>.

### Setup (run once before the demo starts)

```bash
docker compose up -d
docker compose ps     # confirm everything is "running" or "Exited (0)"
```

`ksqldb-init` and `dw_data_producer` will show as exited — that's fine,
they only run once. Wait until `ksqldb-server` is healthy (about 30
seconds) before starting the demo.

### Demo Part 1 — Streaming sales (Pipeline B)

This is the show-stopper. Walk through it slowly.

**Terminal 1 — open the ksqlDB shell and inspect what got created:**

```bash
docker compose exec ksqldb-cli ksql http://ksqldb-server:8088
```

Inside the shell:

```sql
SHOW STREAMS;
SHOW TABLES;
DESCRIBE SALES_TRANSACTIONS;
DESCRIBE FACT_SALES_BY_PRODUCT_LOCATION;
```

Talk track:
> "When the stack started, an init container applied this SQL file"
> *(point to `kafka/ksql/init.sql`)* "to declare a stream over the
> Kafka topic and three live fact tables on top of it. Let me show you
> the schema."

**Terminal 1 — start a continuous query on the raw stream:**

```sql
SET 'auto.offset.reset' = 'earliest';
SELECT * FROM SALES_TRANSACTIONS EMIT CHANGES;
```

Talk track:
> "`EMIT CHANGES` means this query *never finishes*. It's going to keep
> printing new rows the moment they arrive in the Kafka topic. Right
> now there are no rows because we haven't produced anything yet."

**Terminal 2 — open another ksqlDB shell, watch the fact table:**

```bash
docker compose exec ksqldb-cli ksql http://ksqldb-server:8088
```

```sql
SET 'auto.offset.reset' = 'earliest';
SELECT * FROM FACT_SALES_BY_PRODUCT_LOCATION EMIT CHANGES;
```

Talk track:
> "And in this terminal I'm watching the live fact table. Same idea,
> but instead of individual sale events, ksqlDB keeps a running
> `SUM(sale) GROUP BY product, location` for me automatically."

**Terminal 3 — produce the sales transactions:**

```bash
docker compose run --rm data-producer
```

Talk track *(while it's running)*:
> "This script reads `kafka/data/sales_transactions.txt`, twenty CSV
> lines, and pushes them one by one into the Kafka topic with a
> one-second pause between each. Watch the other two terminals."

What the audience sees:

- Terminal 1 prints one new row of `SALES_TRANSACTIONS` every second —
  the raw event stream.
- Terminal 2's fact table rows update in real time. The same
  `(product, location)` combination's `total_sale` keeps incrementing
  every time a matching transaction arrives.

**Terminal 1 / 2 — pull-query the fact table on demand:**

After the producer finishes, hit Ctrl+C in terminals 1 and 2 to stop
the push queries, then run a normal one-shot query:

```sql
SELECT * FROM FACT_SALES_BY_PRODUCT_LOCATION;
SELECT * FROM FACT_SALES_BY_CUSTOMER WHERE cust_id = 'CUST001';
```

Talk track:
> "This is the same idea your Postgres queries answer, but the data is
> being maintained live as events arrive instead of by a nightly
> batch."

### Demo Part 2 — Query observability (Pipeline A)

This is shorter — five minutes max.

**Terminal 1 — start a continuous query on the query-event stream:**

```sql
SET 'auto.offset.reset' = 'latest';
SELECT * FROM QUERY_EXECUTIONS_RAW EMIT CHANGES;
```

(`latest` here so you only see *new* events triggered during the demo,
not every event since the stack started.)

**Browser — drive the umbrella UI:**

Open <http://localhost:5000>, click into one of the database explorers,
and run two or three predefined queries.

Talk track:
> "Every time I click Run, the Flask app publishes a small JSON event
> describing what was executed, against which database, how long it
> took, and whether it succeeded. ksqlDB picks that up here."

The audience sees rows appearing live in terminal 1 as you click Run.

**Terminal 2 — show the rolled-up metrics table:**

```sql
SELECT * FROM QUERY_EXECUTION_METRICS;
```

Talk track:
> "And here's the same data aggregated into a metrics table — count of
> queries, average latency, last execution time, grouped by database
> and query type, in 5-minute windows. This is what a monitoring
> dashboard would chart."

### Closing the demo

```sql
EXIT;
```

…in each ksqlDB CLI session. Leave the docker stack running for the
rest of your presentation; nothing extra to clean up unless you want to
fully reset:

```bash
docker compose down -v
```

---

## 6. Likely instructor questions and short answers

**Q: "Where does the streaming fact table actually live?"**
A: It lives inside ksqlDB, which stores its state in **changelog
topics on the Kafka broker**. So it survives restarts of ksqlDB — when
ksqlDB comes back up, it replays the changelog to rebuild the table in
memory. It does *not* live in Postgres.

**Q: "Why use ksqlDB if you already have Postgres?"**
A: Two different use cases. Postgres is great for historical analytics
on a complete dataset. ksqlDB is great for keeping aggregates current
in real time, with millisecond latency, as new events arrive. In a
production system you'd typically use both — ksqlDB for the live
view, Postgres for the historical record.

**Q: "What's the difference between a stream and a table in ksqlDB?"**
A: A stream is a never-ending log — every event is preserved. A table
is the latest value per key — old values are overwritten. `INSERT INTO
sales VALUES (...)` adds a row to the stream; the table on top of it
just updates the matching `GROUP BY` row.

**Q: "Why DELIMITED for sales but JSON for query events?"**
A: To match the two tutorials we followed. Lab 9's example uses
DELIMITED (CSV) because the file format itself is CSV. The Flask
producer naturally emits structured JSON, so JSON is a better fit
there. ksqlDB supports both transparently — the schema in `CREATE
STREAM` is declared independently of the value format.

**Q: "What happens if the Kafka broker goes down mid-demo?"**
A: The Flask app catches the exception, the user still sees their
query result, and a `streaming_error` field gets included in the
response. The umbrella UI doesn't break. (See the try/except in
[app/app.py:53-59](app/app.py).)

**Q: "Did you use kSQL? That's the optional extra credit."**
A: Yes — both pipelines are entirely declared in ksqlDB SQL in
[kafka/ksql/init.sql](kafka/ksql/init.sql). The fact-table aggregation
is a continuous `CREATE TABLE ... AS SELECT ... GROUP BY ... EMIT
CHANGES` query, which is the kSQL idiom for materialised views.

**Q: "Could you stream into Postgres so `fact_sales` updates live?"**
A: Yes — Kafka Connect's JDBC sink connector can subscribe to the
ksqlDB output topic and INSERT into Postgres. We didn't wire it up
because the project requires demonstrating the streaming pipeline
itself, not the sink. It would be a one-config-file addition.

---

## 7. Cheat-sheet (pin this to your monitor)

| Action | Command |
| --- | --- |
| Start everything | `docker compose up -d` |
| Open ksqlDB shell | `docker compose exec ksqldb-cli ksql http://ksqldb-server:8088` |
| List streams / tables | `SHOW STREAMS;` / `SHOW TABLES;` |
| Watch raw sale events live | `SELECT * FROM SALES_TRANSACTIONS EMIT CHANGES;` |
| Watch live fact table | `SELECT * FROM FACT_SALES_BY_PRODUCT_LOCATION EMIT CHANGES;` |
| Watch live query events | `SELECT * FROM QUERY_EXECUTIONS_RAW EMIT CHANGES;` |
| Pull-query fact table | `SELECT * FROM FACT_SALES_BY_PRODUCT_LOCATION;` |
| Send sample sales | `docker compose run --rm data-producer` |
| Reset everything | `docker compose down -v && docker compose up -d` |

Three things to remember during the live demo:

1. `EMIT CHANGES` = "stream forever, Ctrl+C to stop."
2. `auto.offset.reset = 'earliest'` = "show me everything from the
   start." Use `'latest'` if you only want events triggered live.
3. The init container applies the SQL once at startup — if you change
   `init.sql`, run `docker compose down -v && docker compose up -d` to
   re-apply it.

---

## 8. The one paragraph you should be able to say cold

If the instructor asks "explain in 30 seconds what your streaming
layer does":

> "We have a Kafka broker running in Docker with two pipelines on top.
> The first one is observability: every time a query runs in the
> umbrella UI, the Flask app pushes a JSON event into a topic, and
> ksqlDB rolls those events up into a metrics table grouped by
> database and query type. The second one is the streaming fact table:
> a Python producer pushes raw sales transactions into another topic,
> ksqlDB exposes them as a SQL stream, and a continuous `CREATE TABLE
> AS SELECT ... GROUP BY product, location` query keeps a live fact
> table up to date — the same shape as our `fact_sales` star-schema
> table in Supabase, but maintained event-by-event instead of by a
> nightly batch."

That sentence has all the pieces. If you can say *just that* and back
it up by running the three terminals above, you've nailed it.
