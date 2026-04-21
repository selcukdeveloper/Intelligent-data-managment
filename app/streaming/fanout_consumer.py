from __future__ import annotations

import json
import os
import signal
import sys
from typing import Any

import certifi
import psycopg2
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from neo4j import GraphDatabase
from pymongo import MongoClient


DEFAULT_TOPIC = "dw.sales.ingest"
DEFAULT_GROUP = "dw-sales-ingest-fanout"


class PostgresSink:
    def __init__(self) -> None:
        self.conn_params = {
            "host":            os.environ["PG_HOST"],
            "port":            os.environ.get("PG_PORT", "5432"),
            "dbname":          os.environ.get("PG_DB", "postgres"),
            "user":            os.environ.get("PG_USER", "postgres"),
            "password":        os.environ["PG_PASSWORD"],
            "sslmode":         "require",
            "connect_timeout": 10,
        }

    def _connect(self):
        return psycopg2.connect(**self.conn_params)

    @staticmethod
    def _upsert_dim(cur, *, table: str, key_col: str,
                    where_sql: str, where_params: tuple,
                    extra_cols: tuple[str, ...], extra_vals: tuple) -> int:
        cur.execute(f"SELECT {key_col} FROM {table} WHERE {where_sql}", where_params)
        row = cur.fetchone()
        if row is not None:
            return row[0]

        cols = (key_col,) + extra_cols
        placeholders = [f"(SELECT COALESCE(MAX({key_col}), 0) + 1 FROM {table})"]
        placeholders.extend(["%s"] * len(extra_vals))
        sql = (
            f"INSERT INTO {table} ({', '.join(cols)}) "
            f"VALUES ({', '.join(placeholders)}) RETURNING {key_col}"
        )
        cur.execute(sql, extra_vals)
        return cur.fetchone()[0]

    def write(self, event: dict[str, Any]) -> None:
        conn = self._connect()
        try:
            with conn, conn.cursor() as cur:
                product_key = self._upsert_dim(
                    cur, table="dim_product", key_col="product_key",
                    where_sql="product_name = %s",
                    where_params=(event["product_name"],),
                    extra_cols=("product_name", "category_name"),
                    extra_vals=(event["product_name"], event["category"]),
                )
                customer_key = self._upsert_dim(
                    cur, table="dim_customer", key_col="customer_key",
                    where_sql="customer_name = %s",
                    where_params=(event["customer_name"],),
                    extra_cols=("customer_name",),
                    extra_vals=(event["customer_name"],),
                )
                location_key = self._upsert_dim(
                    cur, table="dim_location", key_col="location_key",
                    where_sql="state = %s AND region = %s",
                    where_params=(event["state"], event["region"]),
                    extra_cols=("state", "region"),
                    extra_vals=(event["state"], event["region"]),
                )
                ship_mode_key = self._upsert_dim(
                    cur, table="dim_ship_mode", key_col="ship_mode_key",
                    where_sql="ship_mode_name = %s",
                    where_params=(event["ship_mode"],),
                    extra_cols=("ship_mode_name",),
                    extra_vals=(event["ship_mode"],),
                )
                date_key = self._upsert_dim(
                    cur, table="dim_date", key_col="date_key",
                    where_sql="year = %s AND month = %s",
                    where_params=(event["year"], event["month"]),
                    extra_cols=("year", "month", "month_name"),
                    extra_vals=(event["year"], event["month"], event["month_name"]),
                )

                cur.execute(
                    """
                    INSERT INTO fact_sales (
                        sales_fact_id,
                        product_key, customer_key, location_key,
                        ship_mode_key, date_key,
                        sales_amount, quantity, profit_amount,
                        order_id, line_no
                    ) VALUES (
                        (SELECT COALESCE(MAX(sales_fact_id), 0) + 1 FROM fact_sales),
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s
                    )
                    """,
                    (
                        product_key, customer_key, location_key,
                        ship_mode_key, date_key,
                        event["sales_amount"], event["quantity"], event["profit_amount"],
                        event.get("order_id"), event.get("line_no"),
                    ),
                )
        finally:
            conn.close()


class MongoSink:
    def __init__(self) -> None:
        self.client = MongoClient(
            os.environ["MONGO_URI"],
            tlsCAFile=certifi.where(),
            serverSelectionTimeoutMS=10000,
        )
        self.db = self.client[os.environ.get("MONGO_DB", "ecommerce_dw")]

    def write(self, event: dict[str, Any]) -> None:
        doc = {
            "sales_fact_id": event["sales_fact_id"],
            "sales_amount":  event["sales_amount"],
            "quantity":      event["quantity"],
            "profit_amount": event["profit_amount"],
            "product": {
                "product_name": event["product_name"],
                "category":     event["category"],
            },
            "customer": {
                "customer_name": event["customer_name"],
            },
            "date": {
                "day":        event["day"],
                "month":      event["month"],
                "month_name": event["month_name"],
                "year":       event["year"],
            },
            "location": {
                "state":  event["state"],
                "region": event["region"],
            },
            "ship_mode": event["ship_mode"],
        }
        self.db.sales.update_one(
            {"sales_fact_id": event["sales_fact_id"]},
            {"$set": doc},
            upsert=True,
        )


class Neo4jSink:
    UPSERT_CYPHER = """
    MERGE (p:Product  {product_name: $product_name})
    MERGE (cat:Category {category_name: $category})
    MERGE (p)-[:IN_CATEGORY]->(cat)

    MERGE (u:Customer {customer_name: $customer_name})
    MERGE (st:State   {state_name:    $state})
      ON CREATE SET st.region = $region
    MERGE (sm:ShipMode {ship_mode_name: $ship_mode})
    MERGE (d:Date {date: $order_date})
    MERGE (m:Month {year: $year, month: $month})
      ON CREATE SET m.month_name = $month_name
    MERGE (d)-[:IN_MONTH]->(m)

    MERGE (s:Sale {sales_fact_id: $sales_fact_id})
      ON CREATE SET s.sales_amount  = $sales_amount,
                    s.quantity      = $quantity,
                    s.profit_amount = $profit_amount
      ON MATCH  SET s.sales_amount  = $sales_amount,
                    s.quantity      = $quantity,
                    s.profit_amount = $profit_amount
    MERGE (s)-[:OF_PRODUCT]->(p)
    MERGE (s)-[:BY_CUSTOMER]->(u)
    MERGE (s)-[:SHIPPED_TO]->(st)
    MERGE (s)-[:ON_DATE]->(d)
    MERGE (s)-[:USES_SHIP_MODE]->(sm)
    """

    def __init__(self) -> None:
        self.driver = GraphDatabase.driver(
            os.environ["NEO4J_URI"],
            auth=(os.environ["NEO4J_USER"], os.environ["NEO4J_PASSWORD"]),
        )

    def write(self, event: dict[str, Any]) -> None:
        with self.driver.session() as session:
            session.run(
                self.UPSERT_CYPHER,
                sales_fact_id=event["sales_fact_id"],
                product_name=event["product_name"],
                category=event["category"],
                customer_name=event["customer_name"],
                state=event["state"],
                region=event["region"],
                ship_mode=event["ship_mode"],
                order_date=event["order_date"],
                year=event["year"],
                month=event["month"],
                month_name=event["month_name"],
                sales_amount=event["sales_amount"],
                quantity=event["quantity"],
                profit_amount=event["profit_amount"],
            )

    def close(self) -> None:
        self.driver.close()


def _default_broker() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")


def _fan_out(event: dict, sinks: dict[str, Any]) -> dict[str, str]:
    outcomes: dict[str, str] = {}
    for name, sink in sinks.items():
        try:
            sink.write(event)
            outcomes[name] = "ok"
        except Exception as exc:
            outcomes[name] = f"ERR: {exc.__class__.__name__}: {exc}"
    return outcomes


def run() -> int:
    topic  = os.environ.get("KAFKA_TOPIC_SALES_INGEST", DEFAULT_TOPIC)
    broker = _default_broker()
    group  = os.environ.get("KAFKA_CONSUMER_GROUP", DEFAULT_GROUP)

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=broker,
            group_id=group,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            consumer_timeout_ms=0,
        )
    except NoBrokersAvailable as exc:
        raise SystemExit(f"Cannot reach Kafka broker at {broker}: {exc}")

    sinks = {
        "postgres": PostgresSink(),
        "mongo":    MongoSink(),
        "neo4j":    Neo4jSink(),
    }

    stop = False

    def _handle_signal(signum, _frame):
        nonlocal stop
        stop = True
        print(f"\nSignal {signum} received, shutting down after current message...")

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT,  _handle_signal)

    print(f"Listening on topic={topic!r} broker={broker!r} group={group!r}")
    processed = 0
    try:
        for msg in consumer:
            if stop:
                break
            event = msg.value
            outcomes = _fan_out(event, sinks)
            processed += 1
            print(
                f"[{processed}] offset={msg.offset} id={event.get('sales_fact_id')}  "
                f"pg={outcomes['postgres']}  mongo={outcomes['mongo']}  neo4j={outcomes['neo4j']}"
            )
    finally:
        consumer.close()
        sinks["neo4j"].close()
        print(f"Stopped. Processed {processed} messages.")
    return 0


if __name__ == "__main__":
    sys.exit(run())
