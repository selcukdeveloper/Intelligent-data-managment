from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable


DEFAULT_TOPIC = "dw.sales.ingest"
DEFAULT_INTERVAL = 0.2

MONTH_NAMES = [
    "", "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]


def _parse_date(raw: str) -> datetime | None:
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(raw.strip(), fmt)
        except ValueError:
            continue
    return None


def row_to_event(row: dict[str, str], line_no: int) -> dict | None:
    order_date = _parse_date(row.get("Order Date", ""))
    if order_date is None:
        return None
    try:
        return {
            "sales_fact_id":  f"{row['Order ID'].strip()}-{line_no}",
            "order_id":       row["Order ID"].strip(),
            "line_no":        line_no,
            "order_date":     order_date.date().isoformat(),
            "year":           order_date.year,
            "month":          order_date.month,
            "month_name":     MONTH_NAMES[order_date.month],
            "day":            order_date.day,
            "ship_mode":      row["Ship Mode"].strip(),
            "customer_id":    row.get("Customer ID", "").strip(),
            "customer_name":  row["Customer Name"].strip(),
            "segment":        row.get("Segment", "").strip(),
            "country":        row.get("Country", "").strip(),
            "city":           row.get("City", "").strip(),
            "state":          row["State"].strip(),
            "postal_code":    row.get("Postal Code", "").strip(),
            "region":         row["Region"].strip(),
            "product_id":     row.get("Product ID", "").strip(),
            "category":       row["Category"].strip(),
            "sub_category":   row.get("Sub-Category", "").strip(),
            "product_name":   row["Product Name"].strip(),
            "sales_amount":   float(row["Sales"]),
            "quantity":       int(row["Quantity"]),
            "discount":       float(row.get("Discount", "0") or 0),
            "profit_amount":  float(row["Profit"]),
        }
    except (KeyError, ValueError):
        return None


def _default_broker() -> str:
    if os.environ.get("KAFKA_BOOTSTRAP_SERVERS"):
        return os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    if Path("/.dockerenv").exists():
        return "kafka:29092"
    return "localhost:9092"


def _default_file() -> Path:
    for candidate in (
        Path(os.environ.get("INGEST_CSV_FILE", "")),
        Path("/data/dataset_add.csv"),
        Path(__file__).resolve().parents[2] / "dataset_add.csv",
    ):
        if candidate and candidate.is_file():
            return candidate
    return Path(__file__).resolve().parents[2] / "dataset_add.csv"


def produce(topic: str, broker: str, csv_file: Path, interval: float) -> int:
    try:
        producer = KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda v: v.encode("utf-8") if v else None,
            acks=1,
            retries=3,
            linger_ms=10,
        )
    except NoBrokersAvailable as exc:
        raise SystemExit(f"Cannot reach Kafka broker at {broker}: {exc}")

    sent = 0
    skipped = 0
    with csv_file.open(newline="", encoding="latin-1") as fh:
        reader = csv.DictReader(fh)
        for i, raw in enumerate(reader):
            event = row_to_event(raw, i)
            if event is None:
                skipped += 1
                continue
            try:
                future = producer.send(topic, key=event["sales_fact_id"], value=event)
                future.get(timeout=5.0)
                sent += 1
                print(
                    f"[{sent}] -> {topic}  id={event['sales_fact_id']}  "
                    f"customer={event['customer_name']!r}  sales={event['sales_amount']:.2f}"
                )
            except KafkaError as exc:
                print(f"ERROR sending row {i}: {exc}", file=sys.stderr)
            time.sleep(interval)

    producer.flush()
    print(f"\nDone. Sent {sent} events, skipped {skipped} invalid rows.")
    return sent


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Stream a Superstore-format CSV into Kafka.")
    p.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC_SALES_INGEST", DEFAULT_TOPIC))
    p.add_argument("--broker", default=_default_broker())
    p.add_argument("--file", type=Path, default=_default_file())
    p.add_argument("--interval", type=float, default=DEFAULT_INTERVAL)
    args = p.parse_args(argv)

    if not args.file.is_file():
        raise SystemExit(f"CSV file not found: {args.file}")

    produce(args.topic, args.broker, args.file, args.interval)
    return 0


if __name__ == "__main__":
    sys.exit(main())
