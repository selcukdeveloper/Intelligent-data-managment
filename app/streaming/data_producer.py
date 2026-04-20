from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


DEFAULT_TOPIC = "dw.sales.transactions"
DEFAULT_INTERVAL = 1.0


def _default_data_path() -> Path:
    candidates = [
        Path(os.environ.get("KAFKA_DATA_FILE", "")),
        Path("/data/sales_transactions.txt"),
        Path(__file__).resolve().parents[2] / "kafka" / "data" / "sales_transactions.txt",
    ]
    for path in candidates:
        if path and path.is_file():
            return path
    return candidates[-1]


def _default_broker() -> str:
    if os.environ.get("KAFKA_BOOTSTRAP_SERVERS"):
        return os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    if Path("/.dockerenv").exists():
        return "kafka:29092"
    return "localhost:9092"


def produce(topic: str, broker: str, data_file: Path, interval: float) -> int:
    try:
        producer = KafkaProducer(
            bootstrap_servers=broker,
            api_version=(2, 0, 2),
        )
    except NoBrokersAvailable as exc:
        raise SystemExit(f"Cannot reach Kafka broker at {broker}: {exc}")

    sent = 0
    with data_file.open("r", encoding="utf-8") as src:
        for line in src:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            producer.send(topic, line.encode("utf-8"))
            sent += 1
            print(f"Produced tuple {sent}: {line}")
            time.sleep(interval)

    producer.flush()
    print(f"\nDone. Produced {sent} records to topic '{topic}' on {broker}.")
    return sent


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Sales transaction Kafka producer.")
    parser.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC_SALES", DEFAULT_TOPIC))
    parser.add_argument("--broker", default=_default_broker())
    parser.add_argument("--file", type=Path, default=_default_data_path())
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL)
    args = parser.parse_args(argv)

    if not args.file.is_file():
        raise SystemExit(f"Data file not found: {args.file}")

    produce(args.topic, args.broker, args.file, args.interval)
    return 0


if __name__ == "__main__":
    sys.exit(main())
