from __future__ import annotations

import json
import os

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError, NoBrokersAvailable


def _as_bool(value: str | None, *, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


class QueryEventProducer:
    def __init__(self):
        self.enabled = _as_bool(os.environ.get("KAFKA_ENABLED"), default=True)
        self.bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
        self.topic = os.environ.get(
            "KAFKA_TOPIC_QUERY_EXECUTIONS",
            "dw.query.executions",
        )
        self.send_timeout_seconds = float(os.environ.get("KAFKA_SEND_TIMEOUT_SECONDS", "1.5"))
        self._producer: KafkaProducer | None = None

    def _get_producer(self) -> KafkaProducer:
        if self._producer is None:
            try:
                self._producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
                    acks="1",
                    retries=2,
                    linger_ms=10,
                    request_timeout_ms=3000,
                    api_version_auto_timeout_ms=3000,
                )
            except (NoBrokersAvailable, KafkaError) as exc:
                raise RuntimeError(
                    f"Could not connect to Kafka at {self.bootstrap_servers}"
                ) from exc
        return self._producer

    def publish(self, event: dict) -> None:
        if not self.enabled:
            return

        producer = self._get_producer()
        try:
            future = producer.send(self.topic, value=event)
            future.get(timeout=self.send_timeout_seconds)
        except (KafkaTimeoutError, KafkaError) as exc:
            raise RuntimeError(
                f"Failed to publish to topic '{self.topic}'"
            ) from exc
