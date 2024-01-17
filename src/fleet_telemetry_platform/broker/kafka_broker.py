from __future__ import annotations

import json
from typing import Any

from kafka import KafkaConsumer, KafkaProducer
from tenacity import retry, stop_after_attempt, wait_fixed

from .base import BrokerMessage


class KafkaBroker:
    def __init__(self, bootstrap_servers: str, consumer_group: str):
        self.bootstrap_servers = bootstrap_servers
        self.consumer_group = consumer_group
        self._producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        )
        self._consumers: dict[str, KafkaConsumer] = {}

    @retry(stop=stop_after_attempt(5), wait=wait_fixed(1))
    def publish(self, topic: str, message: dict[str, Any]) -> None:
        self._producer.send(topic, value=message)
        self._producer.flush()

    def _get_consumer(self, topic: str) -> KafkaConsumer:
        if topic not in self._consumers:
            self._consumers[topic] = KafkaConsumer(
                topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.consumer_group,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                consumer_timeout_ms=1000,
                value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            )
        return self._consumers[topic]

    def consume(self, topic: str, max_messages: int = 100) -> list[BrokerMessage]:
        consumer = self._get_consumer(topic)
        messages: list[BrokerMessage] = []
        for message in consumer:
            messages.append(BrokerMessage(payload=message.value, ack=consumer.commit))
            if len(messages) >= max_messages:
                break
        return messages
