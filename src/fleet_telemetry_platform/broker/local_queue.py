from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any

from .base import BrokerMessage


class LocalQueueBroker:
    def __init__(self, root: Path):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def _topic_dir(self, topic: str) -> Path:
        topic_dir = self.root / topic.replace(".", "_")
        topic_dir.mkdir(parents=True, exist_ok=True)
        return topic_dir

    def publish(self, topic: str, message: dict[str, Any]) -> None:
        filename = f"{time.time_ns()}_{uuid.uuid4().hex}.json"
        path = self._topic_dir(topic) / filename
        path.write_text(json.dumps(message), encoding="utf-8")

    def consume(self, topic: str, max_messages: int = 100) -> list[BrokerMessage]:
        messages: list[BrokerMessage] = []
        for path in sorted(self._topic_dir(topic).glob("*.json"))[:max_messages]:
            payload = json.loads(path.read_text(encoding="utf-8"))
            messages.append(BrokerMessage(payload=payload, ack=lambda p=path: p.unlink(missing_ok=True)))
        return messages
