from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Protocol


@dataclass(slots=True)
class BrokerMessage:
    payload: dict[str, Any]
    ack: Callable[[], None]


class Broker(Protocol):
    def publish(self, topic: str, message: dict[str, Any]) -> None:
        ...

    def consume(self, topic: str, max_messages: int = 100) -> list[BrokerMessage]:
        ...
