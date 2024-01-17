from ..config import settings
from .local_queue import LocalQueueBroker


def get_broker():
    if settings.broker_backend == "kafka":
        from .kafka_broker import KafkaBroker

        return KafkaBroker(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            consumer_group=settings.kafka_consumer_group,
        )
    return LocalQueueBroker(root=settings.local_broker_root)
