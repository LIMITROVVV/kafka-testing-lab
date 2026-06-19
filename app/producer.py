import json
import logging
from typing import Any, Callable, Optional

from confluent_kafka import Producer, KafkaException

logger = logging.getLogger(__name__)


class OrderProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self._producer = Producer({"bootstrap.servers": bootstrap_servers})

    def send(
        self,
        topic: str,
        event: dict[str, Any],
        key: Optional[str] = None,
        on_delivery: Optional[Callable] = None,
    ) -> None:
        """send a single event; key is used for partition assignment."""
        value = json.dumps(event).encode("utf-8")
        encoded_key = key.encode("utf-8") if key else None

        self._producer.produce(
            topic=topic,
            key=encoded_key,
            value=value,
            on_delivery=on_delivery or self._default_delivery_callback,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        """block until all pending messages are delivered. returns remaining."""
        remaining = self._producer.flush(timeout)
        if remaining > 0:
            logger.warning("flush timeout: %d messages not delivered", remaining)
        return remaining

    @staticmethod
    def _default_delivery_callback(err, msg) -> None:
        if err:
            logger.error("delivery failed: %s", err)
        else:
            logger.debug(
                "delivered to %s [%d] @ offset %d",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

    def close(self) -> None:
        self.flush()
