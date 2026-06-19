import json
import logging
from typing import Any, Generator, Optional

from confluent_kafka import Consumer, KafkaException, KafkaError, Message

logger = logging.getLogger(__name__)

# how long to wait on a single poll before giving up
DEFAULT_POLL_TIMEOUT = 5.0


class OrderConsumer:
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        group_id: str = "default-group",
        auto_offset_reset: str = "earliest",
        enable_auto_commit: bool = False,
    ):
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": auto_offset_reset,
                # manual commit - we control when offset moves
                "enable.auto.commit": enable_auto_commit,
            }
        )

    def subscribe(self, topics: list[str]) -> None:
        self._consumer.subscribe(topics)

    def poll_one(self, timeout: float = DEFAULT_POLL_TIMEOUT) -> Optional[dict[str, Any]]:
        """poll for a single message. returns parsed dict or None on timeout."""
        msg: Optional[Message] = self._consumer.poll(timeout)
        if msg is None:
            return None
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return None
            raise KafkaException(msg.error())

        return json.loads(msg.value().decode("utf-8"))

    def poll_messages(
        self, count: int, timeout_per_msg: float = DEFAULT_POLL_TIMEOUT
    ) -> Generator[dict[str, Any], None, None]:
        """yield up to `count` messages. stops early on timeout."""
        received = 0
        while received < count:
            msg = self.poll_one(timeout_per_msg)
            if msg is None:
                break
            yield msg
            received += 1

    def commit(self) -> None:
        self._consumer.commit(asynchronous=False)

    def close(self) -> None:
        self._consumer.close()
