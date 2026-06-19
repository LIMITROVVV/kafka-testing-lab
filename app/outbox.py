"""
outbox pattern - simplified for the demo.
real outbox uses a db table (e.g. postgres) + a separate relay process
(debezium CDC or a polling relay). here both live in-memory.
"""

import logging
from dataclasses import dataclass, field
from typing import Any

from app.producer import OrderProducer

logger = logging.getLogger(__name__)


@dataclass
class OutboxEntry:
    event_id: str
    topic: str
    key: str
    payload: dict[str, Any]
    published: bool = False


class OutboxStore:
    """in-memory stand-in for a db outbox table."""

    def __init__(self) -> None:
        self._entries: list[OutboxEntry] = []

    def append(self, entry: OutboxEntry) -> None:
        self._entries.append(entry)

    def unpublished(self) -> list[OutboxEntry]:
        return [e for e in self._entries if not e.published]

    def mark_published(self, event_id: str) -> None:
        for entry in self._entries:
            if entry.event_id == event_id:
                entry.published = True
                return


class OutboxRelay:
    """
    polls the outbox store and forwards entries to kafka.
    in production this runs as a separate process / background thread.
    """

    def __init__(self, store: OutboxStore, producer: OrderProducer) -> None:
        self._store = store
        self._producer = producer

    def relay(self) -> int:
        """relay all unpublished entries. returns count sent."""
        entries = self._store.unpublished()
        sent = 0
        delivery_errors: list[Exception] = []

        for entry in entries:
            def on_delivery(err, msg, _entry=entry):
                if err:
                    delivery_errors.append(err)
                else:
                    self._store.mark_published(_entry.event_id)

            self._producer.send(
                topic=entry.topic,
                event=entry.payload,
                key=entry.key,
                on_delivery=on_delivery,
            )
            sent += 1

        self._producer.flush()

        if delivery_errors:
            logger.error("relay errors: %s", delivery_errors)

        return sent
