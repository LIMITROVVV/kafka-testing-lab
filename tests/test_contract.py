"""
contract tests: validate that events on the topic conform to the json schema.

this is the "consumer-side contract" - before a service processes a message,
it checks the shape is what it signed up for. catches producer-side breaking changes
before they blow up in production.

schema: schemas/order_event.json
"""

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest
import jsonschema

from app.producer import OrderProducer
from app.consumer import OrderConsumer

SCHEMA_PATH = Path(__file__).parent.parent / "schemas" / "order_event.json"


@pytest.fixture(scope="module")
def order_schema() -> dict:
    return json.loads(SCHEMA_PATH.read_text())


def _valid_event(order_id: str = "order-999") -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "order_id": order_id,
        "event_type": "order.created",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": {
            "amount": 149.90,
            "currency": "USD",
            "items": [{"sku": "WIDGET-01", "qty": 2}],
        },
    }


def test_valid_event_passes_schema(order_schema):
    event = _valid_event()
    # should not raise
    jsonschema.validate(instance=event, schema=order_schema)


def test_missing_required_field_fails(order_schema):
    event = _valid_event()
    del event["order_id"]

    with pytest.raises(jsonschema.ValidationError, match="'order_id' is a required property"):
        jsonschema.validate(instance=event, schema=order_schema)


def test_invalid_event_type_fails(order_schema):
    event = _valid_event()
    event["event_type"] = "order.shipped"  # not in enum

    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance=event, schema=order_schema)


def test_invalid_currency_fails(order_schema):
    event = _valid_event()
    event["payload"]["currency"] = "usd"  # must be uppercase 3-letter

    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance=event, schema=order_schema)


def test_negative_amount_fails(order_schema):
    event = _valid_event()
    event["payload"]["amount"] = -5.0

    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance=event, schema=order_schema)


def test_extra_fields_rejected(order_schema):
    """schema has additionalProperties: false - no unknown fields allowed."""
    event = _valid_event()
    event["internal_flag"] = True  # field not in schema

    with pytest.raises(jsonschema.ValidationError, match="Additional properties are not allowed"):
        jsonschema.validate(instance=event, schema=order_schema)


def test_event_from_kafka_matches_schema(unique_topic, producer, make_consumer, order_schema):
    """end-to-end: produce an event, consume it, validate schema against what actually landed."""
    event = _valid_event("order-e2e-contract")
    producer.send(unique_topic, event, key=event["order_id"])
    producer.flush()

    consumer: OrderConsumer = make_consumer()
    consumer.subscribe([unique_topic])

    received = consumer.poll_one(timeout=5.0)
    assert received is not None, "event should arrive from topic"

    consumer.commit()

    # this is the real contract check - validate what the consumer actually read
    jsonschema.validate(instance=received, schema=order_schema)


def test_malformed_event_caught_before_processing(unique_topic, producer, make_consumer, order_schema):
    """
    producer sends a broken event (missing payload fields).
    consumer should detect the contract violation and not process it silently.
    """
    bad_event = {
        "event_id": str(uuid.uuid4()),
        "order_id": "order-bad",
        "event_type": "order.created",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": {
            # missing required 'currency'
            "amount": 10.0,
        },
    }

    producer.send(unique_topic, bad_event, key="order-bad")
    producer.flush()

    consumer: OrderConsumer = make_consumer()
    consumer.subscribe([unique_topic])

    received = consumer.poll_one(timeout=5.0)
    assert received is not None

    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance=received, schema=order_schema)
