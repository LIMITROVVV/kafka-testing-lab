import uuid
import time
import logging
import pytest

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException

from app.producer import OrderProducer
from app.consumer import OrderConsumer

BOOTSTRAP_SERVERS = "localhost:9092"
logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def admin_client():
    client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    return client


def _wait_for_kafka(admin_client, retries=15, delay=2.0):
    """block until kafka responds. fails fast in CI if broker is down."""
    for attempt in range(retries):
        try:
            meta = admin_client.list_topics(timeout=3)
            if meta:
                return
        except KafkaException:
            pass
        logger.info("waiting for kafka... (%d/%d)", attempt + 1, retries)
        time.sleep(delay)
    pytest.fail("kafka not reachable after retries")


@pytest.fixture(scope="session", autouse=True)
def wait_for_kafka(admin_client):
    _wait_for_kafka(admin_client)


@pytest.fixture
def unique_topic(admin_client):
    """create a fresh topic per test, delete it after."""
    topic_name = f"test-{uuid.uuid4().hex[:8]}"
    fs = admin_client.create_topics([NewTopic(topic_name, num_partitions=1, replication_factor=1)])
    for topic, f in fs.items():
        f.result()  # raise on error

    yield topic_name

    admin_client.delete_topics([topic_name])


@pytest.fixture
def producer():
    p = OrderProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    yield p
    p.close()


@pytest.fixture
def make_consumer():
    """factory so each test gets its own group id - no offset state bleed."""
    consumers = []

    def _make(group_id: str = None, auto_offset_reset: str = "earliest"):
        gid = group_id or f"test-group-{uuid.uuid4().hex[:8]}"
        c = OrderConsumer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=gid,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=False,
        )
        consumers.append(c)
        return c

    yield _make

    for c in consumers:
        c.close()
