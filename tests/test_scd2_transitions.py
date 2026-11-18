"""Unit and integration tests for Slowly Changing Dimension Type 2 (SCD2) transitions."""

import pytest

from consumer.sink_worker import CDCSinkWorker
from consumer.storage import LakehouseStorage


@pytest.fixture
def test_storage():
    storage = LakehouseStorage(db_path=":memory:")
    yield storage
    storage.close()


@pytest.fixture
def test_worker(test_storage):
    return CDCSinkWorker(storage=test_storage)


def test_initial_customer_creation(test_worker, test_storage):
    """Initial customer CDC insert should create an active SCD2 record with is_current=True."""
    payload = {
        "customer_id": "cust_100",
        "name": "Jane Doe",
        "email": "jane@enterprise.ie",
        "phone": "+353-87-123-4567",
        "tier": "STANDARD",
        "address_street": "1 Grand Canal",
        "address_city": "Dublin",
        "address_country": "Ireland",
        "__op": "c",
        "__lsn": 100,
        "__ts_ms": 1700000000000,
    }
    ok, err = test_worker.process_record("customers.events", payload)
    assert ok is True
    assert err is None

    history = test_storage.query_customer_history("cust_100")
    assert len(history) == 1
    assert history[0]["is_current"] is True
    assert history[0]["valid_to"] is None
    assert history[0]["address_city"] == "Dublin"
    assert history[0]["tier"] == "STANDARD"


def test_customer_attribute_mutation_creates_scd2_version(test_worker, test_storage):
    """Updating tracked attributes (address/tier) must expire the old version and create a new one."""
    # 1. Base record
    p1 = {
        "customer_id": "cust_200",
        "name": "Robert Smith",
        "email": "robert@dublin.ie",
        "tier": "STANDARD",
        "address_city": "Dublin",
        "address_country": "Ireland",
        "__op": "c",
        "__lsn": 200,
        "__ts_ms": 1700000000000,
    }
    test_worker.process_record("customers.events", p1)

    # 2. Update to Frankfurt
    p2 = {
        "customer_id": "cust_200",
        "name": "Robert Smith",
        "email": "robert@dublin.ie",
        "tier": "PREMIUM",
        "address_city": "Frankfurt",
        "address_country": "Germany",
        "__op": "u",
        "__lsn": 205,
        "__ts_ms": 1700000500000,
    }
    ok, err = test_worker.process_record("customers.events", p2)
    assert ok is True

    history = test_storage.query_customer_history("cust_200")
    assert len(history) == 2

    # First record should be closed
    v1 = history[0]
    assert v1["is_current"] is False
    assert v1["valid_to"] is not None
    assert v1["address_city"] == "Dublin"
    assert v1["tier"] == "STANDARD"

    # Second record should be current active
    v2 = history[1]
    assert v2["is_current"] is True
    assert v2["valid_to"] is None
    assert v2["address_city"] == "Frankfurt"
    assert v2["tier"] == "PREMIUM"


def test_no_op_mutation_is_idempotent(test_worker, test_storage):
    """An update with identical tracked attributes must NOT create redundant dimension rows."""
    p1 = {
        "customer_id": "cust_300",
        "name": "Alice Brown",
        "email": "alice@bengaluru.in",
        "tier": "ENTERPRISE",
        "address_city": "Bengaluru",
        "address_country": "India",
        "__op": "c",
        "__lsn": 300,
        "__ts_ms": 1700000000000,
    }
    test_worker.process_record("customers.events", p1)

    # Send exact same values again with newer LSN
    p2 = dict(p1)
    p2["__op"] = "u"
    p2["__lsn"] = 305
    p2["__ts_ms"] = 1700000100000
    test_worker.process_record("customers.events", p2)

    history = test_storage.query_customer_history("cust_300")
    assert len(history) == 1
    assert history[0]["is_current"] is True


def test_customer_deletion_expires_active_record(test_worker, test_storage):
    """Deletion CDC event ('d') must close active record leaving zero current active rows."""
    p1 = {
        "customer_id": "cust_400",
        "name": "To Be Deleted",
        "email": "delete@gdpr.eu",
        "tier": "STANDARD",
        "address_city": "Dublin",
        "address_country": "Ireland",
        "__op": "c",
        "__lsn": 400,
        "__ts_ms": 1700000000000,
    }
    test_worker.process_record("customers.events", p1)

    p_del = {
        "customer_id": "cust_400",
        "name": "To Be Deleted",
        "email": "delete@gdpr.eu",
        "__op": "d",
        "__lsn": 410,
        "__ts_ms": 1700000600000,
    }
    ok, _ = test_worker.process_record("customers.events", p_del)
    assert ok is True

    history = test_storage.query_customer_history("cust_400")
    assert len(history) == 1
    assert history[0]["is_current"] is False
    assert history[0]["valid_to"] is not None

    # Verify no active current customer in analytical mart
    active = test_storage.con.execute(
        "SELECT COUNT(*) FROM dim_customers_scd2 WHERE customer_id = 'cust_400' AND is_current = TRUE"
    ).fetchone()[0]
    assert active == 0
