"""Unit and integration tests for Dead-Letter Queue (DLQ) routing, triage, and replay."""

import pytest

from consumer.dlq_replayer import DLQManager
from consumer.sink_worker import CDCSinkWorker
from consumer.storage import LakehouseStorage


@pytest.fixture
def dlq_setup():
    storage = LakehouseStorage(db_path=":memory:")
    worker = CDCSinkWorker(storage=storage)
    manager = DLQManager(storage=storage)
    yield storage, worker, manager
    storage.close()


def test_corrupt_negative_amount_routed_to_dlq(dlq_setup):
    """Negative order amount violates data contract and must be routed to DLQ without crashing."""
    storage, worker, _ = dlq_setup

    poison_order = {
        "order_id": "ord_bad_01",
        "customer_id": "cust_100",
        "status": "CREATED",
        "amount_cents": -9999,
        "currency": "EUR",
        "__op": "c",
    }
    ok, err = worker.process_record("orders.events", poison_order)
    assert ok is False
    assert err is not None
    assert "gt=0" in err or "Order contract violation" in err

    dlqs = storage.get_dlq_records()
    assert len(dlqs) == 1
    assert dlqs[0]["original_topic"] == "orders.events"
    assert dlqs[0]["status"] == "PENDING"
    assert dlqs[0]["payload"]["order_id"] == "ord_bad_01"


def test_malformed_email_routed_to_dlq(dlq_setup):
    """Missing domain/at in email triggers Pydantic validator and routes to DLQ."""
    storage, worker, _ = dlq_setup

    poison_customer = {
        "customer_id": "cust_bad_email",
        "name": "Bad Email Guy",
        "email": "not-an-email",
        "tier": "STANDARD",
        "__op": "c",
    }
    ok, err = worker.process_record("customers.events", poison_customer)
    assert ok is False
    assert "Invalid email" in err or "Schema contract" in err

    dlqs = storage.get_dlq_records()
    assert len(dlqs) == 1
    assert dlqs[0]["original_topic"] == "customers.events"


def test_malformed_raw_json_routed_to_dlq(dlq_setup):
    """Completely unparseable raw bytes or corrupted strings must be routed to DLQ."""
    storage, worker, _ = dlq_setup

    broken_string = "{\"order_id\": 'broken quote"
    ok, err = worker.process_record("orders.events", broken_string)
    assert ok is False
    assert "JSON parse error" in err

    dlqs = storage.get_dlq_records()
    assert len(dlqs) == 1
    assert "raw" in dlqs[0]["payload"]


def test_dlq_repair_and_replay_lifecycle(dlq_setup):
    """Full DLQ triage lifecycle: Corrupt payload -> DLQ -> Repair field -> Replay -> Lakehouse insertion."""
    storage, worker, manager = dlq_setup

    # 1. Inject corrupted order
    poison_order = {
        "order_id": "ord_recoverable_01",
        "customer_id": "cust_777",
        "status": "CREATED",
        "amount_cents": -500,  # Invalid
        "currency": "EUR",
        "__op": "c",
    }
    worker.process_record("orders.events", poison_order)
    dlq_records = manager.list_records(status="PENDING")
    assert len(dlq_records) == 1
    target_dlq_id = dlq_records[0]["dlq_id"]

    # 2. Attempt replay before repair (should fail again and increment retry)
    replay_res1 = manager.replay_record(target_dlq_id)
    assert replay_res1 is False
    rec_after_first_retry = manager.get_record(target_dlq_id)
    assert rec_after_first_retry["retry_count"] == 1
    assert rec_after_first_retry["status"] == "PENDING"

    # 3. Repair the payload: set valid positive amount
    manager.repair_payload(target_dlq_id, {"amount_cents": 25000})

    # 4. Replay after repair
    replay_res2 = manager.replay_record(target_dlq_id)
    assert replay_res2 is True

    # 5. Check DLQ status transitioned to REPLAYED
    rec_after_success = manager.get_record(target_dlq_id)
    assert rec_after_success["status"] == "REPLAYED"

    # 6. Verify row now exists in analytical fct_orders
    row = storage.con.execute(
        "SELECT order_id, amount_cents FROM fct_orders WHERE order_id = 'ord_recoverable_01'"
    ).fetchone()
    assert row is not None
    assert row[1] == 25000


def test_dlq_quarantine_action(dlq_setup):
    """Unrecoverable poisoned records can be marked as QUARANTINED."""
    _, worker, manager = dlq_setup

    worker.process_record("orders.events", {"bad": "data"})
    dlqs = manager.list_records(status="PENDING")
    assert len(dlqs) == 1
    dlq_id = dlqs[0]["dlq_id"]

    manager.quarantine_record(dlq_id)
    rec = manager.get_record(dlq_id)
    assert rec["status"] == "QUARANTINED"
