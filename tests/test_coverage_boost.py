"""Comprehensive test suite covering DLQ CLI, batch workers, and metric aggregations."""

import pytest

from consumer.backfill_manager import BackfillManager
from consumer.dlq_replayer import DLQManager, main
from consumer.sink_worker import CDCSinkWorker
from consumer.storage import LakehouseStorage


@pytest.fixture
def fixture_env():
    storage = LakehouseStorage(db_path=":memory:")
    worker = CDCSinkWorker(storage=storage)
    manager = DLQManager(storage=storage)
    yield storage, worker, manager
    storage.close()


def test_sink_worker_batch_processing(fixture_env):
    """Verify high-throughput batch processing of mixed customer, order, and corrupt items."""
    storage, worker, _ = fixture_env

    batch = [
        ("customers.events", {
            "customer_id": "c_batch_1",
            "name": "Batch User 1",
            "email": "batch1@corp.eu",
            "tier": "STANDARD",
            "__op": "c",
        }, 100),
        ("orders.events", {
            "order_id": "o_batch_1",
            "customer_id": "c_batch_1",
            "status": "CREATED",
            "amount_cents": 15000,
            "currency": "EUR",
            "__op": "c",
        }, 101),
        ("orders.events", {
            "order_id": "o_batch_bad",
            "customer_id": "c_batch_1",
            "status": "INVALID_STATUS",
            "amount_cents": 100,
        }, 102),
        ("unknown_topic", {"dummy": 123}, 103),
    ]

    res = worker.process_batch(batch)
    assert res["total"] == 4
    assert res["success"] == 2
    assert res["dlq"] == 2


def test_sink_worker_unsupported_types(fixture_env):
    """Verify handling of numbers or arbitrary types passed as payload."""
    _, worker, _ = fixture_env
    ok, err = worker.process_record("orders.events", 12345)
    assert ok is False
    assert "Unsupported payload type" in err


def test_dlq_manager_print_table_and_replay_all(fixture_env):
    """Test DLQ manager table output formatting and bulk replay functionality."""
    storage, worker, manager = fixture_env

    # 1. Print empty table
    manager.print_table()

    # 2. Add recoverable poisoned order
    worker.process_record("orders.events", {
        "order_id": "o_bulk_1",
        "customer_id": "c_1",
        "status": "CREATED",
        "amount_cents": -10,
    })
    manager.print_table(status="PENDING")

    # 3. Repair and bulk replay
    records = manager.list_records(status="PENDING")
    assert len(records) == 1
    dlq_id = records[0]["dlq_id"]

    manager.repair_payload(dlq_id, {"amount_cents": 9999})
    bulk_res = manager.replay_all_pending()
    assert bulk_res["total"] == 1
    assert bulk_res["replayed"] == 1
    assert bulk_res["failed"] == 0


def test_dlq_manager_edge_cases(fixture_env):
    """Test DLQ lookup and updates on non-existent records."""
    _, _, manager = fixture_env

    assert manager.get_record("non_existent_id") is None
    assert manager.repair_payload("non_existent_id", {"a": 1}) is False
    assert manager.replay_record("non_existent_id") is False
    assert manager.quarantine_record("non_existent_id") is False


def test_backfill_stream_batch_and_unsupported_table(fixture_env):
    """Verify batch reconciliation and error handling on unrecognized tables."""
    storage, _, _ = fixture_env
    backfill = BackfillManager(storage=storage)

    # Unknown table
    loaded = backfill.load_historical_snapshot("unknown_table", [{"x": 1}], 100)
    assert loaded == 0

    # Batch reconciliation
    stream = [
        ("orders.events", {
            "order_id": "o_recon_1",
            "customer_id": "c_1",
            "status": "CREATED",
            "amount_cents": 5000,
            "currency": "EUR",
        }, 105),
        ("orders.events", {"bad": "order"}, 106),
    ]
    res = backfill.reconcile_stream_batch(stream)
    assert res["reconciled"] == 1
    assert res["failed"] == 1


def test_storage_metrics_and_empty_batches(fixture_env):
    """Verify metrics calculation and no-op empty batch inserts."""
    storage, _, _ = fixture_env
    assert storage.insert_raw_customer_cdc([]) == 0
    assert storage.insert_raw_order_cdc([]) == 0

    metrics = storage.get_metrics()
    assert "raw_orders_cdc" in metrics
    assert "dim_customers_scd2_total" in metrics
    assert "dlq_records" in metrics


def test_dlq_cli_main_dispatch(monkeypatch):
    """Verify CLI main() dispatches subcommands properly."""
    # Test without args (exits 0)
    monkeypatch.setattr("sys.argv", ["dlq_replayer.py"])
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == 0

    # Test list
    monkeypatch.setattr("sys.argv", ["dlq_replayer.py", "--db", ":memory:", "list"])
    main()

    # Test show non-existent
    monkeypatch.setattr("sys.argv", ["dlq_replayer.py", "--db", ":memory:", "show", "missing_id"])
    main()

    # Test replay non-existent
    monkeypatch.setattr("sys.argv", ["dlq_replayer.py", "--db", ":memory:", "replay", "missing_id"])
    main()

    # Test replay-all
    monkeypatch.setattr("sys.argv", ["dlq_replayer.py", "--db", ":memory:", "replay-all"])
    main()

    # Test quarantine non-existent
    monkeypatch.setattr("sys.argv", ["dlq_replayer.py", "--db", ":memory:", "quarantine", "missing_id"])
    main()
