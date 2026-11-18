"""Unit and integration tests for snapshot + streaming watermark backfill reconciliation."""

import pytest

from consumer.backfill_manager import BackfillManager
from consumer.storage import LakehouseStorage


@pytest.fixture
def backfill_setup():
    storage = LakehouseStorage(db_path=":memory:")
    manager = BackfillManager(storage=storage)
    yield storage, manager
    storage.close()


def test_dual_run_backfill_reconciliation(backfill_setup):
    """Historical snapshot up to watermark LSN followed by streaming events correctly deduplicates and updates."""
    storage, manager = backfill_setup

    snapshot_orders = [
        {
            "order_id": "ord_hist_1",
            "customer_id": "cust_1",
            "status": "CREATED",
            "amount_cents": 10000,
            "currency": "EUR",
            "item_count": 1,
        },
        {
            "order_id": "ord_hist_2",
            "customer_id": "cust_2",
            "status": "PAID",
            "amount_cents": 25000,
            "currency": "EUR",
            "item_count": 2,
        },
    ]

    # 1. Load historical snapshot at watermark LSN = 500
    loaded = manager.load_historical_snapshot(
        table_name="orders", rows=snapshot_orders, snapshot_watermark_lsn=500
    )
    assert loaded == 2

    # Verify orders exist in fct_orders
    cnt = storage.con.execute("SELECT COUNT(*) FROM fct_orders").fetchone()[0]
    assert cnt == 2

    # 2. Replay an event from stream that occurred before watermark (e.g. LSN = 450)
    # Status in old stream was 'CREATED', but current in fact table is 'PAID' (LSN 500)
    old_event = {
        "order_id": "ord_hist_2",
        "customer_id": "cust_2",
        "status": "CREATED",
        "amount_cents": 25000,
        "currency": "EUR",
        "item_count": 2,
        "__op": "u",
        "__lsn": 450,
    }
    manager.reconcile_streaming_event("orders.events", old_event, event_lsn=450)

    # Fact table should still retain 'PAID' because LSN 500 > 450!
    row = storage.con.execute(
        "SELECT status, latest_lsn FROM fct_orders WHERE order_id = 'ord_hist_2'"
    ).fetchone()
    assert row[0] == "PAID"
    assert row[1] == 500

    # 3. Stream event that occurred strictly AFTER watermark (LSN = 550)
    newer_event = {
        "order_id": "ord_hist_2",
        "customer_id": "cust_2",
        "status": "SHIPPED",
        "amount_cents": 25000,
        "currency": "EUR",
        "item_count": 2,
        "__op": "u",
        "__lsn": 550,
    }
    manager.reconcile_streaming_event("orders.events", newer_event, event_lsn=550)

    # Fact table should now be updated to 'SHIPPED' with LSN 550
    row_new = storage.con.execute(
        "SELECT status, latest_lsn FROM fct_orders WHERE order_id = 'ord_hist_2'"
    ).fetchone()
    assert row_new[0] == "SHIPPED"
    assert row_new[1] == 550
