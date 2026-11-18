"""Integration test verifying DuckDB analytical marts execution and schema invariants."""

import pytest

from consumer.sink_worker import CDCSinkWorker
from consumer.storage import LakehouseStorage


@pytest.fixture
def populated_duckdb_storage():
    """Returns a DuckDB Lakehouse storage instance pre-populated with CDC events."""
    storage = LakehouseStorage(db_path=":memory:")
    worker = CDCSinkWorker(storage=storage)

    # 1. Insert customer mutations
    c1 = {
        "customer_id": "cust_101",
        "name": "Alice Dublin",
        "email": "alice@dublin.ie",
        "phone": "+353-87-111-2222",
        "tier": "STANDARD",
        "address_street": "1 Grafton St",
        "address_city": "Dublin",
        "address_country": "Ireland",
        "__op": "c",
        "__lsn": 100,
        "__ts_ms": 1700000000000,
    }
    worker.process_record("customers.events", c1)

    c1_update = {
        "customer_id": "cust_101",
        "name": "Alice Dublin",
        "email": "alice@dublin.ie",
        "phone": "+353-87-111-2222",
        "tier": "PREMIUM",
        "address_street": "1 Grafton St",
        "address_city": "Dublin",
        "address_country": "Ireland",
        "__op": "u",
        "__lsn": 105,
        "__ts_ms": 1700000500000,
    }
    worker.process_record("customers.events", c1_update)

    # 2. Insert orders
    o1 = {
        "order_id": "ord_501",
        "customer_id": "cust_101",
        "status": "PAID",
        "amount_cents": 14500,
        "currency": "EUR",
        "item_count": 3,
        "__op": "c",
        "__lsn": 110,
        "__ts_ms": 1700000600000,
    }
    worker.process_record("orders.events", o1)

    yield storage
    storage.close()


def test_marts_schema_and_integrity_constraints(populated_duckdb_storage):
    """Verifies that DuckDB mart tables satisfy schema constraints (unique, not_null)."""
    con = populated_duckdb_storage.con

    # Test dim_customers_scd2 invariants
    dim_rows = con.execute(
        "SELECT customer_sk, customer_id, tier, valid_from, valid_to, is_current FROM dim_customers_scd2"
    ).fetchall()
    assert len(dim_rows) == 2, "Should have 2 versions for cust_101"

    sks = [r[0] for r in dim_rows]
    cids = [r[1] for r in dim_rows]
    currents = [r[5] for r in dim_rows]

    # Unique test on customer_sk
    assert len(sks) == len(set(sks)), "Constraint violation: customer_sk is not unique"

    # Not-null tests on dim_customers_scd2
    assert all(sk is not None for sk in sks), "Constraint violation: customer_sk contains nulls"
    assert all(cid is not None for cid in cids), "Constraint violation: customer_id contains nulls"
    assert all(cur is not None for cur in currents), "Constraint violation: is_current contains nulls"

    # Exactly one active record
    active_records = [r for r in dim_rows if r[5] is True]
    assert len(active_records) == 1
    assert active_records[0][2] == "PREMIUM"

    # Test fct_orders invariants
    fct_rows = con.execute(
        "SELECT order_id, customer_id, status, amount_cents FROM fct_orders"
    ).fetchall()
    assert len(fct_rows) == 1, "Should have 1 order record"

    order_ids = [r[0] for r in fct_rows]
    statuses = [r[2] for r in fct_rows]
    amounts = [r[3] for r in fct_rows]

    # Unique test on order_id
    assert len(order_ids) == len(set(order_ids)), "Constraint violation: order_id is not unique"

    # Not-null tests on fct_orders
    assert all(oid is not None for oid in order_ids), "Constraint violation: order_id contains nulls"
    assert all(amt is not None for amt in amounts), "Constraint violation: amount_cents contains nulls"
    assert all(st is not None for st in statuses), "Constraint violation: status contains nulls"
    assert amounts[0] == 14500
