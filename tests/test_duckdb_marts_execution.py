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
    dim_df = con.execute("SELECT * FROM dim_customers_scd2").df()
    assert len(dim_df) == 2, "Should have 2 versions for cust_101"

    # Unique test on customer_sk
    assert dim_df["customer_sk"].is_unique, "Constraint violation: customer_sk is not unique"

    # Not-null tests on dim_customers_scd2
    assert not dim_df["customer_sk"].isnull().any(), "Constraint violation: customer_sk contains nulls"
    assert not dim_df["customer_id"].isnull().any(), "Constraint violation: customer_id contains nulls"
    assert not dim_df["is_current"].isnull().any(), "Constraint violation: is_current contains nulls"

    # Exactly one active record
    active_records = dim_df[dim_df["is_current"] == True]  # noqa: E712
    assert len(active_records) == 1
    assert active_records.iloc[0]["tier"] == "PREMIUM"

    # Test fct_orders invariants
    fct_df = con.execute("SELECT * FROM fct_orders").df()
    assert len(fct_df) == 1, "Should have 1 order record"

    # Unique test on order_id
    assert fct_df["order_id"].is_unique, "Constraint violation: order_id is not unique"

    # Not-null tests on fct_orders
    assert not fct_df["order_id"].isnull().any(), "Constraint violation: order_id contains nulls"
    assert not fct_df["amount_cents"].isnull().any(), "Constraint violation: amount_cents contains nulls"
    assert not fct_df["status"].isnull().any(), "Constraint violation: status contains nulls"
    assert fct_df.iloc[0]["amount_cents"] == 14500
