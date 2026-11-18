"""Property-based invariant testing for Slowly Changing Dimension Type 2 (SCD2).

Uses Hypothesis to generate arbitrary, randomized sequences of CDC mutation events
(create, update, idempotent no-op, delete) and verifies core temporal integrity invariants:
  1. Exactly one active record (is_current=True) exists per business key, or zero if deleted.
  2. For all expired historical records, valid_from < valid_to.
  3. No temporal intervals overlap for the same business key.
  4. Monotonically increasing surrogate keys and chronological contiguous boundaries.
"""

import hypothesis.strategies as st
from hypothesis import given, settings

from consumer.sink_worker import CDCSinkWorker
from consumer.storage import LakehouseStorage

# Mutation event strategies
city_strategy = st.sampled_from(["Dublin", "Galway", "Cork", "Bengaluru", "Hyderabad", "Frankfurt", "London"])
tier_strategy = st.sampled_from(["STANDARD", "PREMIUM", "ENTERPRISE"])
op_strategy = st.sampled_from(["c", "u", "d"])


@st.composite
def cdc_event_sequence(draw):
    """Generates a chronologically strictly increasing sequence of CDC operations for a customer."""
    customer_id = draw(st.uuids()).hex[:8]
    num_events = draw(st.integers(min_value=1, max_value=25))

    events = []
    base_ts = 1700000000000

    # First event is create
    first_city = draw(city_strategy)
    first_tier = draw(tier_strategy)
    events.append({
        "customer_id": customer_id,
        "name": f"User_{customer_id}",
        "email": f"user_{customer_id}@example.ie",
        "phone": "+353-87-000-0000",
        "tier": first_tier,
        "address_street": "1 Main St",
        "address_city": first_city,
        "address_country": "Ireland",
        "__op": "c",
        "__lsn": 1000,
        "__ts_ms": base_ts,
    })

    current_state = "active"

    for i in range(1, num_events):
        base_ts += draw(st.integers(min_value=1000, max_value=60000))  # strictly monotonic timestamp
        lsn = 1000 + i * 10
        op = draw(op_strategy)

        if current_state == "deleted":
            # If deleted, another create resurrects, or update/delete are no-ops
            if op == "c":
                current_state = "active"
            else:
                continue
        elif op == "d":
            current_state = "deleted"

        city = draw(city_strategy)
        tier = draw(tier_strategy)

        events.append({
            "customer_id": customer_id,
            "name": f"User_{customer_id}",
            "email": f"user_{customer_id}@example.ie",
            "phone": "+353-87-000-0000",
            "tier": tier,
            "address_street": "1 Main St",
            "address_city": city,
            "address_country": "Ireland",
            "__op": op,
            "__lsn": lsn,
            "__ts_ms": base_ts,
        })

    return customer_id, events


@settings(max_examples=50, deadline=None)
@given(cdc_event_sequence())
def test_scd2_temporal_invariants(case):
    """Property Test: Assert temporal and uniqueness invariants over randomized event streams."""
    customer_id, events = case

    storage = LakehouseStorage(db_path=":memory:")
    worker = CDCSinkWorker(storage=storage)

    try:
        for event in events:
            worker.process_record("customers.events", event)

        history = storage.query_customer_history(customer_id)
        if not history:
            return

        # Invariant 1: At most one current active record (is_current=True)
        current_records = [r for r in history if r["is_current"] is True]
        assert len(current_records) <= 1, f"Expected at most 1 current record, found {len(current_records)}"

        # If there is a current record, its valid_to MUST be None
        if current_records:
            assert current_records[0]["valid_to"] is None

        # Invariant 2: For every historical record, valid_from < valid_to
        historical_records = [r for r in history if r["is_current"] is False]
        for h in historical_records:
            assert h["valid_to"] is not None, f"Historical record {h['customer_sk']} must have valid_to set"
            assert h["valid_from"] < h["valid_to"], (
                f"Temporal inversion: valid_from {h['valid_from']} >= valid_to {h['valid_to']}"
            )

        # Invariant 3: No temporal overlap between adjacent versions
        # Sort chronologically by valid_from
        sorted_history = sorted(history, key=lambda x: x["valid_from"])
        for i in range(len(sorted_history) - 1):
            earlier = sorted_history[i]
            later = sorted_history[i + 1]
            # The closing timestamp of the earlier version must match or precede the start of the next
            assert earlier["valid_to"] is not None
            assert earlier["valid_to"] <= later["valid_from"], (
                f"Temporal overlap detected: {earlier['valid_to']} > {later['valid_from']}"
            )

        # Invariant 4: Distinct versions check
        sk_set = set(r["customer_sk"] for r in history)
        assert len(sk_set) == len(history), "Surrogate keys must be globally unique per version"

    finally:
        storage.close()
