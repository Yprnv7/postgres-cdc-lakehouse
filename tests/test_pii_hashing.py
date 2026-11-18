"""Unit tests for Salted SHA-256 PII Hashing & GDPR/DPDP pseudonymization."""

from consumer.models import hash_pii
from consumer.sink_worker import CDCSinkWorker
from consumer.storage import LakehouseStorage


def test_hash_pii_deterministic():
    """Identical email and salt must always produce the same SHA-256 hash."""
    email = "compliance.officer@enterprise.eu"
    salt = "salt_2026_test"
    h1 = hash_pii(email, salt=salt)
    h2 = hash_pii(email, salt=salt)
    assert h1 is not None
    assert h1 == h2
    assert h1.startswith("sha256:")
    assert len(h1) == 7 + 64


def test_hash_pii_case_and_whitespace_insensitivity():
    """Emails differing only by case or trailing space must yield identical hashes."""
    salt = "fixed_salt"
    h1 = hash_pii("Alice@Company.COM ", salt=salt)
    h2 = hash_pii("alice@company.com", salt=salt)
    assert h1 == h2


def test_different_salts_produce_different_hashes():
    """Different salt values prevent rainbow table attacks."""
    email = "target@domain.com"
    h1 = hash_pii(email, salt="salt_alpha")
    h2 = hash_pii(email, salt="salt_beta")
    assert h1 != h2


def test_empty_or_none_pii_returns_none():
    """None or empty string input returns None cleanly."""
    assert hash_pii(None) is None
    assert hash_pii("") is None
    assert hash_pii("   ") is None


def test_pipeline_never_persists_raw_pii():
    """Verify that raw email and phone are completely absent from Lakehouse tables."""
    storage = LakehouseStorage(db_path=":memory:")
    worker = CDCSinkWorker(storage=storage, salt="production_secret_salt")

    raw_email = "very_secret_ceo@multinational.ie"
    raw_phone = "+353-89-999-8888"

    payload = {
        "customer_id": "cust_privacy_01",
        "name": "Secret CEO",
        "email": raw_email,
        "phone": raw_phone,
        "tier": "ENTERPRISE",
        "address_city": "Dublin",
        "__op": "c",
    }

    ok, _ = worker.process_record("customers.events", payload)
    assert ok is True

    # 1. Inspect raw_customers_cdc
    raw_row = storage.con.execute(
        "SELECT email_hash, phone_hash FROM raw_customers_cdc WHERE customer_id = 'cust_privacy_01'"
    ).fetchone()
    assert raw_row[0] != raw_email
    assert raw_row[1] != raw_phone
    assert raw_row[0].startswith("sha256:")
    assert raw_row[1].startswith("sha256:")

    # 2. Inspect dim_customers_scd2
    dim_row = storage.con.execute(
        "SELECT email_hash, phone_hash FROM dim_customers_scd2 WHERE customer_id = 'cust_privacy_01'"
    ).fetchone()
    assert dim_row[0] != raw_email
    assert dim_row[1] != raw_phone
    assert dim_row[0].startswith("sha256:")

    storage.close()
