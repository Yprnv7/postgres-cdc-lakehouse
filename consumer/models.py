"""Data contracts and schemas for PostgreSQL Change Data Capture events."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, Field, field_validator

DEFAULT_SALT = "enterprise_gdpr_compliance_salt_2026"


def hash_pii(value: Optional[str], salt: str = DEFAULT_SALT) -> Optional[str]:
    """Deterministically hash sensitive PII (email, phone) using salted SHA-256.

    Complies with EU GDPR Pseudonymization (Recital 28) and India DPDP 2023.
    Raw PII is never persisted in downstream lakehouse analytical tables.
    """
    if not value or not value.strip():
        return None
    normalized = value.strip().lower()
    hasher = hashlib.sha256()
    hasher.update(salt.encode("utf-8"))
    hasher.update(normalized.encode("utf-8"))
    return f"sha256:{hasher.hexdigest()}"


class CDCEnvelope(BaseModel):
    """Debezium CDC Envelope metadata."""

    op: Literal["c", "u", "d", "r"] = Field(
        ..., description="c=create, u=update, d=delete, r=snapshot read"
    )
    table: Optional[str] = None
    lsn: int = Field(default=0, description="PostgreSQL Log Sequence Number")
    ts_ms: int = Field(default_factory=lambda: int(datetime.now(timezone.utc).timestamp() * 1000))


class CustomerEvent(BaseModel):
    """Customer CDC record contract."""

    customer_id: str = Field(..., min_length=1)
    name: str = Field(..., min_length=1)
    email: str = Field(..., min_length=3)
    phone: Optional[str] = None
    tier: str = Field(default="STANDARD")
    address_street: Optional[str] = None
    address_city: Optional[str] = None
    address_country: Optional[str] = None
    cdc_op: Literal["c", "u", "d", "r"] = "c"
    cdc_lsn: int = 0
    cdc_ts_ms: int = Field(default_factory=lambda: int(datetime.now(timezone.utc).timestamp() * 1000))

    @field_validator("email")
    @classmethod
    def validate_email_format(cls, v: str) -> str:
        if "@" not in v or "." not in v:
            raise ValueError(f"Invalid email address: {v}")
        return v

    def to_lakehouse_row(self, salt: str = DEFAULT_SALT) -> Dict[str, Any]:
        """Convert to sanitized warehouse dictionary with hashed PII."""
        return {
            "customer_id": self.customer_id,
            "name": self.name,
            "email_hash": hash_pii(self.email, salt=salt),
            "phone_hash": hash_pii(self.phone, salt=salt) if self.phone else None,
            "tier": self.tier,
            "address_street": self.address_street,
            "address_city": self.address_city,
            "address_country": self.address_country,
            "_cdc_op": self.cdc_op,
            "_cdc_lsn": self.cdc_lsn,
            "_cdc_ts_ms": self.cdc_ts_ms,
            "_processed_at": datetime.now(timezone.utc).isoformat(),
        }


VALID_ORDER_STATUSES = {
    "CREATED",
    "PAID",
    "SHIPPED",
    "DELIVERED",
    "CANCELLED",
    "REFUNDED",
}


class OrderEvent(BaseModel):
    """Order CDC record contract."""

    order_id: str = Field(..., min_length=1)
    customer_id: str = Field(..., min_length=1)
    status: str = Field(...)
    amount_cents: int = Field(..., gt=0, description="Order total in minor currency units (cents)")
    currency: str = Field(default="EUR", min_length=3, max_length=3)
    item_count: int = Field(default=1, ge=1)
    cdc_op: Literal["c", "u", "d", "r"] = "c"
    cdc_lsn: int = 0
    cdc_ts_ms: int = Field(default_factory=lambda: int(datetime.now(timezone.utc).timestamp() * 1000))

    @field_validator("status")
    @classmethod
    def validate_order_status(cls, v: str) -> str:
        upper = v.upper().strip()
        if upper not in VALID_ORDER_STATUSES:
            raise ValueError(
                f"Invalid order status '{v}'. Must be one of: {sorted(VALID_ORDER_STATUSES)}"
            )
        return upper

    def to_lakehouse_row(self) -> Dict[str, Any]:
        """Convert to warehouse dictionary format."""
        return {
            "order_id": self.order_id,
            "customer_id": self.customer_id,
            "status": self.status,
            "amount_cents": self.amount_cents,
            "currency": self.currency,
            "item_count": self.item_count,
            "_cdc_op": self.cdc_op,
            "_cdc_lsn": self.cdc_lsn,
            "_cdc_ts_ms": self.cdc_ts_ms,
            "_processed_at": datetime.now(timezone.utc).isoformat(),
        }


class DeadLetterRecord(BaseModel):
    """Dead-Letter Queue payload containing corrupted event and audit reason."""

    dlq_id: str
    original_topic: str
    payload: Dict[str, Any]
    error_reason: str
    failed_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    retry_count: int = 0
    status: Literal["PENDING", "REPLAYED", "QUARANTINED"] = "PENDING"
