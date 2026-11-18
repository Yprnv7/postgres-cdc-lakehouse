"""Analytical Lakehouse Storage Engine backed by DuckDB.

Manages raw CDC event logs, SCD Type 2 dimension historical modeling,
fact table deduplication by LSN, and dead-letter queue records.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import duckdb

from consumer.models import DeadLetterRecord


def compute_diff_hash(*values: Optional[str]) -> str:
    """Compute MD5/SHA-256 fingerprint for SCD Type 2 tracked change detection."""
    combined = "|".join((v or "") for v in values)
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()[:16]


class LakehouseStorage:
    """DuckDB Lakehouse Storage Engine."""

    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self.con = duckdb.connect(db_path)
        self._init_schema()

    def _init_schema(self) -> None:
        """Initialize all raw staging, analytical mart, and DLQ tables."""
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS raw_customers_cdc (
                customer_id VARCHAR,
                name VARCHAR,
                email_hash VARCHAR,
                phone_hash VARCHAR,
                tier VARCHAR,
                address_street VARCHAR,
                address_city VARCHAR,
                address_country VARCHAR,
                _cdc_op VARCHAR,
                _cdc_lsn BIGINT,
                _cdc_ts_ms BIGINT,
                _processed_at TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS raw_orders_cdc (
                order_id VARCHAR,
                customer_id VARCHAR,
                status VARCHAR,
                amount_cents BIGINT,
                currency VARCHAR,
                item_count INTEGER,
                _cdc_op VARCHAR,
                _cdc_lsn BIGINT,
                _cdc_ts_ms BIGINT,
                _processed_at TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS dim_customers_scd2 (
                customer_sk VARCHAR PRIMARY KEY,
                customer_id VARCHAR,
                name VARCHAR,
                email_hash VARCHAR,
                phone_hash VARCHAR,
                tier VARCHAR,
                address_street VARCHAR,
                address_city VARCHAR,
                address_country VARCHAR,
                diff_hash VARCHAR,
                valid_from TIMESTAMP,
                valid_to TIMESTAMP,
                is_current BOOLEAN
            );

            CREATE TABLE IF NOT EXISTS fct_orders (
                order_id VARCHAR PRIMARY KEY,
                customer_id VARCHAR,
                status VARCHAR,
                amount_cents BIGINT,
                currency VARCHAR,
                item_count INTEGER,
                latest_lsn BIGINT,
                updated_at TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS dlq_records (
                dlq_id VARCHAR PRIMARY KEY,
                original_topic VARCHAR,
                payload_json VARCHAR,
                error_reason VARCHAR,
                failed_at TIMESTAMP,
                retry_count INTEGER,
                status VARCHAR
            );
        """)

    def insert_raw_customer_cdc(self, records: List[Dict[str, Any]]) -> int:
        """Batch insert raw customer CDC records and execute SCD Type 2 transitions."""
        if not records:
            return 0

        for r in records:
            self.con.execute(
                """
                INSERT INTO raw_customers_cdc VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::TIMESTAMP
                )
                """,
                [
                    r["customer_id"],
                    r["name"],
                    r["email_hash"],
                    r["phone_hash"],
                    r["tier"],
                    r["address_street"],
                    r["address_city"],
                    r["address_country"],
                    r["_cdc_op"],
                    r["_cdc_lsn"],
                    r["_cdc_ts_ms"],
                    r["_processed_at"],
                ],
            )
            self._apply_scd2_customer_transition(r)

        return len(records)

    def _apply_scd2_customer_transition(self, r: Dict[str, Any]) -> None:
        """Apply Slowly Changing Dimension (SCD) Type 2 transition.

        If tracked attributes (tier, address) changed or record is new,
        expires the current version and inserts a new current version.
        If deleted ('d'), expires current version with no replacement.
        """
        cid = r["customer_id"]
        op = r["_cdc_op"]
        ts_ms = r.get("_cdc_ts_ms", int(datetime.now(timezone.utc).timestamp() * 1000))
        event_time = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).replace(tzinfo=None)

        current = self.con.execute(
            """
            SELECT customer_sk, diff_hash, valid_from
            FROM dim_customers_scd2
            WHERE customer_id = ? AND is_current = TRUE
            """,
            [cid],
        ).fetchone()

        new_diff_hash = compute_diff_hash(
            r["name"],
            r["tier"],
            r["address_street"],
            r["address_city"],
            r["address_country"],
        )

        if op == "d":
            # Deletion: close current record
            if current:
                self.con.execute(
                    """
                    UPDATE dim_customers_scd2
                    SET valid_to = ?, is_current = FALSE
                    WHERE customer_sk = ?
                    """,
                    [event_time, current[0]],
                )
            return

        if not current:
            # First appearance of customer
            sk = f"{cid}_{int(event_time.timestamp() * 1000)}"
            self.con.execute(
                """
                INSERT INTO dim_customers_scd2 VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, TRUE
                )
                """,
                [
                    sk,
                    cid,
                    r["name"],
                    r["email_hash"],
                    r["phone_hash"],
                    r["tier"],
                    r["address_street"],
                    r["address_city"],
                    r["address_country"],
                    new_diff_hash,
                    event_time,
                ],
            )
        else:
            prev_sk, prev_diff_hash, _ = current
            # Check if tracked attributes actually changed
            if prev_diff_hash != new_diff_hash:
                # Expire previous version
                self.con.execute(
                    """
                    UPDATE dim_customers_scd2
                    SET valid_to = ?, is_current = FALSE
                    WHERE customer_sk = ?
                    """,
                    [event_time, prev_sk],
                )
                # Insert new active version
                sk = f"{cid}_{int(event_time.timestamp() * 1000)}"
                self.con.execute(
                    """
                    INSERT INTO dim_customers_scd2 VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, TRUE
                    )
                    """,
                    [
                        sk,
                        cid,
                        r["name"],
                        r["email_hash"],
                        r["phone_hash"],
                        r["tier"],
                        r["address_street"],
                        r["address_city"],
                        r["address_country"],
                        new_diff_hash,
                        event_time,
                    ],
                )

    def insert_raw_order_cdc(self, records: List[Dict[str, Any]]) -> int:
        """Batch insert raw order CDC records and update deduplicated fact table."""
        if not records:
            return 0

        for r in records:
            self.con.execute(
                """
                INSERT INTO raw_orders_cdc VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::TIMESTAMP
                )
                """,
                [
                    r["order_id"],
                    r["customer_id"],
                    r["status"],
                    r["amount_cents"],
                    r["currency"],
                    r["item_count"],
                    r["_cdc_op"],
                    r["_cdc_lsn"],
                    r["_cdc_ts_ms"],
                    r["_processed_at"],
                ],
            )
            self._upsert_fct_order(r)

        return len(records)

    def _upsert_fct_order(self, r: Dict[str, Any]) -> None:
        """Deduplicate orders by order_id keeping the highest LSN."""
        oid = r["order_id"]
        lsn = r["_cdc_lsn"]
        op = r["_cdc_op"]

        if op == "d":
            self.con.execute("DELETE FROM fct_orders WHERE order_id = ?", [oid])
            return

        existing = self.con.execute(
            "SELECT latest_lsn FROM fct_orders WHERE order_id = ?", [oid]
        ).fetchone()

        if not existing:
            self.con.execute(
                """
                INSERT INTO fct_orders VALUES (?, ?, ?, ?, ?, ?, ?, ?::TIMESTAMP)
                """,
                [
                    oid,
                    r["customer_id"],
                    r["status"],
                    r["amount_cents"],
                    r["currency"],
                    r["item_count"],
                    lsn,
                    r["_processed_at"],
                ],
            )
        elif lsn >= existing[0]:
            self.con.execute(
                """
                UPDATE fct_orders
                SET customer_id = ?, status = ?, amount_cents = ?, currency = ?,
                    item_count = ?, latest_lsn = ?, updated_at = ?::TIMESTAMP
                WHERE order_id = ?
                """,
                [
                    r["customer_id"],
                    r["status"],
                    r["amount_cents"],
                    r["currency"],
                    r["item_count"],
                    lsn,
                    r["_processed_at"],
                    oid,
                ],
            )

    def record_dlq_event(self, dlq_record: DeadLetterRecord) -> None:
        """Persist poisoned payload to DLQ table with error context."""
        self.con.execute(
            """
            INSERT INTO dlq_records VALUES (?, ?, ?, ?, ?::TIMESTAMP, ?, ?)
            """,
            [
                dlq_record.dlq_id,
                dlq_record.original_topic,
                json.dumps(dlq_record.payload),
                dlq_record.error_reason,
                dlq_record.failed_at,
                dlq_record.retry_count,
                dlq_record.status,
            ],
        )

    def get_dlq_records(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """Retrieve DLQ records optionally filtered by status."""
        query = "SELECT dlq_id, original_topic, payload_json, error_reason, failed_at, retry_count, status FROM dlq_records"
        params: List[Any] = []
        if status:
            query += " WHERE status = ?"
            params.append(status)
        query += " ORDER BY failed_at DESC"

        rows = self.con.execute(query, params).fetchall()
        return [
            {
                "dlq_id": row[0],
                "original_topic": row[1],
                "payload": json.loads(row[2]),
                "error_reason": row[3],
                "failed_at": str(row[4]),
                "retry_count": row[5],
                "status": row[6],
            }
            for row in rows
        ]

    def update_dlq_status(self, dlq_id: str, new_status: str, increment_retry: bool = True) -> bool:
        """Update triage state of a DLQ record."""
        retry_inc = 1 if increment_retry else 0
        res = self.con.execute(
            """
            UPDATE dlq_records
            SET status = ?, retry_count = retry_count + ?
            WHERE dlq_id = ?
            """,
            [new_status, retry_inc, dlq_id],
        )
        return res.fetchall() is not None

    def query_customer_history(self, customer_id: str) -> List[Dict[str, Any]]:
        """Retrieve full audit trail of SCD Type 2 dimension states for a customer."""
        rows = self.con.execute(
            """
            SELECT customer_sk, customer_id, name, email_hash, tier,
                   address_street, address_city, address_country,
                   valid_from, valid_to, is_current
            FROM dim_customers_scd2
            WHERE customer_id = ?
            ORDER BY valid_from ASC
            """,
            [customer_id],
        ).fetchall()
        return [
            {
                "customer_sk": row[0],
                "customer_id": row[1],
                "name": row[2],
                "email_hash": row[3],
                "tier": row[4],
                "address_street": row[5],
                "address_city": row[6],
                "address_country": row[7],
                "valid_from": str(row[8]),
                "valid_to": str(row[9]) if row[9] else None,
                "is_current": bool(row[10]),
            }
            for row in rows
        ]

    def _scalar_query(self, sql: str) -> int:
        """Helper to safely extract integer scalar count from DuckDB query."""
        row = self.con.execute(sql).fetchone()
        return int(row[0]) if row and len(row) > 0 else 0

    def get_metrics(self) -> Dict[str, Any]:
        """Lakehouse aggregate stats."""
        raw_cust = self._scalar_query("SELECT COUNT(*) FROM raw_customers_cdc")
        raw_ord = self._scalar_query("SELECT COUNT(*) FROM raw_orders_cdc")
        dim_cust = self._scalar_query("SELECT COUNT(*) FROM dim_customers_scd2")
        active_cust = self._scalar_query(
            "SELECT COUNT(*) FROM dim_customers_scd2 WHERE is_current = TRUE"
        )
        fct_ord = self._scalar_query("SELECT COUNT(*) FROM fct_orders")
        dlq_count = self._scalar_query("SELECT COUNT(*) FROM dlq_records")

        return {
            "raw_customers_cdc": raw_cust,
            "raw_orders_cdc": raw_ord,
            "dim_customers_scd2_total": dim_cust,
            "dim_customers_active": active_cust,
            "fct_orders": fct_ord,
            "dlq_records": dlq_count,
        }

    def close(self) -> None:
        """Close connection."""
        self.con.close()
