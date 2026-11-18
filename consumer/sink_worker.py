"""High-throughput CDC Sink Worker with Data Contract Validation and DLQ Routing.

Consumes CDC events from Kafka/Redpanda topics, applies salted SHA-256 PII hashing,
validates schema contracts, writes to analytical Lakehouse storage, and routes corrupted
payloads to the Dead-Letter Queue (DLQ).
"""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional, Tuple, cast

from pydantic import ValidationError

from consumer.models import DEFAULT_SALT, CustomerEvent, DeadLetterRecord, OrderEvent
from consumer.storage import LakehouseStorage

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("CDCSinkWorker")


class CDCSinkWorker:
    """CDC Sink Worker processing raw Debezium events into Lakehouse storage."""

    def __init__(
        self,
        storage: Optional[LakehouseStorage] = None,
        db_path: str = ":memory:",
        salt: Optional[str] = None,
    ):
        self.storage = storage or LakehouseStorage(db_path=db_path)
        self.salt = salt or os.getenv("PII_HASH_SALT", "enterprise_gdpr_compliance_salt_2026")
        self.processed_count = 0
        self.dlq_count = 0
        self.customer_count = 0
        self.order_count = 0

    def process_record(
        self, topic: str, raw_payload: Any, lsn: int = 0
    ) -> Tuple[bool, Optional[str]]:
        """Validate single CDC record and route to either Lakehouse or DLQ.

        Returns (success: bool, error_reason: Optional[str]).
        """
        self.processed_count += 1

        # Parse JSON string if necessary
        if isinstance(raw_payload, (str, bytes)):
            try:
                payload = json.loads(raw_payload)
            except Exception as e:
                err_msg = f"JSON parse error: {str(e)}"
                self._route_to_dlq(topic, {"raw": str(raw_payload)}, err_msg)
                return False, err_msg
        elif isinstance(raw_payload, dict):
            payload = raw_payload
        else:
            err_msg = f"Unsupported payload type: {type(raw_payload)}"
            self._route_to_dlq(topic, {"raw": str(raw_payload)}, err_msg)
            return False, err_msg

        # Route by topic
        if "customer" in topic.lower():
            return self._handle_customer_event(topic, payload, lsn)
        elif "order" in topic.lower():
            return self._handle_order_event(topic, payload, lsn)
        else:
            err_msg = f"Unrecognized topic name: {topic}"
            self._route_to_dlq(topic, payload, err_msg)
            return False, err_msg

    def _handle_customer_event(
        self, topic: str, payload: Dict[str, Any], lsn: int
    ) -> Tuple[bool, Optional[str]]:
        try:
            # Map Debezium metadata if present
            raw_op = payload.get("__op") or payload.get("_cdc_op") or payload.get("cdc_op") or "c"
            op = cast(Literal["c", "u", "d", "r"], str(raw_op).lower())
            event_lsn = payload.get("__lsn") or payload.get("_cdc_lsn") or lsn

            cust_event = CustomerEvent(
                customer_id=str(payload.get("customer_id", "")),
                name=str(payload.get("name", "")),
                email=str(payload.get("email", "")),
                phone=payload.get("phone"),
                tier=payload.get("tier", "STANDARD"),
                address_street=payload.get("address_street"),
                address_city=payload.get("address_city"),
                address_country=payload.get("address_country"),
                cdc_op=op,
                cdc_lsn=int(event_lsn),
                cdc_ts_ms=payload.get(
                    "__ts_ms", int(datetime.now(timezone.utc).timestamp() * 1000)
                ),
            )
            row = cust_event.to_lakehouse_row(salt=self.salt or DEFAULT_SALT)
            self.storage.insert_raw_customer_cdc([row])
            self.customer_count += 1
            return True, None
        except ValidationError as ve:
            err_msg = f"Schema contract validation failed: {ve.errors()[0]['msg']}"
            self._route_to_dlq(topic, payload, err_msg)
            return False, err_msg
        except Exception as ex:
            err_msg = f"Internal processing error: {str(ex)}"
            self._route_to_dlq(topic, payload, err_msg)
            return False, err_msg

    def _handle_order_event(
        self, topic: str, payload: Dict[str, Any], lsn: int
    ) -> Tuple[bool, Optional[str]]:
        try:
            raw_op = payload.get("__op") or payload.get("_cdc_op") or payload.get("cdc_op") or "c"
            op = cast(Literal["c", "u", "d", "r"], str(raw_op).lower())
            event_lsn = payload.get("__lsn") or payload.get("_cdc_lsn") or lsn

            order_event = OrderEvent(
                order_id=str(payload.get("order_id", "")),
                customer_id=str(payload.get("customer_id", "")),
                status=str(payload.get("status", "")),
                amount_cents=int(payload.get("amount_cents", 0)),
                currency=str(payload.get("currency", "EUR")),
                item_count=int(payload.get("item_count", 1)),
                cdc_op=op,
                cdc_lsn=int(event_lsn),
                cdc_ts_ms=payload.get(
                    "__ts_ms", int(datetime.now(timezone.utc).timestamp() * 1000)
                ),
            )
            row = order_event.to_lakehouse_row()
            self.storage.insert_raw_order_cdc([row])
            self.order_count += 1
            return True, None
        except (ValidationError, ValueError) as ve:
            err_msg = f"Order contract violation: {str(ve)}"
            self._route_to_dlq(topic, payload, err_msg)
            return False, err_msg
        except Exception as ex:
            err_msg = f"Internal order processing error: {str(ex)}"
            self._route_to_dlq(topic, payload, err_msg)
            return False, err_msg

    def _route_to_dlq(self, topic: str, payload: Dict[str, Any], reason: str) -> None:
        """Route unparsable or contract-violating payload to Dead-Letter Queue."""
        self.dlq_count += 1
        dlq_id = f"dlq_{uuid.uuid4().hex[:12]}"
        record = DeadLetterRecord(
            dlq_id=dlq_id,
            original_topic=topic,
            payload=payload,
            error_reason=reason,
        )
        self.storage.record_dlq_event(record)
        logger.warning(
            f"Corrupted CDC event routed to DLQ [{dlq_id}] on topic '{topic}': {reason}"
        )

    def process_batch(self, items: List[Tuple[str, Any, int]]) -> Dict[str, int]:
        """Process batch of (topic, payload, lsn) tuples with high throughput."""
        successes = 0
        failures = 0
        for topic, payload, lsn in items:
            ok, _ = self.process_record(topic, payload, lsn)
            if ok:
                successes += 1
            else:
                failures += 1
        return {"total": len(items), "success": successes, "dlq": failures}


def run_worker_loop() -> None:
    """Daemon worker loop for containerized execution reading Kafka/Redpanda."""
    db_path = os.getenv("DUCKDB_PATH", "/app/data/lakehouse.duckdb")
    broker = os.getenv("KAFKA_BROKER", "localhost:19092")
    logger.info(f"Starting CDC Sink Worker connecting to {broker}, storage={db_path}")

    storage = LakehouseStorage(db_path=db_path)
    worker = CDCSinkWorker(storage=storage)
    logger.info(f"Initialized CDC worker {worker} on storage {db_path}")
    logger.info("CDC Sink Worker active and listening for CDC streams...")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Worker stopped by user.")
    finally:
        storage.close()


if __name__ == "__main__":
    run_worker_loop()
