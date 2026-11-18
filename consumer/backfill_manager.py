"""Dual-run backfill manager: Historical snapshot + streaming CDC watermark reconciliation.

Solves the classic data engineering challenge: Backfilling billions of historical rows
while real-time CDC mutations are actively flowing, ensuring zero data loss and
zero duplicate counting.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from consumer.sink_worker import CDCSinkWorker
from consumer.storage import LakehouseStorage

logger = logging.getLogger("BackfillManager")


class BackfillManager:
    """Coordinates historical snapshot loading with streaming CDC watermark alignment."""

    def __init__(self, storage: Optional[LakehouseStorage] = None, db_path: str = ":memory:"):
        self.storage = storage or LakehouseStorage(db_path=db_path)
        self.worker = CDCSinkWorker(storage=self.storage)
        self.watermark_lsn: int = 0
        self.snapshot_completed: bool = False

    def load_historical_snapshot(
        self,
        table_name: str,
        rows: List[Dict[str, Any]],
        snapshot_watermark_lsn: int,
    ) -> int:
        """Load static historical rows captured up to `snapshot_watermark_lsn`.

        All rows are marked with cdc_op='r' (read/snapshot) and assigned the watermark LSN.
        """
        self.watermark_lsn = snapshot_watermark_lsn
        loaded = 0

        for r in rows:
            enriched = dict(r)
            enriched["_cdc_op"] = "r"
            enriched["_cdc_lsn"] = snapshot_watermark_lsn

            if table_name == "customers":
                ok, _ = self.worker.process_record("customers.events", enriched, lsn=snapshot_watermark_lsn)
            elif table_name == "orders":
                ok, _ = self.worker.process_record("orders.events", enriched, lsn=snapshot_watermark_lsn)
            else:
                ok = False

            if ok:
                loaded += 1

        self.snapshot_completed = True
        logger.info(
            f"Historical snapshot of '{table_name}' completed. Loaded {loaded} rows at watermark LSN={self.watermark_lsn}."
        )
        return loaded

    def reconcile_streaming_event(
        self, topic: str, payload: Dict[str, Any], event_lsn: int
    ) -> bool:
        """Process incoming live streaming event against watermark.

        If the streaming event has an LSN <= watermark_lsn and was already captured
        in the snapshot, idempotent deduplication ensures no duplicate state mutation.
        If event_lsn > watermark_lsn, it is immediately ingested as a live transition.
        """
        # If streaming event arrived with LSN before watermark, deduplicate or verify
        if self.snapshot_completed and event_lsn <= self.watermark_lsn:
            # Idempotently process; fact table upserts based on latest LSN
            ok, _ = self.worker.process_record(topic, payload, lsn=event_lsn)
            return ok

        # Event is strictly after watermark: apply live mutation
        ok, _ = self.worker.process_record(topic, payload, lsn=event_lsn)
        return ok

    def reconcile_stream_batch(
        self, stream_batch: List[tuple[str, Dict[str, Any], int]]
    ) -> Dict[str, int]:
        """Reconcile a full batch of streaming CDC events."""
        reconciled = 0
        skipped_or_failed = 0
        for topic, payload, lsn in stream_batch:
            if self.reconcile_streaming_event(topic, payload, lsn):
                reconciled += 1
            else:
                skipped_or_failed += 1
        return {"reconciled": reconciled, "failed": skipped_or_failed}
