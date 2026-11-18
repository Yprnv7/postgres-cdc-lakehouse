"""CLI tool to inspect, repair, triage, and replay poisoned DLQ records."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict, List, Optional

from rich.console import Console
from rich.table import Table

from consumer.sink_worker import CDCSinkWorker
from consumer.storage import LakehouseStorage

console = Console()


class DLQManager:
    """Manages Dead-Letter Queue inspection, repair, and replay."""

    def __init__(self, db_path: str = ":memory:", storage: Optional[LakehouseStorage] = None):
        self.storage = storage or LakehouseStorage(db_path=db_path)
        self.worker = CDCSinkWorker(storage=self.storage)

    def list_records(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """List records from DLQ."""
        return self.storage.get_dlq_records(status=status)

    def print_table(self, status: Optional[str] = None) -> None:
        """Render rich terminal table of DLQ entries."""
        records = self.list_records(status=status)
        if not records:
            console.print("[green]No DLQ records found.[/green]")
            return

        table = Table(title=f"Dead-Letter Queue Records ({status or 'ALL'})")
        table.add_column("DLQ ID", style="cyan", no_wrap=True)
        table.add_column("Topic", style="magenta")
        table.add_column("Error Reason", style="red")
        table.add_column("Failed At", style="dim")
        table.add_column("Retries", justify="right")
        table.add_column("Status", style="bold yellow")

        for r in records:
            table.add_row(
                r["dlq_id"],
                r["original_topic"],
                r["error_reason"][:60],
                r["failed_at"][:19],
                str(r["retry_count"]),
                r["status"],
            )
        console.print(table)

    def get_record(self, dlq_id: str) -> Optional[Dict[str, Any]]:
        """Fetch single DLQ entry."""
        all_recs = self.storage.get_dlq_records()
        for r in all_recs:
            if r["dlq_id"] == dlq_id:
                return r
        return None

    def repair_payload(self, dlq_id: str, updates: Dict[str, Any]) -> bool:
        """Modify fields in a DLQ payload before replay."""
        rec = self.get_record(dlq_id)
        if not rec:
            console.print(f"[red]Record {dlq_id} not found.[/red]")
            return False

        payload = rec["payload"]
        if "raw" in payload:
            try:
                payload = json.loads(payload["raw"])
            except Exception:
                pass

        payload.update(updates)
        self.storage.con.execute(
            "UPDATE dlq_records SET payload_json = ? WHERE dlq_id = ?",
            [json.dumps(payload), dlq_id],
        )
        console.print(f"[green]Record {dlq_id} payload updated.[/green]")
        return True

    def replay_record(self, dlq_id: str) -> bool:
        """Re-submit DLQ payload through the sink worker."""
        rec = self.get_record(dlq_id)
        if not rec:
            console.print(f"[red]Record {dlq_id} not found.[/red]")
            return False

        topic = rec["original_topic"]
        payload = rec["payload"]
        if "raw" in payload and len(payload) == 1:
            try:
                payload = json.loads(payload["raw"])
            except Exception:
                pass

        success, err = self.worker.process_record(topic, payload)
        if success:
            self.storage.update_dlq_status(dlq_id, "REPLAYED", increment_retry=True)
            console.print(f"[bold green]Successfully replayed DLQ record {dlq_id}![/bold green]")
            return True
        else:
            self.storage.update_dlq_status(dlq_id, "PENDING", increment_retry=True)
            console.print(
                f"[bold red]Replay failed for {dlq_id}: {err}. Record kept PENDING.[/bold red]"
            )
            return False

    def quarantine_record(self, dlq_id: str) -> bool:
        """Permanently quarantine an unrecoverable DLQ entry."""
        rec = self.get_record(dlq_id)
        if not rec:
            console.print(f"[red]Record {dlq_id} not found.[/red]")
            return False
        self.storage.update_dlq_status(dlq_id, "QUARANTINED", increment_retry=False)
        console.print(f"[yellow]Record {dlq_id} set to QUARANTINED.[/yellow]")
        return True

    def replay_all_pending(self) -> Dict[str, int]:
        """Attempt replay on all PENDING records."""
        records = self.list_records(status="PENDING")
        succeeded = 0
        failed = 0
        for r in records:
            if self.replay_record(r["dlq_id"]):
                succeeded += 1
            else:
                failed += 1
        return {"total": len(records), "replayed": succeeded, "failed": failed}


def main() -> None:
    """CLI Entrypoint."""
    parser = argparse.ArgumentParser(description="Dead-Letter Queue Triage & Replayer")
    parser.add_argument("--db", default=":memory:", help="DuckDB lakehouse database path")

    subparsers = parser.add_subparsers(dest="command")

    # list
    list_p = subparsers.add_parser("list", help="List DLQ records")
    list_p.add_argument("--status", choices=["PENDING", "REPLAYED", "QUARANTINED"], default=None)

    # show
    show_p = subparsers.add_parser("show", help="Show payload of a DLQ record")
    show_p.add_argument("dlq_id", help="DLQ ID to inspect")

    # replay
    replay_p = subparsers.add_parser("replay", help="Replay a specific record")
    replay_p.add_argument("dlq_id", help="DLQ ID to replay")

    # replay-all
    subparsers.add_parser("replay-all", help="Replay all pending records")

    # quarantine
    quar_p = subparsers.add_parser("quarantine", help="Quarantine a record")
    quar_p.add_argument("dlq_id", help="DLQ ID to quarantine")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(0)

    mgr = DLQManager(db_path=args.db)

    if args.command == "list":
        mgr.print_table(status=args.status)
    elif args.command == "show":
        rec = mgr.get_record(args.dlq_id)
        if rec:
            console.print_json(json.dumps(rec, indent=2))
        else:
            console.print(f"[red]Record {args.dlq_id} not found.[/red]")
    elif args.command == "replay":
        mgr.replay_record(args.dlq_id)
    elif args.command == "replay-all":
        mgr.replay_all_pending()
    elif args.command == "quarantine":
        mgr.quarantine_record(args.dlq_id)


if __name__ == "__main__":
    main()
