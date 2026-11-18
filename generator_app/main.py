"""FastAPI + HTMX Live Order & Customer Mutation Simulator and DLQ Control Center."""

from __future__ import annotations

import os
import random
import uuid
from typing import Any, Dict, Optional

from fastapi import FastAPI, Form
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from consumer.dlq_replayer import DLQManager
from consumer.sink_worker import CDCSinkWorker
from consumer.storage import LakehouseStorage

app = FastAPI(
    title="Postgres CDC Lakehouse Simulator",
    description="Live mutation generator, SCD Type 2 demonstrator, and DLQ control center",
    version="1.0.0",
)

# Persistent or memory storage for the simulator
DB_PATH = os.getenv("DUCKDB_PATH", ":memory:")
storage = LakehouseStorage(db_path=DB_PATH)
worker = CDCSinkWorker(storage=storage)
dlq_mgr = DLQManager(storage=storage)

CURRENT_LSN = 1000

SAMPLE_CITIES = [
    ("Dublin", "Ireland", "Grand Canal Dock"),
    ("Bengaluru", "India", "Koramangala 4th Block"),
    ("Hyderabad", "India", "HITEC City Phase 2"),
    ("London", "United Kingdom", "Canary Wharf"),
    ("Frankfurt", "Germany", "Mainzer Landstrasse"),
]

ORDER_STATUS_FLOW = ["CREATED", "PAID", "SHIPPED", "DELIVERED"]


class CustomerMutationRequest(BaseModel):
    name: str
    email: str
    phone: Optional[str] = None
    tier: str = "STANDARD"
    address_street: Optional[str] = None
    address_city: Optional[str] = None
    address_country: Optional[str] = None


class OrderMutationRequest(BaseModel):
    customer_id: str
    amount_cents: int
    currency: str = "EUR"
    status: str = "CREATED"


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    """Live web dashboard with real-time stats and interactive controls."""
    metrics = storage.get_metrics()
    dlq_records = storage.get_dlq_records()[:10]

    dlq_rows_html = ""
    for r in dlq_records:
        dlq_rows_html += f"""
        <tr>
            <td style="font-family: monospace; color: #4f46e5;">{r['dlq_id']}</td>
            <td><span class="badge badge-topic">{r['original_topic']}</span></td>
            <td style="color: #dc2626; font-size: 0.85rem;">{r['error_reason'][:60]}...</td>
            <td><span class="badge badge-{r['status'].lower()}">{r['status']}</span></td>
            <td>
                <form action="/api/dlq/replay" method="post" style="display:inline;">
                    <input type="hidden" name="dlq_id" value="{r['dlq_id']}">
                    <button type="submit" class="btn btn-sm btn-replay">Replay</button>
                </form>
            </td>
        </tr>
        """

    if not dlq_rows_html:
        dlq_rows_html = "<tr><td colspan='5' style='text-align: center; color: #16a34a;'>No corrupted records in DLQ. Clean stream!</td></tr>"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PostgreSQL CDC Lakehouse | Live Simulator</title>
    <style>
        :root {{
            --bg: #0f172a;
            --card: #1e293b;
            --border: #334155;
            --text: #f8fafc;
            --muted: #94a3b8;
            --primary: #38bdf8;
            --accent: #6366f1;
            --danger: #ef4444;
            --success: #22c55e;
        }}
        body {{
            background: var(--bg);
            color: var(--text);
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            margin: 0;
            padding: 24px;
        }}
        .header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 1px solid var(--border);
            padding-bottom: 16px;
            margin-bottom: 24px;
        }}
        .title {{
            font-size: 1.5rem;
            font-weight: 700;
            color: var(--primary);
        }}
        .badge-live {{
            background: rgba(34, 197, 94, 0.2);
            color: var(--success);
            border: 1px solid var(--success);
            padding: 4px 10px;
            border-radius: 9999px;
            font-size: 0.8rem;
            font-weight: 600;
        }}
        .grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 16px;
            margin-bottom: 24px;
        }}
        .card {{
            background: var(--card);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 20px;
        }}
        .card-val {{
            font-size: 2rem;
            font-weight: 800;
            color: var(--primary);
            margin-top: 8px;
        }}
        .card-label {{
            font-size: 0.85rem;
            color: var(--muted);
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }}
        .controls-grid {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 24px;
            margin-bottom: 24px;
        }}
        button {{
            cursor: pointer;
            border: none;
            border-radius: 6px;
            padding: 10px 18px;
            font-weight: 600;
            font-size: 0.9rem;
            transition: opacity 0.2s;
        }}
        button:hover {{ opacity: 0.9; }}
        .btn-primary {{ background: var(--accent); color: white; }}
        .btn-poison {{ background: var(--danger); color: white; }}
        .btn-replay {{ background: var(--success); color: white; }}
        .btn-sm {{ padding: 4px 8px; font-size: 0.75rem; }}
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9rem;
        }}
        th, td {{
            text-align: left;
            padding: 12px;
            border-bottom: 1px solid var(--border);
        }}
        th {{ color: var(--muted); font-size: 0.8rem; text-transform: uppercase; }}
        .badge {{ padding: 2px 8px; border-radius: 4px; font-size: 0.75rem; font-weight: 600; }}
        .badge-pending {{ background: rgba(234, 179, 8, 0.2); color: #eab308; }}
        .badge-replayed {{ background: rgba(34, 197, 94, 0.2); color: var(--success); }}
        .badge-quarantined {{ background: rgba(239, 68, 68, 0.2); color: var(--danger); }}
        .badge-topic {{ background: rgba(99, 102, 241, 0.2); color: var(--accent); }}
    </style>
</head>
<body>
    <div class="header">
        <div>
            <div class="title">Postgres CDC Lakehouse Engine</div>
            <div style="color: var(--muted); font-size: 0.9rem; margin-top: 4px;">Real-Time Replication &bull; SCD Type 2 Audit Trail &bull; Dead-Letter Queue &bull; Salted PII Hashing</div>
        </div>
        <div>
            <span class="badge-live">&#9679; ENGINE ACTIVE</span>
        </div>
    </div>

    <div class="grid">
        <div class="card">
            <div class="card-label">Raw Orders CDC</div>
            <div class="card-val">{metrics['raw_orders_cdc']}</div>
        </div>
        <div class="card">
            <div class="card-label">Deduplicated Fact Orders</div>
            <div class="card-val">{metrics['fct_orders']}</div>
        </div>
        <div class="card">
            <div class="card-label">SCD2 Customer Versions</div>
            <div class="card-val">{metrics['dim_customers_scd2_total']}</div>
        </div>
        <div class="card">
            <div class="card-label">Current Active Customers</div>
            <div class="card-val">{metrics['dim_customers_active']}</div>
        </div>
        <div class="card" style="border-color: {('#ef4444' if metrics['dlq_records'] > 0 else 'var(--border)')};">
            <div class="card-label">Dead-Letter Queue (DLQ)</div>
            <div class="card-val" style="color: {('#ef4444' if metrics['dlq_records'] > 0 else 'var(--primary)')};">{metrics['dlq_records']}</div>
        </div>
    </div>

    <div class="controls-grid">
        <div class="card">
            <h3 style="margin-top:0; color: var(--text);">Simulate Production Traffic</h3>
            <p style="color: var(--muted); font-size: 0.85rem;">Emit realistic customer lifecycle mutations and multi-step order transitions into the CDC stream.</p>
            <div style="display: flex; gap: 12px; margin-top: 16px;">
                <form action="/api/simulate/batch" method="post" style="display:inline;">
                    <input type="hidden" name="count" value="10">
                    <button type="submit" class="btn-primary">+10 Live Mutations</button>
                </form>
                <form action="/api/simulate/batch" method="post" style="display:inline;">
                    <input type="hidden" name="count" value="50">
                    <button type="submit" class="btn-primary">+50 Load Batch</button>
                </form>
                <form action="/api/simulate/customer-update" method="post" style="display:inline;">
                    <button type="submit" class="btn-primary">Mutate Customer Address (SCD2)</button>
                </form>
            </div>
        </div>

        <div class="card">
            <h3 style="margin-top:0; color: var(--danger);">Chaos & Resilience Test (DLQ)</h3>
            <p style="color: var(--muted); font-size: 0.85rem;">Inject deliberate schema corruptions and contract violations to verify pipeline isolation without crashes.</p>
            <div style="display: flex; gap: 12px; margin-top: 16px;">
                <form action="/api/poison/order" method="post" style="display:inline;">
                    <button type="submit" class="btn-poison">Inject Negative Amount</button>
                </form>
                <form action="/api/poison/email" method="post" style="display:inline;">
                    <button type="submit" class="btn-poison">Inject Corrupted Email</button>
                </form>
                <form action="/api/poison/malformed-json" method="post" style="display:inline;">
                    <button type="submit" class="btn-poison">Inject Raw Broken JSON</button>
                </form>
            </div>
        </div>
    </div>

    <div class="card">
        <h3 style="margin-top:0; color: var(--text);">Dead-Letter Queue (DLQ) Audit Log</h3>
        <table>
            <thead>
                <tr>
                    <th>DLQ ID</th>
                    <th>Topic</th>
                    <th>Reason</th>
                    <th>Status</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody>
                {dlq_rows_html}
            </tbody>
        </table>
    </div>
</body>
</html>
"""
    return html


@app.post("/api/simulate/batch")
def simulate_batch(count: int = Form(10)) -> HTMLResponse:
    """Generate synthetic batch of customers and order state machines."""
    global CURRENT_LSN

    for _ in range(count):
        CURRENT_LSN += 1
        cid = f"cust_{uuid.uuid4().hex[:8]}"
        city, country, street = random.choice(SAMPLE_CITIES)

        # 1. Customer creation
        cust_payload = {
            "customer_id": cid,
            "name": f"Enterprise User {cid}",
            "email": f"user.{cid}@enterprise.com",
            "phone": f"+353-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
            "tier": random.choice(["STANDARD", "PREMIUM", "ENTERPRISE"]),
            "address_street": street,
            "address_city": city,
            "address_country": country,
            "__op": "c",
            "__lsn": CURRENT_LSN,
        }
        worker.process_record("customers.events", cust_payload, lsn=CURRENT_LSN)

        # 2. Associated order progression
        CURRENT_LSN += 1
        oid = f"ord_{uuid.uuid4().hex[:8]}"
        order_payload = {
            "order_id": oid,
            "customer_id": cid,
            "status": "CREATED",
            "amount_cents": random.randint(1500, 250000),
            "currency": random.choice(["EUR", "USD", "GBP"]),
            "item_count": random.randint(1, 5),
            "__op": "c",
            "__lsn": CURRENT_LSN,
        }
        worker.process_record("orders.events", order_payload, lsn=CURRENT_LSN)

        # 3. Probabilistic transition to PAID
        if random.random() > 0.3:
            CURRENT_LSN += 1
            order_payload["status"] = "PAID"
            order_payload["__op"] = "u"
            order_payload["__lsn"] = CURRENT_LSN
            worker.process_record("orders.events", order_payload, lsn=CURRENT_LSN)

    return HTMLResponse(
        content="<script>window.location.href='/';</script>", status_code=200
    )


@app.post("/api/simulate/customer-update")
def simulate_customer_update() -> HTMLResponse:
    """Trigger address / tier mutation on an existing customer to test SCD Type 2."""
    global CURRENT_LSN
    CURRENT_LSN += 1

    # Fetch an active customer
    active = storage.con.execute(
        "SELECT customer_id, name, email_hash FROM dim_customers_scd2 WHERE is_current = TRUE LIMIT 1"
    ).fetchone()

    if not active:
        cid = "cust_scd2_demo"
        name = "SCD2 Demo Client"
        email = "demo@enterprise.eu"
    else:
        cid = active[0]
        name = active[1]
        email = f"{cid}@company.com"

    city, country, street = random.choice(SAMPLE_CITIES)
    payload = {
        "customer_id": cid,
        "name": name,
        "email": email,
        "tier": "ENTERPRISE_VIP",
        "address_street": f"{random.randint(10, 999)} Tech Boulevard",
        "address_city": city,
        "address_country": country,
        "__op": "u",
        "__lsn": CURRENT_LSN,
    }
    worker.process_record("customers.events", payload, lsn=CURRENT_LSN)

    return HTMLResponse(
        content="<script>window.location.href='/';</script>", status_code=200
    )


@app.post("/api/poison/order")
def poison_order() -> HTMLResponse:
    """Inject order with illegal negative amount to test DLQ contract guard."""
    global CURRENT_LSN
    CURRENT_LSN += 1

    payload = {
        "order_id": f"ord_poison_{uuid.uuid4().hex[:6]}",
        "customer_id": "cust_victim_01",
        "status": "CREATED",
        "amount_cents": -5000,  # Negative amount violation
        "currency": "EUR",
        "item_count": 1,
        "__op": "c",
        "__lsn": CURRENT_LSN,
    }
    worker.process_record("orders.events", payload, lsn=CURRENT_LSN)
    return HTMLResponse(
        content="<script>window.location.href='/';</script>", status_code=200
    )


@app.post("/api/poison/email")
def poison_email() -> HTMLResponse:
    """Inject customer with missing @ in email to trigger validation failure."""
    global CURRENT_LSN
    CURRENT_LSN += 1

    payload = {
        "customer_id": f"cust_bad_email_{uuid.uuid4().hex[:6]}",
        "name": "Corrupted Client",
        "email": "not-a-valid-email-string",
        "tier": "STANDARD",
        "__op": "c",
        "__lsn": CURRENT_LSN,
    }
    worker.process_record("customers.events", payload, lsn=CURRENT_LSN)
    return HTMLResponse(
        content="<script>window.location.href='/';</script>", status_code=200
    )


@app.post("/api/poison/malformed-json")
def poison_malformed_json() -> HTMLResponse:
    """Inject raw broken string payload."""
    raw_garbage = "{'order_id': 'incomplete, status: BROKEN"
    worker.process_record("orders.events", raw_garbage, lsn=9999)
    return HTMLResponse(
        content="<script>window.location.href='/';</script>", status_code=200
    )


@app.post("/api/dlq/replay")
def replay_dlq(dlq_id: str = Form(...)) -> HTMLResponse:
    """Replay DLQ record through triage manager."""
    dlq_mgr.replay_record(dlq_id)
    return HTMLResponse(
        content="<script>window.location.href='/';</script>", status_code=200
    )


@app.get("/api/stats")
def api_stats() -> Dict[str, Any]:
    """JSON stats endpoint."""
    return storage.get_metrics()
