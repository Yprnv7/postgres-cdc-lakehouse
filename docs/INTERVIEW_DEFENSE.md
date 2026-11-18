# PostgreSQL CDC Lakehouse — Interview Defense Guide

## 1. The 30-Second Elevator Pitch
"I built **PostgreSQL CDC Lakehouse** to solve the dual challenges of data freshness and schema fragility in analytical ETL pipelines. Traditional batch syncs put heavy query load on production OLTP databases, offer zero visibility between runs, and silently break when client payloads mutate.

This pipeline captures row-level PostgreSQL transactions from the Write-Ahead Log (WAL) via **Debezium** and **Redpanda**, validates strict **Pydantic schema contracts** at the ingestion boundary, applies **salted SHA-256 PII hashing** for GDPR compliance, isolates malformed records to an operational **Dead-Letter Queue (DLQ)** without worker termination, and computes an audit-compliant **Slowly Changing Dimension Type 2 (SCD2)** history in **DuckDB**. 

On single-worker testing, it achieves **1,413.5 events/second** with a median commit-to-lakehouse latency of **0.683 ms** (p95 = 0.951 ms), verified by property-based testing over randomized mutation streams using Hypothesis."

---

## 2. Core Technical Architecture & Design Decisions

### A. CDC Ingestion: Why Debezium over Native Logical Replication / AWS DMS?
- **Debezium (`pgoutput` plugin):** Decodes PostgreSQL WAL segments without running SELECT queries against operational tables.
- **Why not a custom `psycopg` logical replication script?**
  - Logical replication slots in Postgres will retain WAL indefinitely if the consumer disconnects or falls behind, eventually exhausting disk space and crashing Postgres. Debezium coordinates checkpoint commits and heartbeat flushes automatically.
- **Why not AWS DMS?**
  - DMS is proprietary, introduces cloud lock-in, lacks schema contract validation at the event boundary, and has significant latency overhead during high DDL churn.

### B. Slowly Changing Dimension Type 2 (SCD2) Engine
- **Upfront SHA-256 Diff Fingerprinting:** Tracked business attributes (tier, address) are hashed into a 16-character diff fingerprint.
  - If a customer record updates in Postgres but the tracked attributes remain unchanged (an idempotent or no-op mutation), the worker skips row expiration in $O(1)$.
  - When tracked attributes mutate, the current row's `valid_to` is updated to the event timestamp, `is_current` is set to `FALSE`, and a new active version is inserted with `valid_to = NULL, is_current = TRUE`.
- **Property-Based Verification (Hypothesis):** `tests/test_scd2_properties.py` runs randomized sequences of create, update, and delete mutations asserting:
  1. Exactly one active record (`is_current=True`) per customer (zero if deleted).
  2. For every closed historical version, `valid_from < valid_to`.
  3. No temporal interval overlap across adjacent versions.
  4. Global uniqueness of surrogate keys (`customer_sk`).

### C. Zero-Crash Dead-Letter Queue (DLQ) & Quarantine CLI
- **Problem:** If a poisoned record (e.g., negative order amount, unparseable JSON) causes the consumer to throw an unhandled exception, Kafka consumer offsets do not advance. The consumer enters a crash loop, blocking subsequent valid messages.
- **Solution:** The worker catches contract violations, tags the record with metadata (original topic, raw payload, error reason, timestamp), and routes it to `dlq_records`.
- **Recovery:** An operator CLI (`consumer.dlq_replayer`) allows querying poisoned records, executing batch schema repairs (e.g. `abs(amount_cents)`), and replaying them back into the lakehouse at 314+ events/sec with zero stream interruption.

### D. Data Lineage (OpenLineage & Marquez)
- Every completed ingestion micro-batch constructs an OpenLineage 1.0.5 `RunEvent` detailing inputs (`oltp_db.public.customers`, `orders`) and outputs (`lakehouse.dim_customers_scd2`, `fct_orders`), which can be dispatched to Marquez for operational DAG observability.

---

## 3. Real Empirical Benchmarks

*Measured on Apple Silicon (Darwin arm64), Python 3.14.6, DuckDB 1.0.0. Raw output in `docs/benchmarks/cdc_benchmark.json`.*

| Benchmark Stage | Measured Value | Notes |
| :--- | :--- | :--- |
| **Ingestion Throughput** | **1,413.5 events/sec** | Single-threaded Python consumer writing to DuckDB |
| **Commit-to-Lakehouse Latency (p50)** | **0.683 ms** | Median time to validate, hash PII, and persist row |
| **Commit-to-Lakehouse Latency (p95)** | **0.951 ms** | 95th percentile under 1.0 ms |
| **Commit-to-Lakehouse Latency (p99)** | **1.868 ms** | Tail latency bounded under 2.0 ms |
| **Average SCD2 Mutation Overhead** | **0.887 ms** | Includes diff hashing + expiration + insertion |
| **DLQ Poison Isolation Latency (p95)**| **0.503 ms** | Time to isolate malformed row into DLQ |
| **DLQ Replay Recovery Throughput** | **314.1 events/sec** | Replay and re-ingest rate after schema patch |
| **Point-in-Time Query Latency (p50)** | **0.460 ms** | Evaluating temporal window filters over history |

---

## 4. Tough Interview Questions & Precise Answers

### Q1: "How do you handle late-arriving CDC records when updating SCD2 valid_from / valid_to?"
**Answer:** "In a pure append-only streaming model, if an event arrives with an earlier event timestamp than the currently active record (e.g., due to network partitioning or Kafka rebalancing), naive timestamp updates can cause `valid_from > valid_to` inversion. In this pipeline, the primary ordering authority is the **Log Sequence Number (LSN)** assigned by PostgreSQL's WAL. In production lakehouses, we handle late arrivals via micro-batch staging tables and a periodic dbt/DuckDB windowing model using `LEAD(valid_from) OVER (PARTITION BY customer_id ORDER BY _cdc_lsn ASC)` to compute non-overlapping temporal intervals deterministically."

### Q2: "What happens to the watermark during a dual-run backfill?"
**Answer:** "When taking an initial table snapshot while live streaming is active, a race condition occurs between the static snapshot and live mutations. We resolve this using an LSN watermark:
1. Before starting the snapshot, Debezium records the current WAL position as the `snapshot_watermark_lsn`.
2. The snapshot reads all existing rows.
3. Once the snapshot finishes, the streaming consumer ignores any CDC event whose `_cdc_lsn <= snapshot_watermark_lsn` because it is already captured in the snapshot.
4. Any event with `_cdc_lsn > snapshot_watermark_lsn` is applied sequentially, ensuring exactly-once transition semantics without data gaps or duplication."

### Q3: "DuckDB is an in-process database. How would you scale this pipeline for a high-concurrency production warehouse?"
**Answer:** "DuckDB is outstanding for single-node embedded analytics and micro-batch processing (over 1,400 eps on a single thread). To scale to tens of thousands of concurrent analytical users or gigabytes of streaming data per minute:
1. **Object Storage Buffer:** The sink worker buffers records into micro-partitioned Apache Parquet files on Amazon S3 or MinIO.
2. **Distributed Query Engine:** Query engines like ClickHouse, Trino, or Snowflake query the Parquet files directly via external tables or Iceberg/Delta lakehouse metadata catalogs.
3. **Multi-Partition Consumer Groups:** Distribute Redpanda partitions across multiple consumer worker containers to scale ingestion horizontally."
