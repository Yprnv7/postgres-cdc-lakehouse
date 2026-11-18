{{ config(materialized='view') }}

-- Deduplicate CDC raw order stream keeping the latest operation and LSN per order_id
WITH ranked_orders AS (
    SELECT
        order_id,
        customer_id,
        status,
        amount_cents,
        amount_cents / 100.0 AS amount_eur,
        currency,
        item_count,
        _cdc_op,
        _cdc_lsn,
        _cdc_ts_ms,
        _processed_at,
        ROW_NUMBER() OVER (
            PARTITION BY order_id 
            ORDER BY _cdc_lsn DESC, _cdc_ts_ms DESC
        ) AS rank_latest
    FROM raw_orders_cdc
)

SELECT
    order_id,
    customer_id,
    status,
    amount_cents,
    amount_eur,
    currency,
    item_count,
    _cdc_op,
    _cdc_lsn,
    _processed_at
FROM ranked_orders
WHERE rank_latest = 1
  AND _cdc_op != 'd'
