{{ config(materialized='table') }}

-- Fact Table: Orders joined with current customer dimensions
WITH latest_orders AS (
    SELECT * FROM {{ ref('stg_orders_cdc') }}
),

current_customers AS (
    SELECT
        customer_sk,
        customer_id,
        tier AS customer_tier,
        address_country AS customer_country
    FROM {{ ref('dim_customers_scd2') }}
    WHERE is_current = TRUE
)

SELECT
    o.order_id,
    o.customer_id,
    c.customer_sk,
    c.customer_tier,
    c.customer_country,
    o.status,
    o.amount_cents,
    o.amount_eur,
    o.currency,
    o.item_count,
    o._cdc_lsn AS latest_lsn,
    o._processed_at AS ingested_at
FROM latest_orders o
LEFT JOIN current_customers c ON o.customer_id = c.customer_id
