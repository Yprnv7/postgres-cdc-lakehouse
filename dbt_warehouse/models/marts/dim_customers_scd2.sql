{{ config(materialized='table') }}

-- Slowly Changing Dimension Type 2 (SCD2) for Customers
-- Preserves complete temporal audit trail of tier upgrades and address mutations

WITH ranked_customer_states AS (
    SELECT
        customer_id,
        name,
        email_hash,
        phone_hash,
        tier,
        address_street,
        address_city,
        address_country,
        _cdc_op,
        _cdc_lsn,
        to_timestamp(_cdc_ts_ms / 1000.0) AS change_timestamp,
        md5(concat_ws('|', name, tier, address_street, address_city, address_country)) AS record_fingerprint
    FROM raw_customers_cdc
),

distinct_changes AS (
    SELECT
        *,
        LAG(record_fingerprint) OVER (
            PARTITION BY customer_id 
            ORDER BY change_timestamp ASC, _cdc_lsn ASC
        ) AS prev_fingerprint
    FROM ranked_customer_states
),

filtered_transitions AS (
    SELECT *
    FROM distinct_changes
    WHERE prev_fingerprint IS NULL 
       OR prev_fingerprint != record_fingerprint 
       OR _cdc_op = 'd'
),

scd_windows AS (
    SELECT
        customer_id,
        name,
        email_hash,
        phone_hash,
        tier,
        address_street,
        address_city,
        address_country,
        _cdc_op,
        record_fingerprint AS diff_hash,
        change_timestamp AS valid_from,
        LEAD(change_timestamp) OVER (
            PARTITION BY customer_id 
            ORDER BY change_timestamp ASC
        ) AS valid_to
    FROM filtered_transitions
)

SELECT
    concat_ws('_', customer_id, strftime(valid_from, '%Y%m%d%H%M%S%f')) AS customer_sk,
    customer_id,
    name,
    email_hash,
    phone_hash,
    tier,
    address_street,
    address_city,
    address_country,
    diff_hash,
    valid_from,
    valid_to,
    CASE 
        WHEN valid_to IS NULL AND _cdc_op != 'd' THEN TRUE 
        ELSE FALSE 
    END AS is_current
FROM scd_windows
WHERE _cdc_op != 'd' OR valid_to IS NOT NULL
