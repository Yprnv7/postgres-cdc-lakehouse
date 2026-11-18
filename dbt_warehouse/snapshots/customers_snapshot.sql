{% snapshot customers_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='check',
      check_cols=['name', 'tier', 'address_street', 'address_city', 'address_country'],
      invalidate_hard_deletes=True
    )
}}

SELECT
    customer_id,
    name,
    email_hash,
    phone_hash,
    tier,
    address_street,
    address_city,
    address_country,
    _processed_at AS updated_at
FROM {{ ref('stg_customers_cdc') }}

{% endsnapshot %}
