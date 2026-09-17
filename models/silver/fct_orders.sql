{{
  config(
    materialized = 'incremental',
    schema = 'silver',
    unique_key = 'order_pk',
    incremental_strategy = 'merge',
    partition_by = ['order_date']
  )
}}

select
    md5(cast(order_id as string)) as order_pk,
    order_id,
    order_date,

    customer_id,
    product_id,
    region_id,
    country,

    md5(cast(customer_id as string)) as customer_fk,
    md5(cast(product_id as string)) as product_fk,
    md5(concat(cast(region_id as string), '_', coalesce(country, ''))) as region_fk,
    quantity,
    unit_price,
    total_amount,
    ingested_at
from {{ ref('stg_orders') }}
where
    order_id is not null

    {% if is_incremental() %}
        and ingested_at > (select max(ingested_at) from {{ this }})
    {% endif %}
