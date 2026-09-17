{{ config(materialized = 'table', schema = 'silver') }}

with
    ranking as (
        select
            customer_id,
            customer_name,
            customer_email,
            order_date,
            ingested_at,
            row_number() over (
                partition by customer_id order by order_date desc, order_id desc
            ) as rn
        from {{ ref('stg_orders') }}
    )

select
    md5(cast(customer_id as string)) as customer_pk,
    customer_id,
    customer_name,
    customer_email,
    order_date as last_order_date,
    ingested_at
from ranking
where rn = 1
