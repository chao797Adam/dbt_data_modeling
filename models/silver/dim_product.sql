{{
    config(
        materialized = 'incremental',
        unique_key = 'product_id',
        incremental_strategy = 'merge',
        schema = 'silver'
    )
}}

with
    dedup as (
        select
            product_id,
            product_name,
            product_category,
            row_number() over (
                partition by product_id order by order_date desc, order_id desc
            ) as rn
        from {{ ref('stg_orders') }}
        where product_id is not null
    )

select
    md5(cast(product_id as string)) as product_pk,
    product_id,
    product_name,
    product_category
from dedup
where rn = 1
