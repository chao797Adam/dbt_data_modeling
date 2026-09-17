{{
  config(
    materialized = 'incremental',
    unique_key = 'product_pk',
    incremental_strategy = 'merge',
    schema = 'silver'
  )
}}

with
    source_data as (
        select product_id, product_name, product_category, order_date
        from {{ ref('stg_orders') }}
        where
            product_id is not null
            {% if is_incremental() %}
                and order_date > (
                    select coalesce(max(_source_order_date), date('1900-01-01'))
                    from {{ this }}
                )
            {% endif %}
    ),

    dedup as (
        select
            *,
            row_number() over (partition by product_id order by order_date desc) as rn
        from source_data
    )

select
    md5(cast(product_id as string)) as product_pk,
    product_id,
    product_name,
    product_category,
    order_date as _source_order_date
from dedup
where rn = 1
