{{
  config(
    materialized = 'incremental',
    unique_key = 'customer_pk',
    incremental_strategy = 'merge',
    schema = 'silver'
  )
}}

with
    source_data as (
        select customer_id, customer_name, customer_email, order_date
        from {{ ref('stg_orders') }}
        where
            customer_id is not null
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
            row_number() over (partition by customer_id order by order_date desc) as rn
        from source_data
    )

select
    md5(cast(customer_id as string)) as customer_pk,
    customer_id,
    customer_name,
    customer_email,
    order_date as _source_order_date
from dedup
where rn = 1
