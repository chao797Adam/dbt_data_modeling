{{
  config(
    materialized = 'incremental',
    unique_key = 'region_pk',
    incremental_strategy = 'merge',
    schema = 'silver'
  )
}}

with
    source_data as (
        select region_id, region_name, country, order_date, order_id
        from {{ ref('stg_orders') }}
        where
            region_id is not null
            {% if is_incremental() %}
                and order_date >= (
                    select coalesce(max(_source_order_date), date('1900-01-01'))
                    from {{ this }}
                )
            {% endif %}
    ),

    dedup as (
        select
            *,
            row_number() over (
                partition by region_id, country order by order_date desc, order_id desc
            ) as rn
        from source_data
    )

select
    md5(concat(cast(region_id as string), '_', coalesce(country, ''))) as region_pk,
    region_id,
    region_name,
    country,
    order_date as _source_order_date
from dedup
where rn = 1
