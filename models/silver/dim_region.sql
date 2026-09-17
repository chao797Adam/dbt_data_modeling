{{ config(materialized = 'table', schema = 'silver') }}

with
    region_latest as (
        select
            region_id,
            region_name,
            country,
            ingested_at,
            order_date,
            row_number() over (
                partition by region_id, country order by order_date desc
            ) as rn
        from {{ ref('stg_orders') }}
    )
select
    md5(concat(cast(region_id as string), '_', coalesce(country, ''))) as region_pk,
    region_id,
    region_name,
    country
from region_latest
where rn = 1
