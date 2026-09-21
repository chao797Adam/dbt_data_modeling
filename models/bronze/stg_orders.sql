{{
  config(
    materialized = 'incremental',
    unique_key = 'order_id',
    incremental_strategy = 'merge',
    schema = 'bronze'
  )
}}

with
    raw_data as (
        select
            orderid as order_id,
            orderdate::date as order_date,
            customerid as customer_id,
            customername as customer_name,
            customeremail as customer_email,
            productid as product_id,
            productname as product_name,
            productcategory as product_category,
            regionid as region_id,
            regionname as region_name,
            country as country,
            quantity as quantity,
            unitprice::decimal(10, 2) as unit_price,
            totalamount::decimal(10, 2) as total_amount,
            current_timestamp() as ingested_at
        from {{ source('external_source', 'orders') }}
        {% if is_incremental() %}
            where
                orderdate::date >= (
                    select coalesce(max(order_date), date('1900-01-01')) from {{ this }}
                )
        {% endif %}
    ),

    dedup as (
        select
            *, row_number() over (partition by order_id order by order_date desc) as rn
        from raw_data
    )

select
    order_id,
    order_date,
    customer_id,
    customer_name,
    customer_email,
    product_id,
    product_name,
    product_category,
    region_id,
    region_name,
    country,
    quantity,
    unit_price,
    total_amount,
    ingested_at
from dedup
where rn = 1
