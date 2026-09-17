{{
  config(
    materialized = 'table', schema = 'gold'
  )
}}

with
    fact as (select * from {{ ref('fct_orders') }}),

    dim_cust as (select * from {{ ref('dim_customer') }}),

    dim_prod as (select * from {{ ref('dim_product') }}),

    dim_reg as (select * from {{ ref('dim_region') }}),

    dim_dat as (select * from {{ ref('dim_date') }})

select
    f.order_pk,
    f.order_id,
    f.order_date,

    c.customer_name,
    c.customer_email,
    p.product_name,
    p.product_category,
    r.region_name,
    r.country,

    d.day_of_week,
    d.day_type,
    d.australian_fiscal_year,

    f.quantity,
    f.unit_price,
    f.total_amount

from fact f
left join dim_cust c on f.customer_fk = c.customer_pk
left join dim_prod p on f.product_fk = p.product_pk
left join dim_reg r on f.region_fk = r.region_pk and f.country = r.country
left join dim_dat d on f.order_date = d.date_pk
