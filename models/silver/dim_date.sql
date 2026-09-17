{{ config(materialized = 'table', schema = 'silver') }}

with
    date_spine as (
        select
            explode(
                sequence(to_date('2024-01-01'), to_date('2024-12-31'), interval 1 day)
            ) as date_day
    )

select
    date_day as date_pk,
    year(date_day) as year,
    month(date_day) as month,
    quarter(date_day) as quarter,
    date_format(date_day, 'EEEE') as day_of_week,
    -- Australian fiscal year starts July 1st
    case
        when month(date_day) >= 7 then year(date_day) + 1 else year(date_day)
    end as australian_fiscal_year,
    -- Spark dayofweek: 1 = Sun, 7 = Sat
    case
        when dayofweek(date_day) in (1, 7) then 'Weekend' else 'Weekday'
    end as day_type
from date_spine
