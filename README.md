# Databricks & dbt Medallion Architecture (SCD Type 1)

## Architecture Flow
Raw (`raw.orders`) 
  └──> Bronze (`stg_orders` - SCD1 Incremental Merge)
        ├──> Silver: `dim_prod` (MD5 surrogate keys, `rn = 1`)
        ├──> Silver: `dim_cus` (MD5 surrogate keys, `rn = 1`)
        ├──> Silver: `dim_region` (MD5 surrogate keys, `rn = 1`)
        └──> Silver: `dim_date` (MD5 surrogate keys, `rn = 1`)
              └──> Gold: `OBT` (One Big Table, region joined on `id + country`)

## Layer Breakdown
- **Bronze (`stg_orders`)**: Incremental merge, `unique_key='order_id'`, `ROW_NUMBER()` deduplicated.
- **Silver (`dim_prod`, `dim_cus`, `dim_region`, `dim_date`)**: Derived from `stg_orders`, filtered `rn = 1` (`order_date desc, order_id desc`), MD5 surrogate keys.
- **Gold (`OBT`)**: Joined with `dim_region` on `fact.region_id = r.region_id AND fact.country = r.country`.

## SCD Type 1 Verification & Limitations

### Verification Steps
1. **Baseline**: `dbt run --select stg_orders dim_prod --full-refresh` -> check `count(*) = 1`.
2. **Mutation**: Insert updated attribute data into `raw.orders` (`product_id = 201`, new name).
3. **Incremental**: `dbt run --select stg_orders dim_prod`.
4. **Assert**: `count(*) = 1`, `product_name` updated in place.

### Limitations of `DISTINCT` for SCD1 / Upserts
- **Loss of Deterministic Chronology**: No ordering guarantee for latest state across conflicting updates.
- **Missing Upsert Semantics**: Cannot define PK matching or conditional update/insert branches for `MERGE`.
- **Masking Grain/Conflict Issues**: Silently collapses multi-attribute edge cases or creates blind spots compared to window deduplication (`ROW_NUMBER() = 1`).

## Quick Reference
```bash
dbt run --select stg_orders dim_prod dim_cus dim_region dim_date obt --full-refresh
dbt run --select stg_orders dim_prod dim_cus dim_region dim_date obt

## References
- Tutorial / Inspiration: [YouTube Guide](https://www.youtube.com/watch?v=HKcEyHF1U00&t=1s)