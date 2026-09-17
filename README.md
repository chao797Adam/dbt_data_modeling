# Databricks & dbt Medallion Architecture (SCD Type 1)


## Architecture Flow
* **Raw Layer**: `raw.orders`
  * └── **Bronze Layer**: `stg_orders` (SCD1 Incremental Merge, `rn = 1`)
      * ├── **Silver Layer (`dim_prod`)**: MD5 surrogate keys, `rn = 1`
      * ├── **Silver Layer (`dim_cus`)**: MD5 surrogate keys, `rn = 1`
      * ├── **Silver Layer (`dim_region`)**: MD5 surrogate keys, `rn = 1`
      * └── **Silver Layer (`dim_date`)**: MD5 surrogate keys, `rn = 1`
          * └── **Gold Layer (`OBT`)**: One Big Table (region joined on `id + country`)

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

### Why `ROW_NUMBER()` Instead of `DISTINCT` (vs. Tutorial Reference)
The reference tutorial uses `DISTINCT` for latest-state deduplication. This project intentionally
uses `ROW_NUMBER() OVER (PARTITION BY <key> ORDER BY order_date DESC)` instead, because `DISTINCT`
has real limitations for SCD1/upsert use cases:
- **Loss of Deterministic Chronology**: `DISTINCT` has no ordering guarantee — when a key has
  conflicting values across rows, there's no defined rule for which version "wins".
- **Missing Upsert Semantics**: `DISTINCT` can't express PK matching or conditional
  update/insert branches the way a `MERGE` statement needs.
- **Masking Grain/Conflict Issues**: `DISTINCT` silently collapses rows without surfacing
  which attribute actually changed, hiding multi-attribute edge cases that `ROW_NUMBER() = 1`
  makes explicit and debuggable.

### Limitation: No Native `updated_at`, Relying on `order_date` as Watermark
The source system provides no modification timestamp. Incremental loads use `order_date` strictly
increasing as a proxy watermark, which has two blind spots:
- **Historical corrections are invisible**: if a record with an already-passed `order_date` is
  corrected at the source, the filter (`order_date > max(order_date)`) will silently skip it.
  A `--full-refresh` is required to pick it up.
- **Late-arriving data with an older `order_date` is also skipped**: once the watermark advances
  past a given date, records that arrive late for that date will never be picked up incrementally.
- **Mitigation (not implemented here)**: a genuine `updated_at`/CDC feed would remove both gaps.

## Quick Reference
```bash
dbt run --select stg_orders dim_prod dim_cus dim_region dim_date obt --full-refresh
dbt run --select stg_orders dim_prod dim_cus dim_region dim_date obt
```

## References
- Tutorial / Inspiration: [YouTube Guide](https://www.youtube.com/watch?v=HKcEyHF1U00&t=1s)
