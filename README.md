# Retail Orders — dbt Medallion Model (SCD Type 1 & Incremental Loading)

A dbt project on **Databricks** that models a single raw `orders` table into a star schema (dimensions, fact table, and a wide reporting table) using the **Medallion architecture**. The focus is on **SCD Type 1 dimensions**, **incremental merge loads**, and **deterministic deduplication**.

**Highlights**

- One raw table modeled into bronze (staging), silver (dimensions + fact) and gold (reporting) layers
- Incremental `merge` loads with a watermark filter and idempotent re-runs
- Deterministic "latest row wins" logic with `ROW_NUMBER()` and an explicit tie-breaker
- MD5 surrogate keys shared between dimensions and fact, including a composite key for region
- Known limitations of the watermark approach are documented, with mitigations

---

## Architecture

```
data_warehouse_xc.raw.orders   (source)
            │
            ▼
      stg_orders                 bronze   incremental merge   key: order_id
            │
            ├──► dim_customer    silver   incremental merge   key: customer_pk
            ├──► dim_product     silver   incremental merge   key: product_pk
            ├──► dim_region      silver   incremental merge   key: region_pk
            └──► fct_orders      silver   incremental merge   key: order_pk

      dim_date                   silver   table (generated calendar, no upstream)

  fct_orders ─┐
  dim_customer ─┤
  dim_product ─┼──► mart_orders_detailed   gold   table   (one row per order)
  dim_region ─┤
  dim_date ───┘
```

### Models

| Layer | Model | Materialization | Grain | Notes |
|---|---|---|---|---|
| Bronze | `stg_orders` | incremental (merge) | one row per `order_id` | Renames and casts columns, deduplicates on `order_id` |
| Silver | `dim_customer` | incremental (merge) | one row per `customer_id` | SCD1, `customer_pk = md5(customer_id)` |
| Silver | `dim_product` | incremental (merge) | one row per `product_id` | SCD1, `product_pk = md5(product_id)` |
| Silver | `dim_region` | incremental (merge) | one row per `region_id` + `country` | SCD1, composite surrogate key |
| Silver | `dim_date` | table | one row per calendar day | Generated date spine (2024), fiscal year and weekday attributes |
| Silver | `fct_orders` | incremental (merge), partitioned by `order_date` | one row per `order_id` | Carries foreign keys to all dimensions and the measures |
| Gold | `mart_orders_detailed` | table | one row per order | One Big Table: fact joined with all dimensions |

---

## Key Design Decisions

### Surrogate keys

Every dimension has an MD5 hash of its business key as primary key (`*_pk`), and the fact table computes the same hash as foreign key (`*_fk`). Region is identified by `region_id` **and** `country`, so its key hashes both:

```sql
md5(concat(cast(region_id as string), '_', coalesce(country, ''))) as region_pk
```

Because the key already includes `country`, joining `fct_orders.region_fk = dim_region.region_pk` is enough to match on region id and country together.

### Latest-row selection: `ROW_NUMBER()` instead of `DISTINCT`

SCD Type 1 keeps only the latest version of each entity, so each business key must appear **exactly once** in the batch that is merged. Otherwise `MERGE` fails with multiple source rows matching one target row, or a dimension ends up with duplicate keys and inflates every join.

`DISTINCT` cannot guarantee that. It removes only rows that are identical across **all** columns, so if a product's name changes, both versions are different rows and both survive. `ROW_NUMBER()` partitions by the business key and ranks rows with an explicit rule, so exactly one row per key is kept and the rule for "which version wins" is visible in the code:

```sql
row_number() over (
    partition by product_id
    order by order_date desc, order_id desc
) as rn
```

`order_id` is the tie-breaker: two orders on the same day for the same product are resolved deterministically instead of arbitrarily.


**Example**: product 201 is renamed on the same day it receives two orders.

| order_id | product_id | product_name | order_date |
|---|---|---|---|
| 5001 | 201 | Desk Lamp | 2024-03-05 |
| 5002 | 201 | Desk Lamp Pro | 2024-03-05 |
| 5000 | 201 | Desk Lamp | 2024-03-04 |

- `order by order_date desc` only: orders 5001 and 5002 tie on `order_date`, so either one can get `rn = 1`. `dim_product` may show `Desk Lamp` or `Desk Lamp Pro`, and the result can change between runs.
- `order by order_date desc, order_id desc`: the higher `order_id` wins the tie, so `rn = 1` is always order 5002 and `dim_product` always shows `Desk Lamp Pro`.

This assumes `order_id` increases over time. If it does not, the tie-breaker still makes the result stable, but not necessarily the most recent version.

### Incremental loading: when and why

| Materialization | Use when |
|---|---|
| `view` | Light logic, small data, results must always be fresh |
| `table` | Data is small or the logic needs the full history on every run (aggregations, calendars, marts); simplest and always correct |
| `incremental` | Source is large and growing, a full rebuild is slow or costly, and rows can be matched with a stable key and a reliable change marker |

How this applies here:

- `stg_orders` and `fct_orders` are the large, growing tables, so they load incrementally.
- The dimensions are small and derived; they are incremental in this project to implement and verify the SCD1 merge pattern. At larger volumes, rebuilding small dimensions as `table` is the safer choice, because a rebuild cannot be affected by watermark gaps.
- `dim_date` (static) and the gold mart (cheap to rebuild) are `table`.
- The raw data here is small, so a full rebuild would also work. Incremental is used deliberately to demonstrate a pattern that scales.

The incremental filter uses `>=`, not `>`:

```sql
{% if is_incremental() %}
    where order_date >= (select coalesce(max(order_date), date('1900-01-01')) from {{ this }})
{% endif %}
```

Rows from the latest already-loaded date are reprocessed on every run, so data that arrives later on the same day is not lost. This is safe because the `merge` on the unique key is idempotent.

---

## SCD Type 1 Verification

SCD1 means an attribute change **overwrites** the old value; no history is kept. To verify it on `dim_product`:

1. **Baseline**: `dbt run --full-refresh`, then check that product 201 exists exactly once:
   ```sql
   select count(*), max(product_name) from silver.dim_product where product_id = 201;
   ```
2. **Mutation**: insert a **new** order into `raw.orders` for `product_id = 201` with a changed `product_name`, and with an `order_date` on or after the current latest date (otherwise the watermark filter correctly skips it).
3. **Incremental run**: `dbt run`
4. **Assert**: the query from step 1 still returns `count(*) = 1`, and `product_name` now shows the new value.

For a variant that keeps history instead of overwriting, see the SCD Type 2 snapshot in the `dbt_core_tutorial` project.

---

## Data Tests

| Model | Column | Tests |
|---|---|---|
| `dim_customer`, `dim_product`, `dim_region`, `dim_date` | `*_pk` | `unique`, `not_null` |
| `fct_orders` | `order_pk` | `unique`, `not_null` |
| `fct_orders` | `customer_fk`, `product_fk`, `region_fk` | `relationships` to the matching dimension key |
| `fct_orders` | `order_date` | `relationships` to `dim_date.date_pk` |
| `mart_orders_detailed` | `order_pk` | `unique`, `not_null` |
| `mart_orders_detailed` | `region_name` | `not_null` |

- The `unique` tests on dimension keys verify the SCD1 guarantee: one row per business key.
- The `relationships` tests catch orphaned foreign keys, which is how gaps caused by the watermark filter would show up.
- The `order_date` test fails if orders fall outside the `dim_date` range (currently 2024).

Run with `dbt test` (or `dbt build` to run models and tests together).

## Limitations

- **No `updated_at` in the source.** `order_date` is used as a proxy watermark, which has two blind spots:
  - *Historical corrections are invisible*: a record with an already-passed `order_date` that is corrected at the source is not picked up. A `--full-refresh` is required.
  - *Late-arriving data with an older `order_date`* is skipped once the watermark has moved past that date (data arriving late on the *same* date is handled by `>=`).
- **Undefined winner for identical `order_id` and `order_date`.** In `stg_orders`, if the same `order_id` arrives twice with the same `order_date`, there is nothing to order on, so the surviving row is arbitrary. This needs a real modification timestamp to solve.
- **`dim_date` is a static 2024 calendar.** Orders outside that range join to `NULL` date attributes in the mart; extend the date spine to cover the data.


### Possible improvements

- Add a look-back window, e.g. `max(order_date) - interval 3 days`, to catch late-arriving rows (cheap because the merge is idempotent)
- Use a real `updated_at` or CDC feed from the source as the watermark
- Generate surrogate keys with a shared macro (e.g. `dbt_utils.generate_surrogate_key`) so the fact and dimension definitions cannot drift apart

---

## Running

```bash
# Initial load (or after changing model logic / correcting history)
dbt run --full-refresh

# Regular incremental run
dbt run

# Rebuild a single model and everything downstream of it
dbt run --select dim_product+
```

dbt resolves the run order from `ref()` dependencies, so models do not need to be listed in layer order.

---

## Reference

- Tutorial / inspiration: [YouTube guide](https://www.youtube.com/watch?v=HKcEyHF1U00&t=1s)
