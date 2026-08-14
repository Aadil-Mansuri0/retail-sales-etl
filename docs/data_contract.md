# Retail ETL Data Contract

This document describes the expected raw input and generated warehouse outputs for the Retail Sales ETL pipeline.

## Raw Input

Raw files are read from `data/raw/` and must be CSV files with these columns:

| Column | Type after transform | Required | Notes |
|---|---|---:|---|
| `order_id` | string | Yes | Business key used for deduplication/upserts |
| `order_date` | date | Yes | Invalid dates are removed |
| `updated_at` | timestamp | Yes | Latest record wins for duplicate orders |
| `customer_id` | string | Yes | Used for customer dimension |
| `product_id` | string | Yes | Used for product dimension |
| `product_name` | string | Yes | Product display name |
| `category` | string | Yes | Product category |
| `city` | string | Yes | Missing values become `Unknown` |
| `quantity` | integer | Yes | Must be positive |
| `unit_price` | decimal | Yes | Must be positive |
| `discount` | decimal | Yes | Clipped to `0` through `0.9` |
| `payment_method` | string | Yes | Missing values become `Unknown` |
| `status` | string | Yes | Missing values become `Completed` |

## Transformation Rules

- Convert date and timestamp fields with pandas datetime parsing.
- Convert `quantity`, `unit_price` and `discount` to numeric values.
- Drop rows missing required business fields after parsing.
- Drop rows with non-positive quantity or unit price.
- Deduplicate on `order_id` after sorting by `updated_at`.
- Calculate `order_amount = quantity * unit_price * (1 - discount)`.
- Add `loaded_at` in UTC for observability.

## Warehouse Outputs

DuckDB warehouse path:

```text
warehouse/retail_warehouse.duckdb
```

Tables:

- `fact_sales`
- `dim_product`
- `dim_customer`
- `dim_date`
- `mart_daily_revenue`
- `mart_top_products`
- `mart_city_performance`

## Quality Report

The pipeline writes:

```text
docs/quality_report.json
```

The run fails if the quality report does not pass all checks.
