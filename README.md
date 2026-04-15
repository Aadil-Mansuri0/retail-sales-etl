# Retail Sales ETL Pipeline (Basic to Intermediate)

This project demonstrates a complete ETL pipeline using Python, SQL, and DuckDB.
It is designed for internship-level data engineering portfolios and can be explained easily in interviews.

## What ETL Means

- Extract: Read raw retail data from CSV files.
- Transform: Clean data, remove duplicates, standardize fields, and calculate metrics.
- Load: Store curated data in a warehouse and build analytics marts.

## Project Features

- Synthetic raw data generator (`src/generate_data.py`)
- Data cleaning and transformation pipeline (`src/etl_pipeline.py`)
- Incremental upsert logic by `order_id`
- Quality checks (nulls, duplicates, invalid values, future dates)
- Star-schema style dimensions and analytics marts
- Output artifacts for reports and dashboards

## Architecture

`Raw CSV files -> Python Transformations -> DuckDB Warehouse -> Dimension + Mart Tables -> CSV exports for dashboard`

## Tech Stack

- Python
- Pandas
- DuckDB
- NumPy

## Folder Structure

```text
retail-sales-etl/
  data/
    raw/
    processed/
  docs/
    quality_report.json
  src/
    generate_data.py
    etl_pipeline.py
  warehouse/
    retail_warehouse.duckdb
  requirements.txt
  README.md
```

## Quick Start

```bash
cd retail-sales-etl
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python src/generate_data.py --rows 50000 --days 120
python src/etl_pipeline.py
```

## Large Dataset Run (Optional)

Use larger rows to simulate larger workloads on a laptop.

```bash
python src/generate_data.py --rows 500000 --days 365
python src/etl_pipeline.py
```

## Outputs

After execution, you will get:

- Cleaned dataset: `data/processed/retail_orders_clean.csv`
- Warehouse DB: `warehouse/retail_warehouse.duckdb`
- Marts:
  - `data/processed/mart_daily_revenue.csv`
  - `data/processed/mart_top_products.csv`
  - `data/processed/mart_city_performance.csv`
- Quality report: `docs/quality_report.json`

## SQL Assets Created in Warehouse

- `fact_sales`
- `dim_product`
- `dim_customer`
- `dim_date`
- `mart_daily_revenue`
- `mart_top_products`
- `mart_city_performance`

## Resume Bullets (Copy-Ready)

- Built an end-to-end retail ETL pipeline using Python and DuckDB to ingest, clean, and model sales data from raw CSV files.
- Implemented incremental upsert logic and automated data quality checks (duplicates, nulls, invalid values), improving reliability of analytics outputs.
- Designed fact and dimension tables plus business marts for daily revenue, top products, and city-level performance reporting.

## Interview Talking Points

- Why upsert by `order_id` was used for idempotent reruns.
- How dirty raw data was handled safely.
- Why warehouse marts were separated from raw and clean layers.
- What quality checks were added and why they matter in production.
