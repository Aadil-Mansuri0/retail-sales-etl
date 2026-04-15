# Retail Sales ETL Pipeline

End-to-end ETL pipeline using Python, Pandas, and DuckDB to process retail sales data from raw CSV files into analytics-ready marts.

## What This Project Does

- Extracts raw retail order files from `data/raw/`
- Cleans and standardizes data types and missing values
- Deduplicates records by `order_id` using latest `updated_at`
- Loads curated data into DuckDB warehouse tables
- Publishes marts for daily revenue, top products, and city performance
- Writes a pipeline quality report for validation checks

## Tech Stack

- Python
- Pandas
- NumPy
- DuckDB
- SQL

## Pipeline Flow

Raw CSV -> Transformations -> Fact Table -> Dimension Tables -> Analytics Marts -> Quality Report

## Repository Structure

```text
retail-sales-etl/
  src/
    generate_data.py
    etl_pipeline.py
  data/
    raw/
    processed/
  warehouse/
  docs/
  requirements.txt
  README.md
```

## How To Run

```bash
cd retail-sales-etl
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python src/generate_data.py --rows 50000 --days 120
python src/etl_pipeline.py
```

## Data Loading Behavior

- The pipeline reads all files in `data/raw/` on each run.
- Upsert logic replaces existing warehouse rows with the same `order_id`.
- This ensures rerun safety and prevents duplicate business keys.

## Warehouse Tables

- `fact_sales`
- `dim_product`
- `dim_customer`
- `dim_date`

## Analytics Marts

- `mart_daily_revenue`
- `mart_top_products`
- `mart_city_performance`

## Output Artifacts

Generated locally after pipeline execution:

- `data/processed/retail_orders_clean.csv`
- `data/processed/mart_daily_revenue.csv`
- `data/processed/mart_top_products.csv`
- `data/processed/mart_city_performance.csv`
- `docs/quality_report.json`
- `warehouse/retail_warehouse.duckdb`

These generated artifacts are intentionally gitignored to keep the repository lightweight.

## Resume Highlights

- Built an end-to-end ETL pipeline for retail analytics with Python and DuckDB.
- Implemented idempotent upsert processing and automated data quality checks.
- Designed fact, dimension, and mart tables for reporting use cases.
