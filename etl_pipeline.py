from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT_DIR / "data" / "raw"
PROCESSED_DIR = ROOT_DIR / "data" / "processed"
WAREHOUSE_PATH = ROOT_DIR / "warehouse" / "retail_warehouse.duckdb"
QUALITY_REPORT_PATH = ROOT_DIR / "docs" / "quality_report.json"

REQUIRED_COLUMNS = {
    "order_id",
    "order_date",
    "updated_at",
    "customer_id",
    "product_id",
    "product_name",
    "category",
    "city",
    "quantity",
    "unit_price",
    "discount",
    "payment_method",
    "status",
}


def extract_raw_data(raw_dir: Path) -> pd.DataFrame:
    raw_files = sorted(raw_dir.glob("*.csv"))
    if not raw_files:
        raise FileNotFoundError(f"No raw CSV files found in {raw_dir}")

    frames: list[pd.DataFrame] = []
    for path in raw_files:
        frame = pd.read_csv(path)
        frame["source_file"] = path.name
        frames.append(frame)

    return pd.concat(frames, ignore_index=True)


def transform_data(raw_df: pd.DataFrame) -> pd.DataFrame:
    missing = REQUIRED_COLUMNS - set(raw_df.columns)
    if missing:
        missing_cols = ", ".join(sorted(missing))
        raise ValueError(f"Raw data is missing required columns: {missing_cols}")

    df = raw_df.copy()
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")

    for numeric_col in ["quantity", "unit_price", "discount"]:
        df[numeric_col] = pd.to_numeric(df[numeric_col], errors="coerce")

    df["discount"] = df["discount"].fillna(0).clip(0, 0.9)
    df["city"] = df["city"].fillna("Unknown")
    df["payment_method"] = df["payment_method"].fillna("Unknown")
    df["status"] = df["status"].fillna("Completed")

    df = df.dropna(subset=["order_id", "product_id", "order_date", "quantity", "unit_price"])
    df = df[(df["quantity"] > 0) & (df["unit_price"] > 0)]

    df["updated_at"] = df["updated_at"].fillna(df["order_date"])
    df = df.sort_values(["order_id", "updated_at"]).drop_duplicates(subset=["order_id"], keep="last")

    df["order_date"] = pd.to_datetime(df["order_date"]).dt.date
    df["order_amount"] = (df["quantity"] * df["unit_price"] * (1 - df["discount"]))
    df["order_amount"] = df["order_amount"].round(2)

    loaded_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    df["loaded_at"] = loaded_at

    final_cols = [
        "order_id",
        "order_date",
        "customer_id",
        "product_id",
        "product_name",
        "category",
        "city",
        "quantity",
        "unit_price",
        "discount",
        "order_amount",
        "payment_method",
        "status",
        "updated_at",
        "loaded_at",
        "source_file",
    ]
    return df[final_cols].reset_index(drop=True)


def run_quality_checks(clean_df: pd.DataFrame) -> dict[str, int | bool]:
    today = pd.Timestamp.today().date()
    order_dates = pd.to_datetime(clean_df["order_date"]).dt.date

    checks: dict[str, int | bool] = {
        "rows_after_transform": int(len(clean_df)),
        "null_order_id": int(clean_df["order_id"].isna().sum()),
        "duplicate_order_id": int(clean_df["order_id"].duplicated().sum()),
        "non_positive_amount": int((clean_df["order_amount"] <= 0).sum()),
        "future_order_date": int((order_dates > today).sum()),
        "invalid_discount": int(((clean_df["discount"] < 0) | (clean_df["discount"] > 0.9)).sum()),
    }

    checks["passed"] = bool(
        checks["rows_after_transform"] > 0
        and checks["null_order_id"] == 0
        and checks["duplicate_order_id"] == 0
        and checks["non_positive_amount"] == 0
        and checks["future_order_date"] == 0
        and checks["invalid_discount"] == 0
    )
    return checks


def load_to_warehouse(clean_df: pd.DataFrame) -> dict[str, int]:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    WAREHOUSE_PATH.parent.mkdir(parents=True, exist_ok=True)

    clean_output_path = PROCESSED_DIR / "retail_orders_clean.csv"
    clean_df.to_csv(clean_output_path, index=False)

    con = duckdb.connect(str(WAREHOUSE_PATH))
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS fact_sales (
                order_id VARCHAR,
                order_date DATE,
                customer_id VARCHAR,
                product_id VARCHAR,
                product_name VARCHAR,
                category VARCHAR,
                city VARCHAR,
                quantity INTEGER,
                unit_price DOUBLE,
                discount DOUBLE,
                order_amount DOUBLE,
                payment_method VARCHAR,
                status VARCHAR,
                updated_at TIMESTAMP,
                loaded_at TIMESTAMP,
                source_file VARCHAR
            )
            """
        )

        con.register("stg_sales", clean_df)
        con.execute("DELETE FROM fact_sales WHERE order_id IN (SELECT order_id FROM stg_sales)")
        con.execute(
            """
            INSERT INTO fact_sales
            SELECT
                order_id,
                CAST(order_date AS DATE),
                CAST(customer_id AS VARCHAR),
                CAST(product_id AS VARCHAR),
                CAST(product_name AS VARCHAR),
                CAST(category AS VARCHAR),
                CAST(city AS VARCHAR),
                CAST(quantity AS INTEGER),
                CAST(unit_price AS DOUBLE),
                CAST(discount AS DOUBLE),
                CAST(order_amount AS DOUBLE),
                CAST(payment_method AS VARCHAR),
                CAST(status AS VARCHAR),
                CAST(updated_at AS TIMESTAMP),
                CAST(loaded_at AS TIMESTAMP),
                CAST(source_file AS VARCHAR)
            FROM stg_sales
            """
        )

        con.execute(
            """
            CREATE OR REPLACE TABLE dim_product AS
            SELECT DISTINCT product_id, product_name, category
            FROM fact_sales
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE dim_customer AS
            SELECT DISTINCT customer_id, city
            FROM fact_sales
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE dim_date AS
            SELECT DISTINCT
                order_date,
                EXTRACT(YEAR FROM order_date) AS year,
                EXTRACT(MONTH FROM order_date) AS month,
                EXTRACT(DAY FROM order_date) AS day,
                STRFTIME(order_date, '%Y-%m') AS year_month,
                EXTRACT(DAYOFWEEK FROM order_date) AS day_of_week
            FROM fact_sales
            """
        )

        con.execute(
            """
            CREATE OR REPLACE TABLE mart_daily_revenue AS
            SELECT
                order_date,
                COUNT(*) AS total_orders,
                SUM(CASE WHEN status = 'Completed' THEN order_amount ELSE 0 END) AS gross_revenue,
                SUM(CASE WHEN status = 'Refunded' THEN order_amount ELSE 0 END) AS refunded_amount,
                SUM(
                    CASE
                        WHEN status = 'Completed' THEN order_amount
                        WHEN status = 'Refunded' THEN -order_amount
                        ELSE 0
                    END
                ) AS net_revenue
            FROM fact_sales
            GROUP BY 1
            ORDER BY 1
            """
        )

        con.execute(
            """
            CREATE OR REPLACE TABLE mart_top_products AS
            SELECT
                product_id,
                product_name,
                category,
                COUNT(*) AS total_orders,
                SUM(CASE WHEN status = 'Completed' THEN order_amount ELSE 0 END) AS revenue
            FROM fact_sales
            GROUP BY 1, 2, 3
            ORDER BY revenue DESC
            LIMIT 20
            """
        )

        con.execute(
            """
            CREATE OR REPLACE TABLE mart_city_performance AS
            SELECT
                city,
                COUNT(*) AS total_orders,
                SUM(CASE WHEN status = 'Completed' THEN order_amount ELSE 0 END) AS gross_revenue,
                AVG(CASE WHEN status = 'Completed' THEN order_amount END) AS avg_order_value
            FROM fact_sales
            GROUP BY 1
            ORDER BY gross_revenue DESC
            """
        )

        export_tables = ["mart_daily_revenue", "mart_top_products", "mart_city_performance"]
        for table_name in export_tables:
            output_path = (PROCESSED_DIR / f"{table_name}.csv").as_posix()
            con.execute(f"COPY {table_name} TO '{output_path}' (HEADER, DELIMITER ',')")

        counts = {
            "fact_sales": int(con.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]),
            "dim_product": int(con.execute("SELECT COUNT(*) FROM dim_product").fetchone()[0]),
            "dim_customer": int(con.execute("SELECT COUNT(*) FROM dim_customer").fetchone()[0]),
            "mart_daily_revenue": int(con.execute("SELECT COUNT(*) FROM mart_daily_revenue").fetchone()[0]),
            "mart_top_products": int(con.execute("SELECT COUNT(*) FROM mart_top_products").fetchone()[0]),
            "mart_city_performance": int(con.execute("SELECT COUNT(*) FROM mart_city_performance").fetchone()[0]),
        }
    finally:
        con.close()

    return counts


def write_quality_report(report: dict[str, object]) -> None:
    QUALITY_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with QUALITY_REPORT_PATH.open("w", encoding="utf-8") as fp:
        json.dump(report, fp, indent=2)


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    raw_df = extract_raw_data(RAW_DIR)
    clean_df = transform_data(raw_df)

    quality_checks = run_quality_checks(clean_df)
    warehouse_counts = load_to_warehouse(clean_df)

    report: dict[str, object] = {
        "run_timestamp_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "raw_rows": int(len(raw_df)),
        "clean_rows": int(len(clean_df)),
        "quality_checks": quality_checks,
        "warehouse_counts": warehouse_counts,
    }
    write_quality_report(report)

    print("Retail ETL run completed")
    print(f"Raw rows: {len(raw_df):,}")
    print(f"Clean rows: {len(clean_df):,}")
    print(f"Quality checks passed: {quality_checks['passed']}")
    print(f"Warehouse file: {WAREHOUSE_PATH}")
    print(f"Quality report: {QUALITY_REPORT_PATH}")

    if not quality_checks["passed"]:
        raise SystemExit("Quality checks failed. Review docs/quality_report.json")


if __name__ == "__main__":
    main()
