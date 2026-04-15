from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

PRODUCT_CATALOG = [
    {"product_id": "P001", "product_name": "Wireless Mouse", "category": "Accessories", "base_price": 799},
    {"product_id": "P002", "product_name": "Mechanical Keyboard", "category": "Accessories", "base_price": 2499},
    {"product_id": "P003", "product_name": "Noise Cancelling Headphones", "category": "Audio", "base_price": 6999},
    {"product_id": "P004", "product_name": "USB-C Charger", "category": "Power", "base_price": 1199},
    {"product_id": "P005", "product_name": "Smart Watch", "category": "Wearables", "base_price": 9999},
    {"product_id": "P006", "product_name": "Webcam HD", "category": "Accessories", "base_price": 3499},
    {"product_id": "P007", "product_name": "Gaming Controller", "category": "Gaming", "base_price": 2999},
    {"product_id": "P008", "product_name": "Portable SSD 1TB", "category": "Storage", "base_price": 7499},
    {"product_id": "P009", "product_name": "Bluetooth Speaker", "category": "Audio", "base_price": 4299},
    {"product_id": "P010", "product_name": "Laptop Stand", "category": "Accessories", "base_price": 1499},
]

CITIES = [
    "Delhi",
    "Mumbai",
    "Bengaluru",
    "Pune",
    "Hyderabad",
    "Chennai",
    "Kolkata",
    "Ahmedabad",
    "Jaipur",
    "Lucknow",
]

PAYMENT_METHODS = ["UPI", "Card", "NetBanking", "COD", "Wallet"]
STATUS_VALUES = ["Completed", "Completed", "Completed", "Refunded", "Cancelled"]
DISCOUNT_VALUES = [0.0, 0.0, 0.0, 0.05, 0.1, 0.15, 0.2]


def parse_args() -> argparse.Namespace:
    default_output_dir = Path(__file__).resolve().parents[1] / "data" / "raw"

    parser = argparse.ArgumentParser(description="Generate synthetic retail raw data for ETL pipeline.")
    parser.add_argument("--rows", type=int, default=50000, help="Number of base rows to generate.")
    parser.add_argument("--days", type=int, default=120, help="Days of historical data to simulate.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=default_output_dir,
        help="Directory where raw CSV file will be stored.",
    )
    return parser.parse_args()


def generate_orders(rows: int, days: int, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    catalog_df = pd.DataFrame(PRODUCT_CATALOG)

    start_ts = pd.Timestamp.today().normalize() - pd.Timedelta(days=days)
    random_minutes = rng.integers(0, max(days * 24 * 60, 1), size=rows)
    order_ts = start_ts + pd.to_timedelta(random_minutes, unit="m")
    updated_ts = order_ts + pd.to_timedelta(rng.integers(5, 360, size=rows), unit="m")

    product_idx = rng.integers(0, len(catalog_df), size=rows)
    product_rows = catalog_df.iloc[product_idx].reset_index(drop=True)

    base_price = product_rows["base_price"].to_numpy(dtype=float)
    price_multiplier = rng.uniform(0.85, 1.25, size=rows)
    unit_price = np.round(base_price * price_multiplier, 2)

    data = pd.DataFrame(
        {
            "order_id": [f"ORD{100000 + i:07d}" for i in range(rows)],
            "order_date": order_ts,
            "updated_at": updated_ts,
            "customer_id": [f"CUST{rng.integers(1000, 9000):05d}" for _ in range(rows)],
            "product_id": product_rows["product_id"],
            "product_name": product_rows["product_name"],
            "category": product_rows["category"],
            "city": rng.choice(CITIES, size=rows),
            "quantity": rng.integers(1, 6, size=rows),
            "unit_price": unit_price,
            "discount": rng.choice(DISCOUNT_VALUES, size=rows),
            "payment_method": rng.choice(PAYMENT_METHODS, size=rows),
            "status": rng.choice(STATUS_VALUES, size=rows),
        }
    )

    # Inject a small amount of bad data to validate ETL cleaning logic.
    city_null_idx = rng.choice(data.index, size=max(1, rows // 120), replace=False)
    data.loc[city_null_idx, "city"] = None

    neg_qty_idx = rng.choice(data.index, size=max(1, rows // 160), replace=False)
    data.loc[neg_qty_idx, "quantity"] = -data.loc[neg_qty_idx, "quantity"].abs()

    neg_price_idx = rng.choice(data.index, size=max(1, rows // 180), replace=False)
    data.loc[neg_price_idx, "unit_price"] = -data.loc[neg_price_idx, "unit_price"].abs()

    duplicate_idx = rng.choice(data.index, size=max(1, rows // 50), replace=False)
    duplicate_rows = data.loc[duplicate_idx].copy()
    duplicate_rows["updated_at"] = pd.to_datetime(duplicate_rows["updated_at"]) + pd.to_timedelta(
        rng.integers(60, 360, size=len(duplicate_rows)), unit="m"
    )
    duplicate_rows["quantity"] = duplicate_rows["quantity"].abs() + 1

    dirty_data = pd.concat([data, duplicate_rows], ignore_index=True)
    dirty_data["order_date"] = pd.to_datetime(dirty_data["order_date"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    dirty_data["updated_at"] = pd.to_datetime(dirty_data["updated_at"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    return dirty_data


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    output_df = generate_orders(rows=args.rows, days=args.days, seed=args.seed)
    output_file = args.output_dir / f"retail_orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    output_df.to_csv(output_file, index=False)

    duplicate_count = int(output_df["order_id"].duplicated().sum())
    print(f"Generated file: {output_file}")
    print(f"Rows written: {len(output_df):,}")
    print(f"Duplicate order_id rows intentionally present: {duplicate_count:,}")


if __name__ == "__main__":
    main()
