import pandas as pd

from src.etl_pipeline import run_quality_checks, transform_data


def test_transform_deduplicates_and_calculates_order_amount() -> None:
    raw = pd.DataFrame(
        [
            {
                "order_id": "A-1",
                "order_date": "2025-01-01",
                "updated_at": "2025-01-01T10:00:00",
                "customer_id": "C-1",
                "product_id": "P-1",
                "product_name": "Notebook",
                "category": "Stationery",
                "city": "Jaipur",
                "quantity": 2,
                "unit_price": 100,
                "discount": 0.10,
                "payment_method": "UPI",
                "status": "Completed",
            },
            {
                "order_id": "A-1",
                "order_date": "2025-01-01",
                "updated_at": "2025-01-01T11:00:00",
                "customer_id": "C-1",
                "product_id": "P-1",
                "product_name": "Notebook",
                "category": "Stationery",
                "city": "Jaipur",
                "quantity": 3,
                "unit_price": 100,
                "discount": 0.10,
                "payment_method": "UPI",
                "status": "Completed",
            },
        ]
    )

    clean = transform_data(raw)

    assert len(clean) == 1
    assert clean.iloc[0]["order_id"] == "A-1"
    assert clean.iloc[0]["quantity"] == 3
    assert clean.iloc[0]["order_amount"] == 270.0


def test_quality_checks_pass_for_valid_transformed_data() -> None:
    raw = pd.DataFrame(
        [
            {
                "order_id": "A-1",
                "order_date": "2020-01-01",
                "updated_at": "2020-01-01T10:00:00",
                "customer_id": "C-1",
                "product_id": "P-1",
                "product_name": "Notebook",
                "category": "Stationery",
                "city": "Jaipur",
                "quantity": 1,
                "unit_price": 50,
                "discount": 0,
                "payment_method": "UPI",
                "status": "Completed",
            }
        ]
    )

    checks = run_quality_checks(transform_data(raw))

    assert checks["passed"] is True
    assert checks["duplicate_order_id"] == 0
