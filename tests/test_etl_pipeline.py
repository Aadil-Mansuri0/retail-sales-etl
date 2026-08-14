from src.etl_pipeline import run_quality_checks, transform_data


def test_transform_deduplicates_and_calculates_amount() -> None:
    import pandas as pd

    raw = pd.DataFrame(
        [
            {
                "order_id": "O1",
                "order_date": "2025-01-01",
                "updated_at": "2025-01-01T10:00:00Z",
                "customer_id": "C1",
                "product_id": "P1",
                "product_name": "Widget",
                "category": "Tools",
                "city": "Jaipur",
                "quantity": 2,
                "unit_price": 100,
                "discount": 0.10,
                "payment_method": "UPI",
                "status": "Completed",
            },
            {
                "order_id": "O1",
                "order_date": "2025-01-01",
                "updated_at": "2025-01-01T11:00:00Z",
                "customer_id": "C1",
                "product_id": "P1",
                "product_name": "Widget",
                "category": "Tools",
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
    assert clean.iloc[0]["quantity"] == 3
    assert clean.iloc[0]["order_amount"] == 270.0


def test_quality_checks_pass_for_valid_data() -> None:
    import pandas as pd

    frame = pd.DataFrame(
        {
            "order_id": ["O1"],
            "order_date": [pd.Timestamp.today().date()],
            "updated_at": [pd.Timestamp.now()],
            "discount": [0.1],
            "order_amount": [100.0],
        }
    )
    result = run_quality_checks(frame)
    assert result["passed"] is True
