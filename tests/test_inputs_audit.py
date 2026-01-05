import pandas as pd
from datetime import datetime

from src.core.inputs_audit import build_inputs_audit_timeseries


def test_build_inputs_audit_timeseries_has_expected_columns_and_rows():
    assets = [
        {
            "id": 1,
            "type": "solar",
            "region": "NSW",
            "contracts": [
                {
                    "type": "green",
                    "startDate": "2025-01-01",
                    "endDate": "2025-12-31",
                    "buyersPercentage": 50,
                    "strikePrice": 45,
                    "indexation": 2.0,
                    "hasFloor": True,
                    "floorValue": 40,
                },
                {
                    "type": "Energy",
                    "startDate": "2025-01-01",
                    "endDate": "2025-12-31",
                    "buyersPercentage": 50,
                    "strikePrice": 55,
                    "indexation": 0.0,
                },
            ],
        }
    ]

    monthly_prices = pd.DataFrame(
        [
            {"time": "01/01/2025", "profile": "solar", "type": "green", "REGION": "NSW", "price": 60},
            {"time": "01/01/2025", "profile": "solar", "type": "Energy", "REGION": "NSW", "price": 70},
            {"time": "01/02/2025", "profile": "solar", "type": "green", "REGION": "NSW", "price": 61},
            {"time": "01/02/2025", "profile": "solar", "type": "Energy", "REGION": "NSW", "price": 71},
        ]
    )

    yearly_spreads = pd.DataFrame(columns=["REGION", "YEAR", "DURATION", "SPREAD"])

    df = build_inputs_audit_timeseries(
        assets=assets,
        monthly_prices=monthly_prices,
        yearly_spreads=yearly_spreads,
        start_date=datetime(2025, 1, 1),
        end_date=datetime(2025, 2, 1),
    )

    assert len(df) == 2
    assert set(["asset_id", "date"]).issubset(df.columns)
    assert set(["market_price_green_raw_$", "market_price_green_used_$"]).issubset(df.columns)
    assert set(["market_price_black_raw_$", "market_price_black_used_$"]).issubset(df.columns)
    assert "contract_1_strike_green_used_$" in df.columns
    assert "contract_2_strike_black_used_$" in df.columns














