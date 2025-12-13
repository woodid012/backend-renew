import numpy as np
import pandas as pd
from datetime import datetime

from ..config import MERCHANT_PRICE_ESCALATION_RATE, MERCHANT_PRICE_ESCALATION_REFERENCE_DATE


def _parse_float(value):
    if value is None:
        return np.nan
    if isinstance(value, str) and value.strip() == "":
        return np.nan
    try:
        return float(value)
    except Exception:
        return np.nan


def _month_diff_years(d1: datetime, d0: datetime) -> float:
    return (d1.year - d0.year) + (d1.month - d0.month) / 12


def _merchant_escalation_factor(date: datetime) -> float:
    reference_date = datetime.strptime(MERCHANT_PRICE_ESCALATION_REFERENCE_DATE, "%Y-%m-%d")
    years_from_reference = _month_diff_years(date, reference_date)
    return (1 + MERCHANT_PRICE_ESCALATION_RATE) ** max(0, years_from_reference)


def _ensure_monthly_prices_time(monthly_prices: pd.DataFrame) -> pd.DataFrame:
    if "_time_dt" not in monthly_prices.columns:
        # NOTE: matches existing parsing logic in src/calculations/price_curves.py
        monthly_prices["_time_dt"] = pd.to_datetime(monthly_prices["time"], format="%d/%m/%Y", errors="coerce")
    return monthly_prices


def _find_monthly_price_base(
    monthly_prices: pd.DataFrame,
    *,
    profile: str,
    price_type: str,
    region: str,
    date: datetime,
    max_lookback_months: int = 12 * 5,
):
    """
    Returns (base_price, source_date) where base_price is the un-escalated price found in monthly_prices.
    Implements the same backward-search fallback behavior as get_merchant_price().
    """
    monthly_prices = _ensure_monthly_prices_time(monthly_prices)

    filtered = monthly_prices[
        (monthly_prices["profile"] == profile)
        & (monthly_prices["type"] == price_type)
        & (monthly_prices["REGION"] == region)
        & (monthly_prices["_time_dt"].dt.year == date.year)
        & (monthly_prices["_time_dt"].dt.month == date.month)
    ]

    if not filtered.empty:
        return float(filtered["price"].iloc[0]), datetime(date.year, date.month, 1)

    year_to_try = date.year
    month_to_try = date.month
    for _ in range(max_lookback_months):
        month_to_try -= 1
        if month_to_try == 0:
            month_to_try = 12
            year_to_try -= 1

        fallback = monthly_prices[
            (monthly_prices["profile"] == profile)
            & (monthly_prices["type"] == price_type)
            & (monthly_prices["REGION"] == region)
            & (monthly_prices["_time_dt"].dt.year == year_to_try)
            & (monthly_prices["_time_dt"].dt.month == month_to_try)
        ]
        if not fallback.empty:
            return float(fallback["price"].iloc[0]), datetime(year_to_try, month_to_try, 1)

    return np.nan, pd.NaT


def _profile_for_asset(asset_type: str) -> str:
    profile_map = {"solar": "solar", "wind": "wind", "storage": "storage"}
    return profile_map.get(asset_type, asset_type)


def _compute_contract_strikes_for_month(contract: dict, current_date: datetime):
    """
    Returns a dict with raw/used strike values split by curve (green/black).

    Notes:
    - "black" maps to the model's Energy curve.
    - "used" reflects indexation + floor logic to mirror src/calculations/contracts.py.
    """
    contract_start_date = datetime.strptime(contract["startDate"], "%Y-%m-%d")
    years_in_contract = _month_diff_years(current_date, contract_start_date)
    indexation = _parse_float(contract.get("indexation", 0)) / 100
    indexation_factor = (1 + indexation) ** max(0, years_in_contract)

    has_floor = bool(contract.get("hasFloor"))
    floor_value = _parse_float(contract.get("floorValue", np.nan))

    contract_type = contract.get("type")

    # Defaults
    raw_green = np.nan
    raw_black = np.nan
    used_green = np.nan
    used_black = np.nan

    if contract_type == "bundled":
        raw_green = _parse_float(contract.get("greenPrice", np.nan))
        raw_black = _parse_float(contract.get("EnergyPrice", np.nan))

        used_green = raw_green * indexation_factor if pd.notna(raw_green) else np.nan
        used_black = raw_black * indexation_factor if pd.notna(raw_black) else np.nan

        if has_floor and pd.notna(floor_value):
            total = (used_green if pd.notna(used_green) else 0) + (used_black if pd.notna(used_black) else 0)
            if total < floor_value:
                if total > 0:
                    used_green = (used_green / total) * floor_value if pd.notna(used_green) else 0
                    used_black = (used_black / total) * floor_value if pd.notna(used_black) else 0
                else:
                    used_green = floor_value / 2
                    used_black = floor_value / 2

    elif contract_type in ("green", "Energy", "fixed"):
        # In model logic:
        # - "green" is the green curve
        # - "Energy" is the black curve
        # - "fixed" uses strikePrice (treated as energy-side in contracts.py)
        raw = _parse_float(contract.get("strikePrice", np.nan))
        used = raw * indexation_factor if pd.notna(raw) else np.nan

        if has_floor and pd.notna(floor_value) and pd.notna(used) and used < floor_value:
            used = floor_value

        if contract_type == "green":
            raw_green, used_green = raw, used
        else:
            raw_black, used_black = raw, used

    else:
        # Unknown contract types -> leave as NaN
        pass

    return {
        "strike_green_raw": raw_green,
        "strike_green_used": used_green,
        "strike_black_raw": raw_black,
        "strike_black_used": used_black,
        "indexation_factor": indexation_factor,
    }


def build_inputs_audit_timeseries(
    assets: list,
    monthly_prices: pd.DataFrame,
    yearly_spreads: pd.DataFrame,  # kept for signature parity/future extension (storage)
    start_date: datetime,
    end_date: datetime,
) -> pd.DataFrame:
    """
    Build a monthly audit table of raw + used inputs by period.

    Output rows: one per (asset_id, date).
    """
    date_range = pd.date_range(start=start_date, end=end_date, freq="MS")
    rows = []

    # Determine maximum number of contracts across assets to keep a consistent column set.
    max_contracts = max((len(a.get("contracts", []) or []) for a in assets), default=0)

    for asset in assets:
        asset_id = asset.get("id")
        asset_type = asset.get("type")
        region = asset.get("region")
        profile = _profile_for_asset(asset_type)

        contracts = asset.get("contracts", []) or []

        for current_date in date_range:
            escalation_factor = _merchant_escalation_factor(current_date.to_pydatetime())

            green_base, green_source_date = _find_monthly_price_base(
                monthly_prices, profile=profile, price_type="green", region=region, date=current_date.to_pydatetime()
            )
            black_base, black_source_date = _find_monthly_price_base(
                monthly_prices, profile=profile, price_type="Energy", region=region, date=current_date.to_pydatetime()
            )

            row = {
                "asset_id": asset_id,
                "date": pd.to_datetime(current_date),
                "profile": profile,
                "region": region,
                "market_price_escalation_factor": escalation_factor,
                "market_price_green_raw_$": green_base,
                "market_price_green_used_$": green_base * escalation_factor if pd.notna(green_base) else np.nan,
                "market_price_green_source_date": green_source_date,
                "market_price_black_raw_$": black_base,
                "market_price_black_used_$": black_base * escalation_factor if pd.notna(black_base) else np.nan,
                "market_price_black_source_date": black_source_date,
            }

            for idx in range(max_contracts):
                n = idx + 1
                prefix = f"contract_{n}"

                if idx < len(contracts):
                    c = contracts[idx]
                    c_start = datetime.strptime(c["startDate"], "%Y-%m-%d")
                    c_end = datetime.strptime(c["endDate"], "%Y-%m-%d")
                    is_active = c_start <= current_date.to_pydatetime() <= c_end

                    strikes = _compute_contract_strikes_for_month(c, current_date.to_pydatetime())

                    row.update(
                        {
                            f"{prefix}_type": c.get("type", np.nan),
                            f"{prefix}_buyers_percentage": _parse_float(c.get("buyersPercentage", np.nan)),
                            f"{prefix}_start_date": c_start,
                            f"{prefix}_end_date": c_end,
                            f"{prefix}_is_active": bool(is_active),
                            f"{prefix}_strike_green_raw_$": strikes["strike_green_raw"],
                            f"{prefix}_strike_green_used_$": strikes["strike_green_used"],
                            f"{prefix}_strike_black_raw_$": strikes["strike_black_raw"],
                            f"{prefix}_strike_black_used_$": strikes["strike_black_used"],
                            f"{prefix}_indexation_factor": strikes["indexation_factor"],
                        }
                    )
                else:
                    # pad to max contracts with NaNs
                    row.update(
                        {
                            f"{prefix}_type": np.nan,
                            f"{prefix}_buyers_percentage": np.nan,
                            f"{prefix}_start_date": pd.NaT,
                            f"{prefix}_end_date": pd.NaT,
                            f"{prefix}_is_active": False,
                            f"{prefix}_strike_green_raw_$": np.nan,
                            f"{prefix}_strike_green_used_$": np.nan,
                            f"{prefix}_strike_black_raw_$": np.nan,
                            f"{prefix}_strike_black_used_$": np.nan,
                            f"{prefix}_indexation_factor": np.nan,
                        }
                    )

            rows.append(row)

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df = df.sort_values(["asset_id", "date"]).reset_index(drop=True)
    return df




