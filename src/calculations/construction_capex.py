import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

def calculate_construction_capex_timeseries(
    asset_id: str,
    total_capex: float,
    construction_start: datetime,
    construction_end: datetime,
    start_date: datetime,
    end_date: datetime,
    max_gearing: float,
    capex_funding_type: str
) -> pd.DataFrame:
    """
    Generates a construction CAPEX schedule for a single asset with linear drawdown.

    Args:
        asset_id (str): The ID of the asset.
        total_capex (float): The total CAPEX amount for the asset.
        construction_start (datetime): The start date of the construction period.
        construction_end (datetime): The end date of the construction period (typically COD).
        start_date (datetime): The overall model start date.
        end_date (datetime): The overall model end date.
        max_gearing (float): The maximum gearing ratio for debt funding.
        capex_funding_type (str): How CAPEX is funded ('equity_first' or 'pari_passu').

    Returns:
        pd.DataFrame: A DataFrame with columns for asset_id, date, capex, equity_capex, and debt_capex.
    """
    date_range = pd.date_range(start=start_date, end=end_date, freq='MS')

    capex_values = []
    equity_capex_values = []
    debt_capex_values = []

    # Calculate total equity and debt required for the asset
    total_debt_funding = total_capex * max_gearing
    total_equity_funding = total_capex * (1 - max_gearing)

    current_equity_funded = 0

    # Calculate construction months
    construction_months = (construction_end.year - construction_start.year) * 12 + \
                          (construction_end.month - construction_start.month)

    monthly_capex_linear = 0
    if construction_months > 0:
        monthly_capex_linear = total_capex / construction_months

    for date in date_range:
        monthly_capex = 0
        monthly_equity_capex = 0
        monthly_debt_capex = 0

        if construction_start <= date < construction_end:
            monthly_capex = monthly_capex_linear
            
            if capex_funding_type == 'equity_first':
                if current_equity_funded < total_equity_funding:
                    equity_needed = total_equity_funding - current_equity_funded
                    monthly_equity_capex = min(monthly_capex, equity_needed)
                    monthly_debt_capex = monthly_capex - monthly_equity_capex
                    current_equity_funded += monthly_equity_capex
                else:
                    monthly_debt_capex = monthly_capex
            elif capex_funding_type == 'pari_passu':
                monthly_equity_capex = monthly_capex * (1 - max_gearing)
                monthly_debt_capex = monthly_capex * max_gearing

        capex_values.append(monthly_capex)
        equity_capex_values.append(monthly_equity_capex)
        debt_capex_values.append(monthly_debt_capex)
    
    asset_capex_df = pd.DataFrame({
        'asset_id': asset_id,
        'date': date_range,
        'capex': capex_values,
        'equity_capex': equity_capex_values,
        'debt_capex': debt_capex_values
    })
    return asset_capex_df
