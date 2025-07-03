
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from ..config import DEFAULT_CAPEX_FUNDING_TYPE

def calculate_opex_timeseries(assets, opex_assumptions, start_date, end_date):
    """
    Calculates monthly operating expenses (OPEX) for each asset.

    Args:
        assets (list): A list of asset dictionaries.
        opex_assumptions (dict): A dictionary with OPEX assumptions for each asset type.
        start_date (datetime): The start date of the analysis period.
        end_date (datetime): The end date of the analysis period.

    Returns:
        pd.DataFrame: A DataFrame with columns for asset_id, date, and opex.
    """
    all_opex_data = []
    date_range = pd.date_range(start=start_date, end=end_date, freq='MS')

    for asset in assets:
        asset_assumptions = opex_assumptions.get(asset['name'], {})
        base_opex = asset_assumptions.get('operatingCosts', 0)
        escalation = asset_assumptions.get('operatingCostEscalation', 0) / 100

        opex_values = []
        asset_start_date = pd.to_datetime(asset['OperatingStartDate'])
        asset_life_end_date = asset_start_date + relativedelta(years=int(asset.get('assetLife', 25)))

        for date in date_range:
            monthly_opex = 0
            if date >= asset_start_date and date < asset_life_end_date:
                years_from_cod = (date.year - asset_start_date.year)
                # Apply escalation
                escalated_opex = base_opex * ((1 + escalation) ** years_from_cod)
                monthly_opex = escalated_opex / 12
            opex_values.append(monthly_opex)

        asset_opex_df = pd.DataFrame({
            'asset_id': asset['id'],
            'date': date_range,
            'opex': opex_values
        })
        all_opex_data.append(asset_opex_df)

    if not all_opex_data:
        return pd.DataFrame(columns=['asset_id', 'date', 'opex'])

    return pd.concat(all_opex_data, ignore_index=True)

from .construction_capex import calculate_construction_capex_timeseries

def calculate_capex_timeseries(assets, capex_assumptions, start_date, end_date, capex_funding_type=DEFAULT_CAPEX_FUNDING_TYPE):
    """
    Aggregates CAPEX schedules for all assets.

    Args:
        assets (list): A list of asset dictionaries.
        capex_assumptions (dict): A dictionary with CAPEX assumptions.
        start_date (datetime): The start date of the analysis period.
        end_date (datetime): The end date of the analysis period.
        capex_funding_type (str): How CAPEX is funded ('equity_first' or 'pari_passu').

    Returns:
        pd.DataFrame: A DataFrame with columns for asset_id, date, capex, equity_capex, and debt_capex.
    """
    all_capex_data = []

    for asset in assets:
        asset_assumptions = capex_assumptions.get(asset['name'], {})
        total_capex = asset_assumptions.get('capex', 0)
        max_gearing = asset_assumptions.get('maxGearing', 0.7) # Default to 70% gearing
        
        construction_start = pd.to_datetime(asset['constructionStartDate'])
        construction_end = pd.to_datetime(asset['OperatingStartDate']) # Assuming OperatingStartDate is COD

        if total_capex > 0:
            asset_capex_df = calculate_construction_capex_timeseries(
                asset_id=asset['id'],
                total_capex=total_capex,
                construction_start=construction_start,
                construction_end=construction_end,
                start_date=start_date,
                end_date=end_date,
                max_gearing=max_gearing,
                capex_funding_type=capex_funding_type
            )
            all_capex_data.append(asset_capex_df)

    if not all_capex_data:
        return pd.DataFrame(columns=['asset_id', 'date', 'capex', 'equity_capex', 'debt_capex'])

    return pd.concat(all_capex_data, ignore_index=True)
