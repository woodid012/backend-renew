
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


