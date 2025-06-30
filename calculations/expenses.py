
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from config import DEFAULT_CAPEX_FUNDING_TYPE

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

def calculate_capex_timeseries(assets, capex_assumptions, start_date, end_date, capex_funding_type=DEFAULT_CAPEX_FUNDING_TYPE):
    """
    Creates a CAPEX schedule for each asset, splitting it into equity and debt components.

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
    date_range = pd.date_range(start=start_date, end=end_date, freq='MS')

    for asset in assets:
        asset_assumptions = capex_assumptions.get(asset['name'], {})
        total_capex = asset_assumptions.get('capex', 0)
        max_gearing = asset_assumptions.get('maxGearing', 0.7) # Default to 70% gearing
        
        construction_start = pd.to_datetime(asset['constructionStartDate'])
        construction_end = pd.to_datetime(asset['OperatingStartDate']) # Assuming OperatingStartDate is COD

        capex_values = []
        equity_capex_values = []
        debt_capex_values = []

        # Calculate total equity and debt required for the asset
        total_debt_funding = total_capex * max_gearing
        total_equity_funding = total_capex * (1 - max_gearing)

        current_equity_funded = 0

        for date in date_range:
            monthly_capex = 0
            monthly_equity_capex = 0
            monthly_debt_capex = 0

            if construction_start <= date < construction_end:
                # Simple straight-line construction spend
                construction_months = (construction_end.year - construction_start.year) * 12 + (construction_end.month - construction_start.month)
                if construction_months > 0:
                    monthly_capex = total_capex / construction_months
                
                if capex_funding_type == 'equity_first':
                    # Fund with equity first
                    if current_equity_funded < total_equity_funding:
                        # Still funding with equity
                        equity_needed = total_equity_funding - current_equity_funded
                        monthly_equity_capex = min(monthly_capex, equity_needed)
                        monthly_debt_capex = monthly_capex - monthly_equity_capex
                        current_equity_funded += monthly_equity_capex
                    else:
                        # Equity exhausted, fund with debt
                        monthly_debt_capex = monthly_capex
                        
                elif capex_funding_type == 'pari_passu':
                    # Fund proportionally
                    monthly_equity_capex = monthly_capex * (1 - max_gearing)
                    monthly_debt_capex = monthly_capex * max_gearing

            capex_values.append(monthly_capex)
            equity_capex_values.append(monthly_equity_capex)
            debt_capex_values.append(monthly_debt_capex)
        
        asset_capex_df = pd.DataFrame({
            'asset_id': asset['id'],
            'date': date_range,
            'capex': capex_values,
            'equity_capex': equity_capex_values,
            'debt_capex': debt_capex_values
        })
        all_capex_data.append(asset_capex_df)

    if not all_capex_data:
        return pd.DataFrame(columns=['asset_id', 'date', 'capex', 'equity_capex', 'debt_capex'])

    return pd.concat(all_capex_data, ignore_index=True)
