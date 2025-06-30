
import pandas as pd
from dateutil.relativedelta import relativedelta
from config import ENABLE_TERMINAL_VALUE

def aggregate_cashflows(revenue, opex, capex, debt_schedule, end_date, assets_data, asset_cost_assumptions):
    """
    Aggregates all financial components into a final cash flow statement for each asset.

    Args:
        revenue (pd.DataFrame): Time-series of revenue.
        opex (pd.DataFrame): Time-series of opex.
        capex (pd.DataFrame): Time-series of capex.
        debt_schedule (pd.DataFrame): Time-series of debt schedule.
        end_date (datetime): The end date of the analysis period.

    Returns:
        pd.DataFrame: A DataFrame with the consolidated cash flow for each asset.
    """
    # Merge the financial components
    cash_flow = pd.merge(revenue, opex, on=['asset_id', 'date'], how='left')
    cash_flow = pd.merge(cash_flow, capex, on=['asset_id', 'date'], how='left')
    cash_flow = pd.merge(cash_flow, debt_schedule, on=['asset_id', 'date'], how='left')

    # Fill NaNs for assets that might not have all components
    cash_flow.fillna(0, inplace=True)

    # Calculate key cash flow lines
    cash_flow['cfads'] = cash_flow['revenue'] - cash_flow['opex']
    # Calculate debt service for DSCR
    debt_service = cash_flow['interest'] + cash_flow['principal']
    # Handle division by zero for DSCR
    cash_flow['dscr'] = cash_flow.apply(lambda row: row['cfads'] / debt_service[row.name] if debt_service[row.name] != 0 else None, axis=1)
    cash_flow['equity_cash_flow'] = cash_flow['cfads'] - cash_flow['interest'] - cash_flow['principal'] - cash_flow['equity_capex']

    # Calculate Terminal Value
    if ENABLE_TERMINAL_VALUE:
        # Iterate through each asset to apply its specific terminal value
        for asset_info in assets_data:
            asset_id = asset_info['id']
            asset_name = asset_info['name']
            asset_start_date = pd.to_datetime(asset_info['OperatingStartDate'])
            asset_life_years = int(asset_info.get('assetLife', 25))
            
            # Calculate the exact end date of the asset's life
            asset_life_end_date = asset_start_date + relativedelta(years=asset_life_years)
            
            if asset_name and asset_name in asset_cost_assumptions:
                asset_tv = asset_cost_assumptions[asset_name].get('terminalValue', 0.0)
                
                if asset_tv > 0:
                    # Find the cash flow entry for the month *before* asset_life_end_date
                    # This is because terminal value is typically at the end of the last operating period
                    terminal_value_date = asset_life_end_date - relativedelta(months=1)

                    # Ensure the terminal_value_date is within the cash_flow DataFrame's dates
                    if terminal_value_date in cash_flow['date'].values:
                        cash_flow.loc[
                            (cash_flow['asset_id'] == asset_id) & (cash_flow['date'] == terminal_value_date),
                            'terminal_value'
                        ] = asset_tv
                        cash_flow.loc[
                            (cash_flow['asset_id'] == asset_id) & (cash_flow['date'] == terminal_value_date),
                            'equity_cash_flow'
                        ] += asset_tv

    return cash_flow
