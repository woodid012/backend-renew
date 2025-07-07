import pandas as pd
from dateutil.relativedelta import relativedelta
from ..config import ENABLE_TERMINAL_VALUE, TAX_RATE, MIN_CASH_BALANCE_FOR_DISTRIBUTION
from .tax import calculate_tax_expense


def aggregate_cashflows(revenue, opex, capex, debt_schedule, d_and_a_df, end_date, assets_data, asset_cost_assumptions):
    """
    Aggregates all financial components into a final cash flow statement for each asset.

    Args:
        revenue (pd.DataFrame): Time-series of revenue.
        opex (pd.DataFrame): Time-series of opex.
        capex (pd.DataFrame): Time-series of capex.
        debt_schedule (pd.DataFrame): Time-series of debt schedule.
        depreciation_df (pd.DataFrame): Time-series of depreciation.
        end_date (datetime): The end date of the analysis period.
        assets_data (list): List of asset dictionaries.
        asset_cost_assumptions (dict): Asset cost assumptions.

    Returns:
        pd.DataFrame: A DataFrame with the consolidated cash flow for each asset.
    """
    # Merge the financial components
    cash_flow = pd.merge(revenue, opex, on=['asset_id', 'date'], how='left')
    cash_flow = pd.merge(cash_flow, capex, on=['asset_id', 'date'], how='left')
    cash_flow = pd.merge(cash_flow, debt_schedule, on=['asset_id', 'date'], how='left')
    cash_flow = pd.merge(cash_flow, d_and_a_df, on=['asset_id', 'date'], how='left')
    cash_flow['d_and_a'] = pd.to_numeric(cash_flow['d_and_a'], errors='coerce').fillna(0)

    # Fill NaNs for assets that might not have all components
    cash_flow.fillna(0, inplace=True)

    # Calculate key cash flow lines
    cash_flow['cfads'] = cash_flow['revenue'] - cash_flow['opex']
    # Calculate debt service for DSCR
    cash_flow['debt_service'] = cash_flow['interest'] + cash_flow['principal']
    cash_flow['debt_service'] = pd.to_numeric(cash_flow['debt_service'], errors='coerce').fillna(0)
    # Handle division by zero for DSCR
    cash_flow['dscr'] = cash_flow.apply(lambda row: row['cfads'] / row['debt_service'] if row['debt_service'] != 0 else None, axis=1)

    # --- TAX CALCULATION ---
    # Calculate Earnings Before Tax (EBT)
    cash_flow['ebit'] = cash_flow['cfads'] - cash_flow['d_and_a'] # Note: This is a simplified EBIT for tax purposes
    cash_flow['ebt'] = cash_flow['ebit'] - cash_flow['interest']
    
    # Calculate tax expense using the new function with accumulated tax losses
    cash_flow = calculate_tax_expense(cash_flow, TAX_RATE)
    
    # Calculate Net Income
    cash_flow['net_income'] = cash_flow['ebt'] - cash_flow['tax_expense']
    
    # --- END TAX CALCULATION ---

    # Initialize terminal_value column
    cash_flow['terminal_value'] = 0.0
    
    cash_flow['equity_cash_flow'] = cash_flow['cfads'] - cash_flow['interest'] - cash_flow['principal'] - cash_flow['equity_capex'] - cash_flow['tax_expense']

    # Equity Injection for Cash Flow
    cash_flow['equity_injection'] = cash_flow['equity_capex']

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

    # --- BALANCE SHEET CALCULATION ---
    # Sort by asset_id and date to ensure correct cumulative sums
    cash_flow = cash_flow.sort_values(by=['asset_id', 'date']).reset_index(drop=True)

    # Calculate cumulative CAPEX and Depreciation for Fixed Assets
    cash_flow['cumulative_capex'] = cash_flow.groupby('asset_id')['capex'].cumsum()
    cash_flow['cumulative_d_and_a'] = cash_flow.groupby('asset_id')['d_and_a'].cumsum()
    cash_flow['fixed_assets'] = cash_flow['cumulative_capex'] - cash_flow['cumulative_d_and_a']

    # Debt is already in debt_schedule as 'outstanding_balance'
    cash_flow['debt'] = cash_flow['ending_balance']

    # Share Capital: Initial equity injection (assuming it's the sum of equity_capex)
    # This assumes all equity_capex is share capital. Adjust if there are other equity sources.
    cash_flow['share_capital'] = cash_flow.groupby('asset_id')['equity_capex'].cumsum()

    # Retained Earnings: Cumulative Net Income
    cash_flow['retained_earnings'] = cash_flow.groupby('asset_id')['net_income'].cumsum()

    # Cash: Cumulative sum of all cash movements
    # Starting cash is assumed to be 0. Adjust if there's a different initial balance.
    cash_flow['cash'] = (
        cash_flow['revenue']
        - cash_flow['opex']
        - cash_flow['capex']
        + cash_flow['drawdowns']
        + cash_flow['equity_injection']
        - cash_flow['interest']
        - cash_flow['principal']
        - cash_flow['tax_expense']
    ).groupby(cash_flow['asset_id']).cumsum()

    # Net Assets and Equity
    cash_flow['total_assets'] = cash_flow['cash'] + cash_flow['fixed_assets']
    cash_flow['total_liabilities'] = cash_flow['debt']
    cash_flow['net_assets'] = cash_flow['total_assets'] - cash_flow['total_liabilities']
    cash_flow['equity'] = cash_flow['share_capital'] + cash_flow['retained_earnings']

    # --- END BALANCE SHEET CALCULATION ---

    # --- DISTRIBUTION CALCULATION ---
    cash_flow['distributions'] = 0.0
    cash_flow['dividends'] = 0.0
    cash_flow['redistributed_capital'] = 0.0

    # Sort again to ensure correct iteration order
    cash_flow = cash_flow.sort_values(by=['asset_id', 'date']).reset_index(drop=True)

    for i, row in cash_flow.iterrows():
        # Distribution conditions:
        # 1. Cash balance > MIN_CASH_BALANCE_FOR_DISTRIBUTION
        # 2. Retained Earnings > 0 (for dividends) or Share Capital > 0 (for capital redistribution)
        # 3. NPAT (Net Income) > 0 for the period

        # Only consider distributions at the end of a quarter
        if row['date'].month in [3, 6, 9, 12]:
            # Calculate distributable amount from cash perspective
            distributable_from_cash = max(0, row['cash'] - MIN_CASH_BALANCE_FOR_DISTRIBUTION)

            # Check core conditions for any distribution
            if (distributable_from_cash > 0 and
                    row['net_income'] > 0):

                # --- Attempt to pay Dividends first (from Retained Earnings) ---
                dividend_amount = 0.0
                if row['retained_earnings'] > 0:
                    # Max dividend is limited by cash, retained earnings, and monthly NPAT
                    dividend_amount = min(distributable_from_cash, row['retained_earnings'], row['net_income'])
                    
                    if dividend_amount > 0:
                        cash_flow.loc[i, 'dividends'] = dividend_amount
                        cash_flow.loc[i, 'cash'] -= dividend_amount
                        cash_flow.loc[i, 'retained_earnings'] -= dividend_amount
                        distributable_from_cash -= dividend_amount # Reduce cash available for further distribution

                # --- Then, attempt to pay Redistributed Capital (if retained earnings are exhausted or insufficient) ---
                redistributed_capital_amount = 0.0
                if distributable_from_cash > 0 and row['share_capital'] > 0:
                    # Max redistributed capital is limited by remaining cash and share capital
                    redistributed_capital_amount = min(distributable_from_cash, row['share_capital'])
                    
                    if redistributed_capital_amount > 0:
                        cash_flow.loc[i, 'redistributed_capital'] = redistributed_capital_amount
                        cash_flow.loc[i, 'cash'] -= redistributed_capital_amount
                        cash_flow.loc[i, 'share_capital'] -= redistributed_capital_amount

                # Total distributions for the period
                cash_flow.loc[i, 'distributions'] = cash_flow.loc[i, 'dividends'] + cash_flow.loc[i, 'redistributed_capital']

    # --- END DISTRIBUTION CALCULATION ---

    return cash_flow