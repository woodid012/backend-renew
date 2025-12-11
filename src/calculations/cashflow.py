# src/calculations/cashflow.py

import pandas as pd
from dateutil.relativedelta import relativedelta
from ..config import ENABLE_TERMINAL_VALUE, TAX_RATE, MIN_CASH_BALANCE_FOR_DISTRIBUTION
from .tax import calculate_tax_expense


def aggregate_cashflows(revenue, opex, capex, debt_schedule, d_and_a_df, end_date, assets_data, asset_cost_assumptions, repayment_frequency='annual', tax_rate=None, enable_terminal_value=None, min_cash_balance_for_distribution=None):
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
    # Use provided values or fall back to config defaults
    if tax_rate is None:
        tax_rate = TAX_RATE
    if enable_terminal_value is None:
        enable_terminal_value = ENABLE_TERMINAL_VALUE
    if min_cash_balance_for_distribution is None:
        min_cash_balance_for_distribution = MIN_CASH_BALANCE_FOR_DISTRIBUTION
    
    # Merge the financial components
    cash_flow = pd.merge(revenue, opex, on=['asset_id', 'date'], how='left')
    cash_flow = pd.merge(cash_flow, capex, on=['asset_id', 'date'], how='left')
    cash_flow = pd.merge(cash_flow, debt_schedule, on=['asset_id', 'date'], how='left')
    cash_flow = pd.merge(cash_flow, d_and_a_df, on=['asset_id', 'date'], how='left')
    cash_flow['d_and_a'] = pd.to_numeric(cash_flow['d_and_a'], errors='coerce').fillna(0)

    # Fill NaNs for assets that might not have all components
    cash_flow.fillna(0, inplace=True)

    # SAFEGUARD: Ensure revenue is zero before OperatingStartDate for each asset
    # This is a final check to prevent any revenue before operations start
    for asset_info in assets_data:
        asset_id = asset_info.get('id')
        if 'OperatingStartDate' not in asset_info or not asset_info['OperatingStartDate']:
            continue
        
        asset_start_date = pd.to_datetime(asset_info['OperatingStartDate'])
        
        # Zero out revenue-related fields before OperatingStartDate
        mask = (cash_flow['asset_id'] == asset_id) & (cash_flow['date'] < asset_start_date)
        
        if mask.any():
            revenue_fields = ['revenue', 'contractedGreenRevenue', 'contractedEnergyRevenue', 
                            'merchantGreenRevenue', 'merchantEnergyRevenue', 'monthlyGeneration']
            
            for field in revenue_fields:
                if field in cash_flow.columns:
                    cash_flow.loc[mask, field] = 0

    # Calculate key cash flow lines
    cash_flow['cfads'] = cash_flow['revenue'] - cash_flow['opex']
    cash_flow['ebitda'] = cash_flow['cfads']
    # Calculate debt service for DSCR
    cash_flow['debt_service'] = cash_flow['interest'] + cash_flow['principal']
    cash_flow['debt_service'] = pd.to_numeric(cash_flow['debt_service'], errors='coerce').fillna(0)
    # Handle division by zero for DSCR
    cash_flow['dscr'] = cash_flow.apply(lambda row: row['cfads'] / row['debt_service'] if row['debt_service'] != 0 else None, axis=1)

    # --- TAX CALCULATION ---
    # Calculate Earnings Before Tax (EBT)
    cash_flow['ebit'] = cash_flow['ebitda'] - cash_flow['d_and_a'] # Note: This is a simplified EBIT for tax purposes
    cash_flow['ebt'] = cash_flow['ebit'] - cash_flow['interest']
    
    # Calculate tax expense using the new function with accumulated tax losses
    cash_flow = calculate_tax_expense(cash_flow, tax_rate)
    
    # Calculate Net Income
    cash_flow['net_income'] = cash_flow['ebt'] - cash_flow['tax_expense']
    
    # --- END TAX CALCULATION ---

    # Initialize terminal_value column
    cash_flow['terminal_value'] = 0.0
    
    # Calculate Equity Cash Flow BEFORE distributions
    # Formula: CFADS - interest - principal - equity_capex - tax_expense
    # Note: equity_capex is subtracted (negative cash flow for equity investors)
    # This represents cash available to equity holders before distributions
    cash_flow['equity_cash_flow_pre_distributions'] = (
        cash_flow['cfads'] - 
        cash_flow['interest'] - 
        cash_flow['principal'] - 
        cash_flow['equity_capex'] - 
        cash_flow['tax_expense']
    )

    # Equity Injection for Cash Flow
    cash_flow['equity_injection'] = cash_flow['equity_capex']

    # Calculate Terminal Value
    if enable_terminal_value:
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
                        # Add terminal value to pre-distributions equity cash flow
                        cash_flow.loc[
                            (cash_flow['asset_id'] == asset_id) & (cash_flow['date'] == terminal_value_date),
                            'equity_cash_flow_pre_distributions'
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

    # Retained Earnings: Cumulative Net Income minus cumulative distributions
    cash_flow['retained_earnings'] = 0.0  # Initialize, will be calculated after distributions

    # --- DISTRIBUTION CALCULATION (FIXED) ---
    cash_flow['distributions'] = 0.0
    cash_flow['dividends'] = 0.0
    cash_flow['redistributed_capital'] = 0.0

    # Initialize cash balance calculation
    cash_flow['cash'] = 0.0

    # Process each asset separately to maintain proper cash balance tracking
    for asset_id in cash_flow['asset_id'].unique():
        asset_mask = cash_flow['asset_id'] == asset_id
        asset_indices = cash_flow[asset_mask].index.tolist()
        
        running_cash_balance = 0.0
        running_retained_earnings = 0.0
        running_share_capital = 0.0
        
        for idx in asset_indices:
            row = cash_flow.loc[idx]
            
            # Update running balances from this period's activities
            # Cash increases from: revenue, debt drawdowns, equity injections
            # Cash decreases from: opex, capex, interest, principal, taxes
            period_cash_change = (
                row['revenue'] - 
                row['opex'] - 
                row['capex'] + 
                row['drawdowns'] + 
                row['equity_injection'] - 
                row['interest'] - 
                row['principal'] - 
                row['tax_expense']
            )
            
            running_cash_balance += period_cash_change
            running_retained_earnings += row['net_income']  # Add net income before distributions
            running_share_capital += row['equity_injection']  # Track share capital
            
            # Distribution logic - only on quarter ends (March, June, September, December)
            distributable_amount = 0.0
            if row['date'].month in [3, 6, 9, 12]:
                # Available cash for distribution (keep minimum balance)
                available_cash = max(0, running_cash_balance - min_cash_balance_for_distribution)
                
                # Only distribute if we have positive equity cash flow for the quarter
                # Calculate quarterly equity cash flow by looking at the last 3 months
                quarter_start_idx = max(0, asset_indices.index(idx) - 2)  # Look back 3 months (including current)
                quarter_indices = asset_indices[quarter_start_idx:asset_indices.index(idx) + 1]
                quarterly_equity_cf = sum(cash_flow.loc[i, 'equity_cash_flow_pre_distributions'] for i in quarter_indices)
                
                if quarterly_equity_cf > 0 and available_cash > 0:
                    # Distribute the minimum of available cash and quarterly equity cash flow
                    distributable_amount = min(available_cash, quarterly_equity_cf)
                    
                    # Apply distribution hierarchy: dividends first (from retained earnings), then capital return
                    dividend_amount = 0.0
                    capital_return_amount = 0.0
                    
                    if running_retained_earnings > 0:
                        dividend_amount = min(distributable_amount, running_retained_earnings)
                        running_retained_earnings -= dividend_amount
                        running_cash_balance -= dividend_amount
                        
                        # Remaining amount can be capital return
                        remaining_distributable = distributable_amount - dividend_amount
                        if remaining_distributable > 0 and running_share_capital > 0:
                            capital_return_amount = min(remaining_distributable, running_share_capital)
                            running_share_capital -= capital_return_amount
                            running_cash_balance -= capital_return_amount
                    else:
                        # No retained earnings, distribute as capital return if available
                        if running_share_capital > 0:
                            capital_return_amount = min(distributable_amount, running_share_capital)
                            running_share_capital -= capital_return_amount
                            running_cash_balance -= capital_return_amount
                    
                    # Record distributions
                    cash_flow.loc[idx, 'dividends'] = dividend_amount
                    cash_flow.loc[idx, 'redistributed_capital'] = capital_return_amount
                    cash_flow.loc[idx, 'distributions'] = dividend_amount + capital_return_amount
            
            # Update balance sheet items
            cash_flow.loc[idx, 'cash'] = running_cash_balance
            cash_flow.loc[idx, 'retained_earnings'] = running_retained_earnings
            cash_flow.loc[idx, 'share_capital'] = running_share_capital

    # Calculate final equity cash flow (after distributions)
    cash_flow['equity_cash_flow'] = cash_flow['equity_cash_flow_pre_distributions'] - cash_flow['distributions']

    # --- COMPLETE BALANCE SHEET ---
    # Net Assets and Equity
    cash_flow['total_assets'] = cash_flow['cash'] + cash_flow['fixed_assets']
    cash_flow['total_liabilities'] = cash_flow['debt']
    cash_flow['net_assets'] = cash_flow['total_assets'] - cash_flow['total_liabilities']
    cash_flow['equity'] = cash_flow['share_capital'] + cash_flow['retained_earnings']

    # --- CUMULATIVE DISTRIBUTIONS ---
    cash_flow['total_dividends'] = cash_flow.groupby('asset_id')['dividends'].cumsum()
    cash_flow['total_redistributed_capital'] = cash_flow.groupby('asset_id')['redistributed_capital'].cumsum()
    cash_flow['total_distributions'] = cash_flow.groupby('asset_id')['distributions'].cumsum()

    # --- CASH FLOW STATEMENT COMPONENTS ---
    # Operating Cash Flow = CFADS - Tax
    cash_flow['operating_cash_flow'] = cash_flow['cfads'] - cash_flow['tax_expense']
    
    # Investing Cash Flow = -CAPEX + Terminal Value (when received)
    cash_flow['investing_cash_flow'] = -cash_flow['capex'] + cash_flow['terminal_value']
    
    # Financing Cash Flow = Debt Drawdowns - Interest - Principal + Equity Injection - Distributions
    cash_flow['financing_cash_flow'] = (
        cash_flow['drawdowns'] - 
        cash_flow['interest'] - 
        cash_flow['principal'] + 
        cash_flow['equity_injection'] - 
        cash_flow['distributions']
    )
    
    # Net Cash Flow = Sum of all three
    cash_flow['net_cash_flow'] = (
        cash_flow['operating_cash_flow'] + 
        cash_flow['investing_cash_flow'] + 
        cash_flow['financing_cash_flow']
    )

    return cash_flow