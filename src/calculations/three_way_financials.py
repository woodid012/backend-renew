# src/calculations/three_way_financials.py

import pandas as pd

def aggregate_timeseries(df, freq='QTR'):
    """
    Aggregates a timeseries DataFrame to the specified frequency.
    """
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')

    if freq == 'M':
        return df.groupby('asset_id').resample('M').sum().reset_index()
    elif freq == 'QTR':
        return df.groupby('asset_id').resample('Q').sum().reset_index()
    elif freq == 'CY':
        return df.groupby('asset_id').resample('A').sum().reset_index()
    elif freq == 'FY':
        return df.groupby('asset_id').resample('A-JUN').sum().reset_index()
    else:
        return df.reset_index()

def generate_pnl(cash_flow_df):
    """
    Generates a Profit & Loss (P&L) statement.
    """
    pnl = cash_flow_df.copy()
    
    # Revenue and Operating Expenses
    pnl['gross_profit'] = pnl['revenue'] - pnl['opex']
    
    # EBITDA (should be same as CFADS for this model)
    pnl['ebitda'] = pnl['revenue'] - pnl['opex']
    
    # EBIT (Earnings Before Interest and Tax)
    pnl['ebit'] = pnl['ebitda'] - pnl['d_and_a']
    
    # EBT (Earnings Before Tax)
    pnl['ebt'] = pnl['ebit'] - pnl['interest']
    
    # Net Income
    pnl['net_income_calc'] = pnl['ebt'] - pnl['tax_expense']
    
    return pnl[['date', 'asset_id', 'revenue', 'opex', 'gross_profit', 'ebitda', 'd_and_a', 'ebit', 'interest', 'ebt', 'tax_expense', 'net_income']]

def generate_cash_flow_statement(cash_flow_df):
    """
    Generates a Cash Flow Statement using the pre-calculated components.
    """
    cf_statement = cash_flow_df.copy()
    
    # Operating Activities
    cf_statement['cash_from_operations'] = cf_statement['operating_cash_flow']
    
    # Investing Activities  
    cf_statement['cash_from_investing'] = cf_statement['investing_cash_flow']
    
    # Financing Activities
    cf_statement['cash_from_financing'] = cf_statement['financing_cash_flow']
    
    # Net Cash Flow
    cf_statement['net_cash_flow_calc'] = cf_statement['net_cash_flow']
    
    # Detailed breakdown for financing activities
    cf_statement['debt_drawdowns'] = cf_statement['drawdowns']
    cf_statement['debt_repayments'] = -(cf_statement['interest'] + cf_statement['principal'])
    cf_statement['equity_contributions'] = cf_statement['equity_injection']
    cf_statement['distributions_paid'] = -cf_statement['distributions']
    
    return cf_statement[[
        'date', 'asset_id', 
        # Operating
        'net_income', 'd_and_a', 'cash_from_operations',
        # Investing  
        'capex', 'terminal_value', 'cash_from_investing',
        # Financing
        'debt_drawdowns', 'debt_repayments', 'equity_contributions', 'distributions_paid', 'cash_from_financing',
        # Net
        'net_cash_flow'
    ]]

def generate_balance_sheet(cash_flow_df):
    """
    Generates a Balance Sheet from the cash flow data.
    """
    bs = cash_flow_df.copy()
    
    # Assets
    bs['current_assets'] = bs['cash']  # Assuming cash is the only current asset
    bs['non_current_assets'] = bs['fixed_assets']
    bs['total_assets_calc'] = bs['current_assets'] + bs['non_current_assets']
    
    # Liabilities  
    bs['current_liabilities'] = 0.0  # No current liabilities in this model
    bs['non_current_liabilities'] = bs['debt']
    bs['total_liabilities_calc'] = bs['current_liabilities'] + bs['non_current_liabilities']
    
    # Equity
    bs['contributed_capital'] = bs['share_capital']
    bs['accumulated_earnings'] = bs['retained_earnings']
    bs['total_equity_calc'] = bs['contributed_capital'] + bs['accumulated_earnings']
    
    # Check: Total Assets = Total Liabilities + Equity
    bs['balance_check'] = bs['total_assets_calc'] - (bs['total_liabilities_calc'] + bs['total_equity_calc'])
    
    return bs[[
        'date', 'asset_id',
        # Assets
        'cash', 'fixed_assets', 'total_assets',
        # Liabilities
        'debt', 'total_liabilities', 
        # Equity
        'share_capital', 'retained_earnings', 'equity',
        # Check
        'balance_check'
    ]]