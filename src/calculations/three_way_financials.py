import pandas as pd

def generate_pnl(cash_flow_df):
    """
    Generates a simple Profit & Loss (P&L) statement.
    Assumes cash_flow_df contains 'revenue', 'opex', 'depreciation', 'interest', 'tax_expense'.
    """
    pnl = cash_flow_df.copy()
    pnl['EBIT'] = pnl['revenue'] - pnl['opex'] - pnl['depreciation']
    pnl['EBT'] = pnl['EBIT'] - pnl['interest']
    pnl['Net_Income'] = pnl['EBT'] - pnl['tax_expense']
    return pnl[['date', 'asset_id', 'revenue', 'opex', 'depreciation', 'EBIT', 'interest', 'EBT', 'tax_expense', 'Net_Income']]

def generate_cash_flow_statement(pnl_df, cash_flow_df):
    """
    Generates a simple Cash Flow Statement.
    Assumes pnl_df contains 'Net_Income' and cash_flow_df contains 'depreciation', 'capex', 'principal', 'equity_cash_flow'.
    """
    cf_statement = pnl_df.copy()
    cf_statement['Net_Income'] = pnl_df['Net_Income']
    cf_statement['Depreciation_NonCash'] = cash_flow_df['depreciation'] # Add back non-cash depreciation
    cf_statement['Cash_from_Operations'] = cf_statement['Net_Income'] + cf_statement['Depreciation_NonCash']
    
    cf_statement['Cash_from_Investing'] = -cash_flow_df['capex'] # CAPEX is an outflow
    
    cf_statement['Cash_from_Financing'] = cash_flow_df['principal'] + cash_flow_df['equity_cash_flow'] # Principal repayment is outflow, equity is inflow/outflow
    
    cf_statement['Net_Cash_Flow'] = cf_statement['Cash_from_Operations'] + cf_statement['Cash_from_Investing'] + cf_statement['Cash_from_Financing']
    
    return cf_statement[['date', 'asset_id', 'Net_Income', 'Depreciation_NonCash', 'Cash_from_Operations', 'Cash_from_Investing', 'Cash_from_Financing', 'Net_Cash_Flow']]

def generate_balance_sheet(pnl_df, cf_statement_df, cash_flow_df, initial_cash=0, initial_ppe=0, initial_debt=0, initial_equity=0):
    """
    Generates a simple Balance Sheet.
    This is an iterative process, building balances period by period.
    Assumes pnl_df contains 'Net_Income', cf_statement_df contains 'Net_Cash_Flow', and cash_flow_df contains 'capex', 'depreciation', 'principal'.
    """
    balance_sheet_records = []
    
    # Sort by date and asset_id to ensure correct cumulative calculations
    sorted_cash_flow_df = cash_flow_df.sort_values(by=['date', 'asset_id']).reset_index(drop=True)
    sorted_pnl_df = pnl_df.sort_values(by=['date', 'asset_id']).reset_index(drop=True)
    sorted_cf_statement_df = cf_statement_df.sort_values(by=['date', 'asset_id']).reset_index(drop=True)

    # Initialize balances for the first period
    current_cash = initial_cash
    current_ppe_net = initial_ppe
    current_debt = initial_debt
    current_retained_earnings = initial_equity # Assuming initial equity is retained earnings

    # Group by date to process period by period (assuming monthly data)
    for date, group in sorted_cash_flow_df.groupby('date'):
        # Aggregate for the current period across all assets (simplification for portfolio-level BS)
        period_net_income = sorted_pnl_df[sorted_pnl_df['date'] == date]['Net_Income'].sum()
        period_net_cash_flow = sorted_cf_statement_df[sorted_cf_statement_df['date'] == date]['Net_Cash_Flow'].sum()
        period_capex = group['capex'].sum()
        period_depreciation = group['depreciation'].sum()
        period_principal_repayment = group['principal'].sum()
        period_debt_incurred = group['debt_draw'].sum() if 'debt_draw' in group.columns else 0 # Assuming debt_draw exists
        period_equity_injection = group['equity_draw'].sum() if 'equity_draw' in group.columns else 0 # Assuming equity_draw exists

        # Update balances
        current_cash += period_net_cash_flow
        current_ppe_net += period_capex - period_depreciation
        current_debt += period_debt_incurred - period_principal_repayment
        current_retained_earnings += period_net_income - period_equity_injection # Assuming equity_injection is like a dividend for simplicity

        balance_sheet_records.append({
            'date': date,
            'Cash': current_cash,
            'PPE_Net': current_ppe_net,
            'Total_Assets': current_cash + current_ppe_net,
            'Debt': current_debt,
            'Retained_Earnings': current_retained_earnings,
            'Total_Liabilities_and_Equity': current_debt + current_retained_earnings
        })

    balance_sheet_df = pd.DataFrame(balance_sheet_records)
    return balance_sheet_df
