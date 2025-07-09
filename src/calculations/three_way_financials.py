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
    Generates a simple Profit & Loss (P&L) statement.
    Assumes cash_flow_df contains 'revenue', 'opex', 'depreciation', 'interest', 'tax_expense'.
    """
    pnl = cash_flow_df.copy()
    pnl['EBIT'] = pnl['revenue'] - pnl['opex'] - pnl['depreciation']
    pnl['EBT'] = pnl['EBIT'] - pnl['interest']
    pnl['Net_Income'] = pnl['EBT'] - pnl['tax_expense']
    return pnl[['date', 'asset_id', 'revenue', 'opex', 'ebitda', 'depreciation', 'EBIT', 'interest', 'EBT', 'tax_expense', 'Net_Income']]

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

