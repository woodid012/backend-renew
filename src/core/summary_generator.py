import pandas as pd

def generate_summary_data(cash_flow_df, fiscal_year_start_month=7):
    """
    Generates Fiscal Year (FY), Calendar Year (CY), and Quarterly (QTR) summary data.

    Args:
        cash_flow_df (pd.DataFrame): The consolidated cash flow DataFrame with a 'date' column.
        fiscal_year_start_month (int): The month number (1-12) that the fiscal year starts.

    Returns:
        dict: A dictionary containing DataFrames for FY, CY, and QTR summaries.
    """
    df = cash_flow_df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)

    # Calendar Year (CY) Summary
    cy_summary = df.resample('YE').sum(numeric_only=True)
    cy_summary.index = cy_summary.index.year
    cy_summary.index.name = 'calendar_year'

    # Quarterly (QTR) Summary
    qtr_summary = df.resample('QE').sum(numeric_only=True)
    qtr_summary.index = qtr_summary.index.to_period('Q')
    qtr_summary.index.name = 'quarter'

    # Fiscal Year (FY) Summary
    # To calculate fiscal year, we need to adjust the year based on the fiscal year start month
    df['fiscal_year'] = df.index.to_period('M').asfreq('Y-%s' % pd.to_datetime(fiscal_year_start_month, format='%m').strftime('%b').upper()).year
    fy_summary = df.groupby('fiscal_year').sum(numeric_only=True)

    return {
        'calendar_year_summary': cy_summary,
        'quarterly_summary': qtr_summary,
        'fiscal_year_summary': fy_summary
    }
