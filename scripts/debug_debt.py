
import pandas as pd
import numpy as np
from datetime import datetime
import sys
import os

# Add src to path

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

# Import via src package to allow relative imports within src to work
from src.calculations.debt import calculate_debt_schedule, size_debt_for_asset

def debug_debt_sculpting():
    # Setup dummy data
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2044, 12, 31)
    dates = pd.date_range(start=start_date, end=end_date, freq='MS')
    
    # Create a dummy asset
    asset = {
        'id': 'test_asset',
        'name': 'Test Asset',
        'OperatingStartDate': '2025-01-01',
        'assetLife': 25
    }
    
    # Create dummy cash flows (seasonal)
    # Revenue: 100 base + 20 seasonality
    # Opex: 20 flat
    n_months = len(dates)
    revenue = []
    opex = []
    
    for d in dates:
        # Simple seasonality
        seasonality = 20 * np.sin(d.month / 12 * 2 * np.pi)
        rev = 100 + seasonality
        op = 20
        
        if d < datetime(2025, 1, 1):
            rev = 0
            op = 0
            
        revenue.append(rev)
        opex.append(op)
        
    cash_flow_df = pd.DataFrame({
        'asset_id': 'test_asset',
        'date': dates,
        'revenue': revenue,
        'opex': opex,
        'contractedGreenRevenue': [r * 0.5 for r in revenue],
        'contractedEnergyRevenue': [r * 0.3 for r in revenue],
        'merchantGreenRevenue': [r * 0.1 for r in revenue],
        'merchantEnergyRevenue': [r * 0.1 for r in revenue]
    })
    
    # Calculate CFADS
    cash_flow_df['cfads'] = cash_flow_df['revenue'] - cash_flow_df['opex']
    
    # Capex schedule
    capex_dates = pd.date_range(start='2024-01-01', end='2024-12-01', freq='MS')
    capex_df = pd.DataFrame({
        'asset_id': 'test_asset',
        'date': capex_dates,
        'capex': [100] * len(capex_dates)  # 1200 total capex
    })
    
    # Debt assumptions
    debt_assumptions = {
        'Test Asset': {
            'capex': 1200,
            'maxGearing': 0.8,
            'interestRate': 0.05,
            'tenorYears': 15,
            'targetDSCRContract': 1.3,
            'targetDSCRMerchant': 1.5
        }
    }
    
    # Run debt calculation
    print("Running debt calculation...")
    debt_df, _ = calculate_debt_schedule(
        [asset], debt_assumptions, capex_df, cash_flow_df, 
        start_date, end_date, 
        repayment_frequency='monthly',
        dscr_calculation_frequency='quarterly',
        debt_sizing_method='dscr'
    )
    
    # Analyze results
    print("\nDebt Schedule Analysis:")
    if debt_df.empty:
        print("No debt schedule generated.")
        return

    # Filter for operating period
    op_start = datetime(2025, 1, 1)
    op_df = debt_df[debt_df['date'] >= op_start].copy()
    
    # Calculate implied DSCR (Quarterly)
    # Group by Quarter
    op_df['quarter'] = op_df['date'].dt.to_period('Q')
    quarterly_df = op_df.groupby('quarter').agg({
        'debt_service': 'sum',
        'principal': 'sum',
        'interest': 'sum',
        'date': 'first'  # Keep a date for plotting/reference
    }).reset_index()
    
    # Get Quarterly CFADS
    # We need to sum CFADS for the same quarters
    cash_flow_df['quarter'] = cash_flow_df['date'].dt.to_period('Q')
    quarterly_cfads = cash_flow_df.groupby('quarter')['cfads'].sum().reset_index()
    
    # Merge
    quarterly_analysis = pd.merge(quarterly_df, quarterly_cfads, on='quarter')
    quarterly_analysis['calc_dscr'] = quarterly_analysis.apply(
        lambda row: row['cfads'] / row['debt_service'] if row['debt_service'] > 0 else float('inf'), 
        axis=1
    )
    
    print(quarterly_analysis[['quarter', 'principal', 'interest', 'debt_service', 'cfads', 'calc_dscr']].head(24))
    
    print("\nSummary Metrics:")
    print(f"Total Principal Paid: {quarterly_analysis['principal'].sum():.2f}")
    print(f"Initial Debt: {debt_df['drawdowns'].sum():.2f}")
    print(f"Min DSCR: {quarterly_analysis['calc_dscr'].min():.4f}")
    print(f"Max DSCR: {quarterly_analysis['calc_dscr'].max():.4f}")
    
    # Check for "cleanliness"
    print("\nDSCR Variability:")
    print(quarterly_analysis['calc_dscr'].describe())


if __name__ == "__main__":
    debug_debt_sculpting()
