"""
Debug script for ZEBRE Asset 1 debt profile issue.
Investigates why debt is paying off in 10 years instead of 20 year tenor.
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from src.core.database import get_data_from_mongodb, database_lifecycle
from src.core.input_processor import load_price_data
from src.calculations.revenue import calculate_revenue_timeseries
from src.calculations.opex import calculate_opex_timeseries
from src.calculations.construction_capex import calculate_capex_timeseries
from src.calculations.debt import calculate_debt_schedule, size_debt_for_asset
from src.config import (
    DEFAULT_DEBT_REPAYMENT_FREQUENCY, DEFAULT_DEBT_SIZING_METHOD, 
    DSCR_CALCULATION_FREQUENCY
)

def debug_zebre_all_assets():
    """Debug debt profile for all ZEBRE assets"""
    
    portfolio_unique_id = "PRIe3oRLfO4uck35xwYFJ"
    
    print("="*80)
    print("DEBUGGING ZEBRE ALL ASSETS DEBT PROFILE")
    print("="*80)
    print(f"Portfolio unique_id: {portfolio_unique_id}")
    print()
    
    with database_lifecycle():
        # Load config from MongoDB
        print("Loading asset data from MongoDB...")
        config_data = get_data_from_mongodb('CONFIG_Inputs', {'unique_id': portfolio_unique_id})
        
        if not config_data:
            print(f"ERROR: No config found for unique_id: {portfolio_unique_id}")
            return
        
        # Get the most recent config
        config = config_data[-1]
        assets = config.get('asset_inputs', [])
        
        # Find Asset 1
        asset = None
        for a in assets:
            if a.get('id') == asset_id:
                asset = a
                break
        
        if not asset:
            print(f"ERROR: Asset {asset_id} not found in portfolio")
            return
        
        print(f"Found asset: {asset.get('name', f'Asset_{asset_id}')}")
        print(f"Operating Start Date: {asset.get('OperatingStartDate')}")
        
        # Get cost assumptions
        cost_assumptions = asset.get('costAssumptions', {})
        print(f"\nCost Assumptions:")
        print(f"  CAPEX: ${cost_assumptions.get('capex', 0):,.2f}M")
        print(f"  Max Gearing: {cost_assumptions.get('maxGearing', 0):.1%}")
        print(f"  Interest Rate: {cost_assumptions.get('interestRate', 0):.1%}")
        print(f"  Tenor Years: {cost_assumptions.get('tenorYears', 0)}")
        print(f"  Target DSCR Contract: {cost_assumptions.get('targetDSCRContract', 0)}")
        print(f"  Target DSCR Merchant: {cost_assumptions.get('targetDSCRMerchant', 0)}")
        
        # Load price data
        current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        monthly_price_path = os.path.join(current_dir, 'data', 'raw_inputs', 'merchant_price_monthly.csv')
        yearly_spread_path = os.path.join(current_dir, 'data', 'raw_inputs', 'merchant_yearly_spreads.csv')
        
        print(f"\nLoading price data...")
        monthly_prices, yearly_spreads = load_price_data(monthly_price_path, yearly_spread_path)
        
        # Determine model dates first
        operations_start = pd.to_datetime(asset['OperatingStartDate'])
        construction_start = pd.to_datetime(asset.get('constructionStartDate', operations_start - relativedelta(months=12)))
        model_start = construction_start - relativedelta(months=1)
        model_end = operations_start + relativedelta(years=asset.get('assetLife', 25))
        
        # Prepare cost assumptions
        asset_name = asset.get('name', f'Asset_{asset_id}')
        ASSET_COST_ASSUMPTIONS = {asset_name: cost_assumptions}
        
        # Calculate revenue and opex
        print(f"\nCalculating revenue and opex...")
        output_dir = os.path.join(current_dir, 'output', 'model_results')
        revenue_df = calculate_revenue_timeseries([asset], monthly_prices, yearly_spreads, model_start, model_end, output_dir)
        opex_df = calculate_opex_timeseries([asset], ASSET_COST_ASSUMPTIONS, model_start, model_end)
        
        # Calculate CAPEX schedule
        print(f"\nCalculating CAPEX schedule...")
        from src.config import DEFAULT_CAPEX_FUNDING_TYPE
        capex_df = calculate_capex_timeseries([asset], ASSET_COST_ASSUMPTIONS, model_start, model_end, DEFAULT_CAPEX_FUNDING_TYPE)
        
        print(f"\nModel Period:")
        print(f"  Start: {model_start.strftime('%Y-%m-%d')}")
        print(f"  End: {model_end.strftime('%Y-%m-%d')}")
        print(f"  Operations Start: {operations_start.strftime('%Y-%m-%d')}")
        
        # Prepare cash flow data
        cash_flow_df = pd.merge(
            revenue_df[['asset_id', 'date', 'revenue', 'contractedGreenRevenue', 
                        'contractedEnergyRevenue', 'merchantGreenRevenue', 'merchantEnergyRevenue']],
            opex_df[['asset_id', 'date', 'opex']],
            on=['asset_id', 'date'],
            how='inner'
        )
        
        # Size debt for asset
        print(f"\n{'='*80}")
        print("DEBT SIZING ANALYSIS")
        print(f"{'='*80}")
        
        debt_assumptions = {asset_name: cost_assumptions}
        
        debt_sizing_result = size_debt_for_asset(
            asset, cost_assumptions, 
            revenue_df, opex_df, 
            dscr_calculation_frequency=DSCR_CALCULATION_FREQUENCY
        )
        
        print(f"\nDebt Sizing Results:")
        print(f"  Optimal Debt: ${debt_sizing_result.get('optimal_debt', 0):,.2f}M")
        print(f"  Gearing: {debt_sizing_result.get('gearing', 0):.1%}")
        print(f"  Interest Rate: {debt_sizing_result.get('interest_rate', 0):.1%}")
        print(f"  Tenor Years: {debt_sizing_result.get('tenor_years', 0)}")
        print(f"  Debt Service Start: {debt_sizing_result.get('debt_service_start_date')}")
        
        annual_schedule = debt_sizing_result.get('annual_schedule')
        if annual_schedule:
            print(f"\nAnnual Schedule Details:")
            print(f"  Period Frequency: {annual_schedule.get('period_frequency', 'unknown')}")
            print(f"  Number of Periods: {len(annual_schedule.get('interest_payments', []))}")
            
            # Check debt balance progression
            debt_balances = annual_schedule.get('debt_balance', [])
            principal_payments = annual_schedule.get('principal_payments', [])
            interest_payments = annual_schedule.get('interest_payments', [])
            
            print(f"\nDebt Balance Progression (first 25 periods):")
            print(f"{'Period':<8} {'Beginning Balance':<20} {'Principal':<15} {'Interest':<15} {'Ending Balance':<20}")
            print("-" * 80)
            
            for i in range(min(25, len(debt_balances) - 1)):
                beg_bal = debt_balances[i]
                princ = principal_payments[i] if i < len(principal_payments) else 0
                int_pmt = interest_payments[i] if i < len(interest_payments) else 0
                end_bal = debt_balances[i + 1]
                
                if beg_bal > 0.001 or princ > 0.001:  # Only show periods with debt
                    print(f"{i+1:<8} ${beg_bal:>18,.2f}M ${princ:>13,.2f}M ${int_pmt:>13,.2f}M ${end_bal:>18,.2f}M")
            
            # Find when debt is fully paid
            for i, bal in enumerate(debt_balances):
                if bal < 0.001:
                    period_freq = annual_schedule.get('period_frequency', 'annual')
                    if period_freq == 'quarterly':
                        years = i / 4
                    elif period_freq == 'monthly':
                        years = i / 12
                    else:
                        years = i
                    print(f"\n⚠️  Debt fully paid off at period {i} ({years:.2f} years)")
                    break
        
        # Generate full monthly debt schedule
        print(f"\n{'='*80}")
        print("MONTHLY DEBT SCHEDULE ANALYSIS")
        print(f"{'='*80}")
        
        debt_df, _ = calculate_debt_schedule(
            [asset], debt_assumptions, capex_df, cash_flow_df,
            model_start, model_end,
            repayment_frequency=DEFAULT_DEBT_REPAYMENT_FREQUENCY,
            debt_sizing_method=DEFAULT_DEBT_SIZING_METHOD,
            dscr_calculation_frequency=DSCR_CALCULATION_FREQUENCY
        )
        
        if debt_df.empty:
            print("ERROR: No debt schedule generated")
            return
        
        # Filter to debt service period
        debt_service_start = debt_sizing_result.get('debt_service_start_date')
        tenor_years = debt_sizing_result.get('tenor_years', 20)
        
        if debt_service_start:
            debt_service_end = pd.to_datetime(debt_service_start) + relativedelta(years=tenor_years)
            
            print(f"\nDebt Service Period:")
            print(f"  Start: {debt_service_start.strftime('%Y-%m-%d')}")
            print(f"  Expected End (tenor): {debt_service_end.strftime('%Y-%m-%d')}")
            
            service_period = debt_df[
                (debt_df['date'] >= debt_service_start) & 
                (debt_df['date'] <= debt_service_end)
            ].copy()
            
            if not service_period.empty:
                # Find when debt is fully paid
                paid_off_periods = service_period[service_period['ending_balance'] < 0.001]
                if not paid_off_periods.empty:
                    first_paid_off = paid_off_periods.iloc[0]
                    months_since_start = (first_paid_off['date'].year - debt_service_start.year) * 12 + \
                                       (first_paid_off['date'].month - debt_service_start.month)
                    years_to_payoff = months_since_start / 12
                    
                    print(f"\n⚠️  ISSUE FOUND:")
                    print(f"  Debt fully paid off at: {first_paid_off['date'].strftime('%Y-%m-%d')}")
                    print(f"  Months since start: {months_since_start}")
                    print(f"  Years to payoff: {years_to_payoff:.2f}")
                    print(f"  Expected tenor: {tenor_years} years")
                    print(f"  Difference: {tenor_years - years_to_payoff:.2f} years early")
                
                # Show summary by year
                service_period['year'] = service_period['date'].dt.year
                yearly_summary = service_period.groupby('year').agg({
                    'principal': 'sum',
                    'interest': 'sum',
                    'debt_service': 'sum',
                    'ending_balance': 'last'
                }).reset_index()
                
                print(f"\nYearly Debt Service Summary:")
                print(f"{'Year':<8} {'Principal':<15} {'Interest':<15} {'Debt Service':<15} {'Ending Balance':<20}")
                print("-" * 80)
                for _, row in yearly_summary.head(25).iterrows():
                    if row['ending_balance'] > 0.001 or row['principal'] > 0.001:
                        print(f"{int(row['year']):<8} ${row['principal']:>13,.2f}M ${row['interest']:>13,.2f}M "
                              f"${row['debt_service']:>13,.2f}M ${row['ending_balance']:>18,.2f}M")
        
        print(f"\n{'='*80}")
        print("ANALYSIS COMPLETE")
        print(f"{'='*80}")

if __name__ == "__main__":
    debug_zebre_asset1()

