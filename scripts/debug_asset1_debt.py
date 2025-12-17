"""
Debug script to check Asset 1 debt schedule and payoff timing.
"""

import sys
import os
import pandas as pd
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
    DSCR_CALCULATION_FREQUENCY, DEFAULT_CAPEX_FUNDING_TYPE
)

def debug_asset1_debt():
    """Debug Asset 1 debt schedule"""
    
    portfolio_unique_id = "PRIe3oRLfO4uck35xwYFJ"
    asset_id = 1
    
    print("="*80)
    print("DEBUGGING ASSET 1 DEBT SCHEDULE")
    print("="*80)
    
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
        
        asset_name = asset.get('name', f'Asset_{asset_id}')
        print(f"Found asset: {asset_name}")
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
        
        # Determine model dates
        operations_start = pd.to_datetime(asset['OperatingStartDate'])
        construction_start = pd.to_datetime(asset.get('constructionStartDate', operations_start - relativedelta(months=12)))
        model_start = construction_start - relativedelta(months=1)
        model_end = operations_start + relativedelta(years=asset.get('assetLife', 25))
        
        # Prepare cost assumptions
        ASSET_COST_ASSUMPTIONS = {asset_name: cost_assumptions}
        
        # Calculate revenue and opex
        print(f"\nCalculating revenue and opex...")
        output_dir = os.path.join(current_dir, 'output', 'model_results')
        revenue_df = calculate_revenue_timeseries([asset], monthly_prices, yearly_spreads, model_start, model_end, output_dir)
        opex_df = calculate_opex_timeseries([asset], ASSET_COST_ASSUMPTIONS, model_start, model_end)
        
        # Calculate CAPEX schedule
        print(f"\nCalculating CAPEX schedule...")
        capex_df = calculate_capex_timeseries([asset], ASSET_COST_ASSUMPTIONS, model_start, model_end, DEFAULT_CAPEX_FUNDING_TYPE)
        
        # Prepare cash flow data
        cash_flow_df = pd.merge(
            revenue_df[['asset_id', 'date', 'revenue', 'contractedGreenRevenue', 
                        'contractedEnergyRevenue', 'merchantGreenRevenue', 'merchantEnergyRevenue']],
            opex_df[['asset_id', 'date', 'opex']],
            on=['asset_id', 'date'],
            how='inner'
        )
        
        debt_assumptions = {asset_name: cost_assumptions}
        
        # Size debt
        print(f"\n{'='*80}")
        print("DEBT SIZING")
        print(f"{'='*80}")
        
        debt_sizing_result = size_debt_for_asset(
            asset, cost_assumptions, 
            revenue_df, opex_df, 
            dscr_calculation_frequency=DSCR_CALCULATION_FREQUENCY
        )
        
        optimal_debt = debt_sizing_result.get('optimal_debt', 0)
        tenor_years = debt_sizing_result.get('tenor_years', 0)
        debt_service_start = debt_sizing_result.get('debt_service_start_date')
        
        print(f"\nDebt Sizing Results:")
        print(f"  Optimal Debt: ${optimal_debt:,.2f}M")
        print(f"  Gearing: {debt_sizing_result.get('gearing', 0):.1%}")
        print(f"  Tenor Years: {tenor_years}")
        print(f"  Debt Service Start: {debt_service_start}")
        
        # Check annual schedule
        annual_schedule = debt_sizing_result.get('annual_schedule')
        if annual_schedule:
            print(f"\nAnnual Schedule Analysis:")
            print(f"  Period Frequency: {annual_schedule.get('period_frequency', 'unknown')}")
            
            debt_balances = annual_schedule.get('debt_balance', [])
            principal_payments = annual_schedule.get('principal_payments', [])
            debt_service = annual_schedule.get('debt_service', [])
            
            # Find when debt is fully paid
            payoff_period = None
            for i, bal in enumerate(debt_balances):
                if bal < 0.001:
                    payoff_period = i
                    break
            
            if payoff_period is not None:
                period_freq = annual_schedule.get('period_frequency', 'quarterly')
                if period_freq == 'quarterly':
                    years_to_payoff = payoff_period / 4
                elif period_freq == 'monthly':
                    years_to_payoff = payoff_period / 12
                else:
                    years_to_payoff = payoff_period
                
                print(f"  ⚠️  ISSUE: Debt fully paid off at period {payoff_period} ({years_to_payoff:.2f} years)")
                print(f"  Expected tenor: {tenor_years} years")
                print(f"  Difference: {tenor_years - years_to_payoff:.2f} years early")
                
                # Show first 20 periods
                print(f"\n  First 20 Periods:")
                print(f"{'Period':<8} {'Debt Balance':<20} {'Principal':<15} {'Debt Service':<15} {'Remaining Periods':<20}")
                print("-" * 80)
                for i in range(min(20, len(debt_balances) - 1)):
                    bal = debt_balances[i]
                    princ = principal_payments[i] if i < len(principal_payments) else 0
                    ds = debt_service[i] if i < len(debt_service) else 0
                    remaining = len(debt_balances) - 1 - i
                    if bal > 0.001 or princ > 0.001:
                        print(f"{i+1:<8} ${bal:>18,.2f}M ${princ:>13,.2f}M ${ds:>13,.2f}M {remaining:<20}")
        
        # Generate full debt schedule
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
        
        if debt_service_start and tenor_years:
            debt_service_end = pd.to_datetime(debt_service_start) + relativedelta(years=tenor_years)
            
            print(f"\nDebt Service Period:")
            print(f"  Start: {debt_service_start.strftime('%Y-%m-%d')}")
            print(f"  Expected End: {debt_service_end.strftime('%Y-%m-%d')}")
            
            # Filter to debt service period
            service_period = debt_df[
                (debt_df['date'] >= debt_service_start) & 
                (debt_df['date'] <= debt_service_end) &
                (debt_df['asset_id'] == asset_id)
            ].copy()
            
            if not service_period.empty:
                # Find when debt is fully paid
                paid_off_periods = service_period[service_period['ending_balance'] < 0.001]
                if not paid_off_periods.empty:
                    first_paid_off = paid_off_periods.iloc[0]
                    months_since_start = (first_paid_off['date'].year - debt_service_start.year) * 12 + \
                                       (first_paid_off['date'].month - debt_service_start.month)
                    years_to_payoff = months_since_start / 12
                    
                    print(f"\n  ⚠️  ISSUE FOUND:")
                    print(f"  Debt fully paid off at: {first_paid_off['date'].strftime('%Y-%m-%d')}")
                    print(f"  Months since start: {months_since_start}")
                    print(f"  Years to payoff: {years_to_payoff:.2f}")
                    print(f"  Expected tenor: {tenor_years} years")
                    print(f"  Difference: {tenor_years - years_to_payoff:.2f} years early")
                
                # Show yearly summary
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
    debug_asset1_debt()






