"""
Test debt schedule calculation with a specific debt amount to see what's happening.
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
from src.calculations.debt import (
    prepare_annual_cash_flows_from_operations_start,
    calculate_debt_schedule_from_cfads_by_type
)
from src.config import DSCR_CALCULATION_FREQUENCY

def test_debt_schedule():
    """Test debt schedule with specific amount"""
    
    portfolio_unique_id = "PRIe3oRLfO4uck35xwYFJ"
    asset_id = 1
    
    with database_lifecycle():
        # Load config
        config_data = get_data_from_mongodb('CONFIG_Inputs', {'unique_id': portfolio_unique_id})
        config = config_data[-1]
        assets = config.get('asset_inputs', [])
        
        asset = None
        for a in assets:
            if a.get('id') == asset_id:
                asset = a
                break
        
        cost_assumptions = asset.get('costAssumptions', {})
        target_dscr_contract = cost_assumptions.get('targetDSCRContract', 1.4)
        target_dscr_merchant = cost_assumptions.get('targetDSCRMerchant', 1.8)
        interest_rate = cost_assumptions.get('interestRate', 0.06)
        tenor_years = cost_assumptions.get('tenorYears', 20)
        
        # Load price data
        current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        monthly_price_path = os.path.join(current_dir, 'data', 'raw_inputs', 'merchant_price_monthly.csv')
        yearly_spread_path = os.path.join(current_dir, 'data', 'raw_inputs', 'merchant_yearly_spreads.csv')
        
        from src.core.input_processor import load_price_data
        monthly_prices, yearly_spreads = load_price_data(monthly_price_path, yearly_spread_path)
        
        # Determine model dates
        operations_start = pd.to_datetime(asset['OperatingStartDate'])
        construction_start = pd.to_datetime(asset.get('constructionStartDate', operations_start - relativedelta(months=12)))
        model_start = construction_start - relativedelta(months=1)
        model_end = operations_start + relativedelta(years=asset.get('assetLife', 25))
        
        # Calculate revenue and opex
        output_dir = os.path.join(current_dir, 'output', 'model_results')
        revenue_df = calculate_revenue_timeseries([asset], monthly_prices, yearly_spreads, model_start, model_end, output_dir)
        
        ASSET_COST_ASSUMPTIONS = {asset.get('name', f'Asset_{asset_id}'): cost_assumptions}
        opex_df = calculate_opex_timeseries([asset], ASSET_COST_ASSUMPTIONS, model_start, model_end)
        
        # Prepare period data
        period_data = prepare_annual_cash_flows_from_operations_start(
            asset, revenue_df, opex_df, DSCR_CALCULATION_FREQUENCY
        )
        
        # Calculate debt service capacities and extract period fractions
        debt_service_capacities = []
        period_fractions = []
        for _, row in period_data.iterrows():
            merchant_revenue = row['merchantGreenRevenue'] + row['merchantEnergyRevenue']
            contracted_revenue = row['contractedGreenRevenue'] + row['contractedEnergyRevenue']
            period_opex = row.get('opex', 0)
            
            merchant_debt_service = merchant_revenue / target_dscr_merchant if target_dscr_merchant > 0 and merchant_revenue > 0 else 0
            contracted_debt_service = contracted_revenue / target_dscr_contract if target_dscr_contract > 0 and contracted_revenue > 0 else 0
            total_debt_service = merchant_debt_service + contracted_debt_service - period_opex
            total_debt_service = max(0, total_debt_service)
            
            debt_service_capacities.append(total_debt_service)
            
            # Get period fraction (default to 1.0 if not present)
            period_fraction = row.get('period_fraction', 1.0)
            period_fractions.append(period_fraction)
        
        print(f"Testing debt amounts...")
        print(f"Debt service capacity: avg=${sum(debt_service_capacities)/len(debt_service_capacities):.2f}M, min=${min(debt_service_capacities):.2f}M")
        
        # Test with $155M (what we saw earlier)
        test_debt = 155.0
        print(f"\nTesting with ${test_debt}M debt...")
        
        schedule = calculate_debt_schedule_from_cfads_by_type(
            test_debt, debt_service_capacities, interest_rate, tenor_years, 
            period_frequency='quarterly', period_fractions=period_fractions
        )
        
        print(f"\nSchedule Results:")
        print(f"  Fully repaid: {schedule['metrics']['fully_repaid']}")
        print(f"  Final balance: ${schedule['metrics']['final_balance']:,.3f}M")
        print(f"  DSCR breached: {schedule['metrics'].get('dscr_breached', False)}")
        print(f"  Payoff period: {schedule['metrics'].get('payoff_period')}")
        print(f"  Payoff periods from end: {schedule['metrics'].get('payoff_periods_from_end')}")
        
        # Show first 20 periods
        print(f"\nFirst 20 Periods:")
        print(f"{'Period':<8} {'Debt Balance':<20} {'Interest':<15} {'Principal':<15} {'Debt Service':<15} {'Min Principal Req':<20} {'Remaining Periods':<20}")
        print("-" * 120)
        
        debt_balances = schedule['debt_balance']
        interest_payments = schedule['interest_payments']
        principal_payments = schedule['principal_payments']
        debt_service = schedule['debt_service']
        
        num_periods = len(debt_balances) - 1
        period_rate = interest_rate / 4  # Quarterly
        
        for i in range(min(20, num_periods)):
            bal = debt_balances[i]
            int_pmt = interest_payments[i] if i < len(interest_payments) else 0
            princ = principal_payments[i] if i < len(principal_payments) else 0
            ds = debt_service[i] if i < len(debt_service) else 0
            remaining = num_periods - i - 1
            min_principal_req = bal / remaining if remaining > 0 and bal > 0 else 0
            capacity = debt_service_capacities[i] if i < len(debt_service_capacities) else 0
            max_principal = max(0, capacity - int_pmt)
            
            print(f"{i+1:<8} ${bal:>18,.2f}M ${int_pmt:>13,.2f}M ${princ:>13,.2f}M ${ds:>13,.2f}M "
                  f"${min_principal_req:>18,.2f}M {remaining:<20}")
            
            if i < 5:
                period_fraction = period_fractions[i] if i < len(period_fractions) else 1.0
                print(f"         Capacity: ${capacity:.2f}M, Interest: ${int_pmt:.2f}M (fraction: {period_fraction:.3f}), Max Principal: ${max_principal:.2f}M, Actual Principal: ${princ:.2f}M")
        
        # Find when debt is paid off
        for i, bal in enumerate(debt_balances):
            if bal < 0.001:
                years = i / 4
                print(f"\nDebt fully paid off at period {i} ({years:.2f} years)")
                print(f"Expected tenor: {tenor_years} years")
                print(f"Difference: {tenor_years - years:.2f} years early")
                break

if __name__ == "__main__":
    test_debt_schedule()

