"""
Debug debt service capacity calculation for Asset 1.
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
from src.calculations.debt import prepare_annual_cash_flows_from_operations_start
from src.config import DSCR_CALCULATION_FREQUENCY

def debug_debt_capacity():
    """Debug debt service capacity calculation"""
    
    portfolio_unique_id = "PRIe3oRLfO4uck35xwYFJ"
    asset_id = 1
    
    print("="*80)
    print("DEBUGGING DEBT SERVICE CAPACITY")
    print("="*80)
    
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
        
        if not asset:
            print(f"ERROR: Asset {asset_id} not found")
            return
        
        cost_assumptions = asset.get('costAssumptions', {})
        target_dscr_contract = cost_assumptions.get('targetDSCRContract', 1.4)
        target_dscr_merchant = cost_assumptions.get('targetDSCRMerchant', 1.8)
        
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
        
        print(f"\nPeriod Data (first 10 periods):")
        print(f"{'Period':<8} {'Merchant Rev':<15} {'Contracted Rev':<15} {'OPEX':<15} {'CFADS':<15} {'Debt Service Cap':<20}")
        print("-" * 100)
        
        for i, (_, row) in enumerate(period_data.head(10).iterrows()):
            merchant_revenue = row['merchantGreenRevenue'] + row['merchantEnergyRevenue']
            contracted_revenue = row['contractedGreenRevenue'] + row['contractedEnergyRevenue']
            period_opex = row.get('opex', 0)
            period_cfads = row['cfads']
            
            merchant_debt_service = merchant_revenue / target_dscr_merchant if target_dscr_merchant > 0 and merchant_revenue > 0 else 0
            contracted_debt_service = contracted_revenue / target_dscr_contract if target_dscr_contract > 0 and contracted_revenue > 0 else 0
            total_debt_service = merchant_debt_service + contracted_debt_service - period_opex
            total_debt_service = max(0, total_debt_service)
            
            print(f"{i:<8} ${merchant_revenue:>13,.2f}M ${contracted_revenue:>13,.2f}M ${period_opex:>13,.2f}M "
                  f"${period_cfads:>13,.2f}M ${total_debt_service:>18,.2f}M")
        
        # Calculate all debt service capacities
        debt_service_capacities = []
        for _, row in period_data.iterrows():
            merchant_revenue = row['merchantGreenRevenue'] + row['merchantEnergyRevenue']
            contracted_revenue = row['contractedGreenRevenue'] + row['contractedEnergyRevenue']
            period_opex = row.get('opex', 0)
            
            merchant_debt_service = merchant_revenue / target_dscr_merchant if target_dscr_merchant > 0 and merchant_revenue > 0 else 0
            contracted_debt_service = contracted_revenue / target_dscr_contract if target_dscr_contract > 0 and contracted_revenue > 0 else 0
            total_debt_service = merchant_debt_service + contracted_debt_service - period_opex
            total_debt_service = max(0, total_debt_service)
            
            debt_service_capacities.append(total_debt_service)
        
        print(f"\nDebt Service Capacity Summary:")
        print(f"  Total periods: {len(debt_service_capacities)}")
        print(f"  Average capacity: ${sum(debt_service_capacities) / len(debt_service_capacities):,.2f}M")
        print(f"  Min capacity: ${min(debt_service_capacities):,.2f}M")
        print(f"  Max capacity: ${max(debt_service_capacities):,.2f}M")
        print(f"  Negative periods: {sum(1 for dsc in debt_service_capacities if dsc <= 0)}")
        
        print(f"\n{'='*80}")
        print("ANALYSIS COMPLETE")
        print(f"{'='*80}")

if __name__ == "__main__":
    debug_debt_capacity()






