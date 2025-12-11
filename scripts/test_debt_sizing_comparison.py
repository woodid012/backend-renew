"""
Test script to compare old binary search vs new CFADS-by-type debt sizing methods.
Tests with ZEBRE Portfolio - Asset 1
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Fix encoding for Windows console
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from src.core.database import get_data_from_mongodb, database_lifecycle
from src.core.input_processor import load_price_data
from src.calculations.revenue import calculate_revenue_timeseries
from src.calculations.opex import calculate_opex_timeseries
from src.calculations.construction_capex import calculate_capex_timeseries
from src.calculations.debt import (
    calculate_debt_schedule, 
    size_debt_for_asset_binary_search,
    size_debt_for_asset_cfads_by_type
)
from src.calculations.cashflow import aggregate_cashflows
from src.calculations.depreciation import calculate_d_and_a
from src.core.equity_irr import calculate_equity_irr
from src.config import (
    DEFAULT_DEBT_REPAYMENT_FREQUENCY, DEFAULT_DEBT_SIZING_METHOD, 
    DSCR_CALCULATION_FREQUENCY, DEFAULT_CAPEX_FUNDING_TYPE
)

def test_debt_sizing_comparison():
    """Compare old vs new debt sizing methods for all assets 1-8"""
    
    portfolio_unique_id = "PRIe3oRLfO4uck35xwYFJ"
    asset_ids = list(range(1, 9))  # Assets 1-8
    
    print("="*80)
    print("DEBT SIZING METHOD COMPARISON TEST")
    print("="*80)
    print(f"Portfolio unique_id: {portfolio_unique_id}")
    print(f"Asset IDs: {asset_ids}")
    print()
    
    # Store results for all assets
    all_results = []
    
    with database_lifecycle():
        # Load config from MongoDB
        print("Loading asset data from MongoDB...")
        config_data = get_data_from_mongodb('CONFIG_Inputs', {'unique_id': portfolio_unique_id})
        
        if not config_data:
            print(f"ERROR: No config found for unique_id: {portfolio_unique_id}")
            return
        
        # Get the most recent config
        config = config_data[-1]
        all_assets = config.get('asset_inputs', [])
        
        # Load price data once
        current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        monthly_price_path = os.path.join(current_dir, 'data', 'raw_inputs', 'merchant_price_monthly.csv')
        yearly_spread_path = os.path.join(current_dir, 'data', 'raw_inputs', 'merchant_yearly_spreads.csv')
        
        print(f"\nLoading price data...")
        monthly_prices, yearly_spreads = load_price_data(monthly_price_path, yearly_spread_path)
        
        # Process each asset
        for asset_id in asset_ids:
            print(f"\n{'='*80}")
            print(f"PROCESSING ASSET {asset_id}")
            print(f"{'='*80}")
            
            # Find asset
            asset = None
            for a in all_assets:
                if a.get('id') == asset_id:
                    asset = a
                    break
            
            if not asset:
                print(f"ERROR: Asset {asset_id} not found in portfolio")
                all_results.append({
                    'asset_id': asset_id,
                    'asset_name': f'Asset_{asset_id}',
                    'error': 'Asset not found'
                })
                continue
        
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
            
            # ===== TEST OLD METHOD (BINARY SEARCH) =====
            print(f"\n{'='*80}")
            print("TESTING OLD METHOD: Binary Search")
            print(f"{'='*80}")
            
            old_debt_sizing_result = size_debt_for_asset_binary_search(
                asset, cost_assumptions, 
                revenue_df, opex_df, 
                dscr_calculation_frequency=DSCR_CALCULATION_FREQUENCY
            )
            
            old_optimal_debt = old_debt_sizing_result.get('optimal_debt', 0)
            old_gearing = old_debt_sizing_result.get('gearing', 0)
            
            print(f"\nOld Method Results:")
            print(f"  Optimal Debt: ${old_optimal_debt:,.2f}M")
            print(f"  Gearing: {old_gearing:.1%}")
            
            # Temporarily replace size_debt_for_asset to use old method
            from src.calculations import debt as debt_module
            original_size_debt = debt_module.size_debt_for_asset
            debt_module.size_debt_for_asset = size_debt_for_asset_binary_search
            
            try:
                old_debt_df, old_capex_df = calculate_debt_schedule(
                    [asset], debt_assumptions, capex_df, cash_flow_df,
                    model_start, model_end,
                    repayment_frequency=DEFAULT_DEBT_REPAYMENT_FREQUENCY,
                    debt_sizing_method='dscr',
                    dscr_calculation_frequency=DSCR_CALCULATION_FREQUENCY
                )
            finally:
                debt_module.size_debt_for_asset = original_size_debt
            
            # Calculate D&A
            d_and_a_df = calculate_d_and_a(
                old_capex_df, 
                pd.DataFrame(columns=['asset_id', 'date', 'intangible_capex']),
                [asset],
                25, 25,  # Default asset life
                model_start, model_end
            )
            
            # Aggregate old cash flows
            old_final_cashflow = aggregate_cashflows(
                revenue_df, opex_df, old_capex_df, old_debt_df, d_and_a_df,
                model_end, [asset], ASSET_COST_ASSUMPTIONS
            )
            
            # Calculate old IRR
            old_equity_cf_df = old_final_cashflow[old_final_cashflow['asset_id'] == asset_id][['date', 'equity_cash_flow']].copy()
            old_equity_cf_df = old_equity_cf_df[old_equity_cf_df['equity_cash_flow'] != 0]
            old_irr = calculate_equity_irr(old_equity_cf_df) if not old_equity_cf_df.empty else None
            
            print(f"  Equity IRR: {old_irr:.2%}" if old_irr and not pd.isna(old_irr) else "  Equity IRR: N/A")
            
            # ===== TEST NEW METHOD (CFADS BY TYPE) =====
            print(f"\n{'='*80}")
            print("TESTING NEW METHOD: CFADS by Type")
            print(f"{'='*80}")
            
            new_debt_sizing_result = size_debt_for_asset_cfads_by_type(
                asset, cost_assumptions, 
                revenue_df, opex_df, 
                dscr_calculation_frequency=DSCR_CALCULATION_FREQUENCY
            )
            
            new_optimal_debt = new_debt_sizing_result.get('optimal_debt', 0)
            new_gearing = new_debt_sizing_result.get('gearing', 0)
            
            print(f"\nNew Method Results:")
            print(f"  Optimal Debt: ${new_optimal_debt:,.2f}M")
            print(f"  Gearing: {new_gearing:.1%}")
            
            # Temporarily replace size_debt_for_asset to use new method
            debt_module.size_debt_for_asset = size_debt_for_asset_cfads_by_type
            
            try:
                new_debt_df, new_capex_df = calculate_debt_schedule(
                    [asset], debt_assumptions, capex_df, cash_flow_df,
                    model_start, model_end,
                    repayment_frequency=DEFAULT_DEBT_REPAYMENT_FREQUENCY,
                    debt_sizing_method='dscr',
                    dscr_calculation_frequency=DSCR_CALCULATION_FREQUENCY
                )
            finally:
                debt_module.size_debt_for_asset = original_size_debt
            
            # Calculate D&A for new method
            d_and_a_df_new = calculate_d_and_a(
                new_capex_df, 
                pd.DataFrame(columns=['asset_id', 'date', 'intangible_capex']),
                [asset],
                25, 25,
                model_start, model_end
            )
            
            # Aggregate new cash flows
            new_final_cashflow = aggregate_cashflows(
                revenue_df, opex_df, new_capex_df, new_debt_df, d_and_a_df_new,
                model_end, [asset], ASSET_COST_ASSUMPTIONS
            )
            
            # Calculate new IRR
            new_equity_cf_df = new_final_cashflow[new_final_cashflow['asset_id'] == asset_id][['date', 'equity_cash_flow']].copy()
            new_equity_cf_df = new_equity_cf_df[new_equity_cf_df['equity_cash_flow'] != 0]
            new_irr = calculate_equity_irr(new_equity_cf_df) if not new_equity_cf_df.empty else None
            
            print(f"  Equity IRR: {new_irr:.2%}" if new_irr and not pd.isna(new_irr) else "  Equity IRR: N/A")
            
            # Calculate differences
            debt_diff = new_optimal_debt - old_optimal_debt
            debt_pct_diff = (debt_diff / old_optimal_debt * 100) if old_optimal_debt > 0 else 0
            gearing_diff = new_gearing - old_gearing
            
            # Store results
            all_results.append({
                'asset_id': asset_id,
                'asset_name': asset_name,
                'old_debt': old_optimal_debt,
                'new_debt': new_optimal_debt,
                'debt_diff': debt_diff,
                'debt_pct_diff': debt_pct_diff,
                'old_gearing': old_gearing,
                'new_gearing': new_gearing,
                'gearing_diff': gearing_diff,
                'old_irr': old_irr if old_irr and not pd.isna(old_irr) else None,
                'new_irr': new_irr if new_irr and not pd.isna(new_irr) else None,
                'irr_diff': (new_irr - old_irr) if (old_irr and not pd.isna(old_irr) and new_irr and not pd.isna(new_irr)) else None,
                'capex': cost_assumptions.get('capex', 0)
            })
        
        # Print summary table
        print(f"\n{'='*80}")
        print("COMPARISON SUMMARY TABLE - ALL ASSETS")
        print(f"{'='*80}")
        
        print(f"\n{'Asset':<20} {'Old Debt':<15} {'New Debt':<15} {'Diff':<15} {'% Diff':<10} {'Old Gearing':<12} {'New Gearing':<12} {'Old IRR':<10} {'New IRR':<10} {'IRR Diff':<10}")
        print("-" * 140)
        
        for result in all_results:
            if 'error' in result:
                print(f"{result['asset_name']:<20} {'ERROR':<15}")
                continue
            
            old_debt_str = f"${result['old_debt']:,.0f}M" if result['old_debt'] else "N/A"
            new_debt_str = f"${result['new_debt']:,.0f}M" if result['new_debt'] else "N/A"
            diff_str = f"${result['debt_diff']:,.0f}M" if result['debt_diff'] else "N/A"
            pct_diff_str = f"{result['debt_pct_diff']:+.1f}%" if result['debt_pct_diff'] else "N/A"
            old_gear_str = f"{result['old_gearing']:.1%}" if result['old_gearing'] else "N/A"
            new_gear_str = f"{result['new_gearing']:.1%}" if result['new_gearing'] else "N/A"
            old_irr_str = f"{result['old_irr']:.2%}" if result['old_irr'] is not None else "N/A"
            new_irr_str = f"{result['new_irr']:.2%}" if result['new_irr'] is not None else "N/A"
            irr_diff_str = f"{result['irr_diff']:+.2%}" if result['irr_diff'] is not None else "N/A"
            
            print(f"{result['asset_name']:<20} {old_debt_str:<15} {new_debt_str:<15} {diff_str:<15} {pct_diff_str:<10} "
                  f"{old_gear_str:<12} {new_gear_str:<12} {old_irr_str:<10} {new_irr_str:<10} {irr_diff_str:<10}")
        
        # Summary statistics
        valid_results = [r for r in all_results if 'error' not in r]
        if valid_results:
            print(f"\n{'='*80}")
            print("AGGREGATE STATISTICS")
            print(f"{'='*80}")
            
            total_old_debt = sum(r['old_debt'] for r in valid_results)
            total_new_debt = sum(r['new_debt'] for r in valid_results)
            total_capex = sum(r['capex'] for r in valid_results)
            
            print(f"\nTotal Portfolio:")
            print(f"  Total CAPEX: ${total_capex:,.2f}M")
            print(f"  Total Old Debt: ${total_old_debt:,.2f}M ({total_old_debt/total_capex:.1%} gearing)")
            print(f"  Total New Debt: ${total_new_debt:,.2f}M ({total_new_debt/total_capex:.1%} gearing)")
            print(f"  Total Difference: ${total_new_debt - total_old_debt:,.2f}M ({(total_new_debt - total_old_debt)/total_old_debt*100:+.1f}%)")
            
            # Average IRR change
            irr_diffs = [r['irr_diff'] for r in valid_results if r['irr_diff'] is not None]
            if irr_diffs:
                avg_irr_diff = sum(irr_diffs) / len(irr_diffs)
                print(f"  Average IRR Change: {avg_irr_diff:+.2%}")
        
        print(f"\n{'='*80}")
        print("TEST COMPLETE")
        print(f"{'='*80}")

if __name__ == "__main__":
    test_debt_sizing_comparison()

