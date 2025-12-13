"""
Debug script for all ZEBRE assets debt profile.
Generates a summary table of debt profiles for all assets.
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
    DSCR_CALCULATION_FREQUENCY, DEFAULT_CAPEX_FUNDING_TYPE
)

def analyze_asset_debt(asset, monthly_prices, yearly_spreads, current_dir):
    """Analyze debt profile for a single asset"""
    
    asset_id = asset.get('id')
    asset_name = asset.get('name', f'Asset_{asset_id}')
    
    try:
        # Get cost assumptions
        cost_assumptions = asset.get('costAssumptions', {})
        
        # Determine model dates
        operations_start = pd.to_datetime(asset['OperatingStartDate'])
        construction_start = pd.to_datetime(asset.get('constructionStartDate', operations_start - relativedelta(months=12)))
        model_start = construction_start - relativedelta(months=1)
        model_end = operations_start + relativedelta(years=asset.get('assetLife', 25))
        
        # Prepare cost assumptions
        ASSET_COST_ASSUMPTIONS = {asset_name: cost_assumptions}
        
        # Calculate revenue and opex
        output_dir = os.path.join(current_dir, 'output', 'model_results')
        revenue_df = calculate_revenue_timeseries([asset], monthly_prices, yearly_spreads, model_start, model_end, output_dir)
        opex_df = calculate_opex_timeseries([asset], ASSET_COST_ASSUMPTIONS, model_start, model_end)
        
        # Calculate CAPEX schedule
        capex_df = calculate_capex_timeseries([asset], ASSET_COST_ASSUMPTIONS, model_start, model_end, DEFAULT_CAPEX_FUNDING_TYPE)
        
        # Prepare cash flow data
        cash_flow_df = pd.merge(
            revenue_df[['asset_id', 'date', 'revenue', 'contractedGreenRevenue', 
                        'contractedEnergyRevenue', 'merchantGreenRevenue', 'merchantEnergyRevenue']],
            opex_df[['asset_id', 'date', 'opex']],
            on=['asset_id', 'date'],
            how='inner'
        )
        
        # Size debt for asset
        debt_assumptions = {asset_name: cost_assumptions}
        
        debt_sizing_result = size_debt_for_asset(
            asset, cost_assumptions, 
            revenue_df, opex_df, 
            dscr_calculation_frequency=DSCR_CALCULATION_FREQUENCY
        )
        
        # Generate full monthly debt schedule
        debt_df, _ = calculate_debt_schedule(
            [asset], debt_assumptions, capex_df, cash_flow_df,
            model_start, model_end,
            repayment_frequency=DEFAULT_DEBT_REPAYMENT_FREQUENCY,
            debt_sizing_method=DEFAULT_DEBT_SIZING_METHOD,
            dscr_calculation_frequency=DSCR_CALCULATION_FREQUENCY
        )
        
        if debt_df.empty:
            return None
        
        # Extract key metrics
        optimal_debt = debt_sizing_result.get('optimal_debt', 0)
        gearing = debt_sizing_result.get('gearing', 0)
        tenor_years = debt_sizing_result.get('tenor_years', 0)
        debt_service_start = debt_sizing_result.get('debt_service_start_date')
        annual_schedule = debt_sizing_result.get('annual_schedule')
        
        # Calculate actual payoff period
        years_to_payoff = None
        if debt_service_start and not debt_df.empty:
            debt_service_end = pd.to_datetime(debt_service_start) + relativedelta(years=tenor_years)
            service_period = debt_df[
                (debt_df['date'] >= debt_service_start) & 
                (debt_df['date'] <= debt_service_end)
            ].copy()
            
            if not service_period.empty:
                paid_off_periods = service_period[service_period['ending_balance'] < 0.001]
                if not paid_off_periods.empty:
                    first_paid_off = paid_off_periods.iloc[0]
                    months_since_start = (first_paid_off['date'].year - debt_service_start.year) * 12 + \
                                       (first_paid_off['date'].month - debt_service_start.month)
                    years_to_payoff = months_since_start / 12
        
        # Get min DSCR
        min_dscr = None
        if annual_schedule and 'metrics' in annual_schedule:
            min_dscr = annual_schedule['metrics'].get('min_dscr')
        
        # Get CAPEX
        capex = cost_assumptions.get('capex', 0)
        
        return {
            'asset_id': asset_id,
            'asset_name': asset_name,
            'capex': capex,
            'optimal_debt': optimal_debt,
            'gearing': gearing,
            'tenor_years': tenor_years,
            'years_to_payoff': years_to_payoff,
            'early_by_years': tenor_years - years_to_payoff if years_to_payoff else None,
            'min_dscr': min_dscr,
            'interest_rate': cost_assumptions.get('interestRate', 0),
            'operations_start': operations_start.strftime('%Y-%m-%d') if operations_start else None
        }
        
    except Exception as e:
        print(f"  ERROR processing {asset_name}: {e}")
        return {
            'asset_id': asset_id,
            'asset_name': asset_name,
            'error': str(e)
        }

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
        
        print(f"Found {len(assets)} assets in portfolio")
        print()
        
        # Load price data once for all assets
        current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        monthly_price_path = os.path.join(current_dir, 'data', 'raw_inputs', 'merchant_price_monthly.csv')
        yearly_spread_path = os.path.join(current_dir, 'data', 'raw_inputs', 'merchant_yearly_spreads.csv')
        
        print("Loading price data...")
        monthly_prices, yearly_spreads = load_price_data(monthly_price_path, yearly_spread_path)
        print()
        
        # Process each asset
        results = []
        for i, asset in enumerate(assets, 1):
            asset_id = asset.get('id')
            asset_name = asset.get('name', f'Asset_{asset_id}')
            print(f"Processing Asset {asset_id} ({asset_name})... [{i}/{len(assets)}]")
            
            result = analyze_asset_debt(asset, monthly_prices, yearly_spreads, current_dir)
            if result:
                results.append(result)
            print()
        
        # Generate summary table
        print("="*80)
        print("SUMMARY TABLE - ALL ASSETS")
        print("="*80)
        print()
        
        if not results:
            print("No results to display")
            return
        
        # Create DataFrame for better formatting
        df_data = []
        for r in results:
            if 'error' not in r:
                df_data.append({
                    'Asset ID': r['asset_id'],
                    'Asset Name': r['asset_name'],
                    'CAPEX ($M)': f"${r['capex']:,.2f}",
                    'Debt ($M)': f"${r['optimal_debt']:,.2f}",
                    'Gearing': f"{r['gearing']:.1%}",
                    'Tenor (years)': r['tenor_years'],
                    'Payoff (years)': f"{r['years_to_payoff']:.2f}" if r['years_to_payoff'] else "N/A",
                    'Early by (years)': f"{r['early_by_years']:.2f}" if r['early_by_years'] else "N/A",
                    'Min DSCR': f"{r['min_dscr']:.2f}" if r['min_dscr'] else "N/A",
                    'Interest Rate': f"{r['interest_rate']:.1%}",
                    'Ops Start': r['operations_start']
                })
            else:
                df_data.append({
                    'Asset ID': r['asset_id'],
                    'Asset Name': r['asset_name'],
                    'CAPEX ($M)': "ERROR",
                    'Debt ($M)': "ERROR",
                    'Gearing': "ERROR",
                    'Tenor (years)': "ERROR",
                    'Payoff (years)': "ERROR",
                    'Early by (years)': "ERROR",
                    'Min DSCR': "ERROR",
                    'Interest Rate': "ERROR",
                    'Ops Start': "ERROR"
                })
        
        df = pd.DataFrame(df_data)
        
        # Print formatted table
        print(df.to_string(index=False))
        print()
        
        # Print summary statistics
        valid_results = [r for r in results if 'error' not in r]
        if valid_results:
            print("="*80)
            print("SUMMARY STATISTICS")
            print("="*80)
            print(f"Total Assets Processed: {len(results)}")
            print(f"Successful: {len(valid_results)}")
            print(f"Errors: {len(results) - len(valid_results)}")
            print()
            
            if valid_results:
                total_capex = sum(r['capex'] for r in valid_results)
                total_debt = sum(r['optimal_debt'] for r in valid_results)
                avg_gearing = sum(r['gearing'] for r in valid_results) / len(valid_results)
                avg_early_by = np.mean([r['early_by_years'] for r in valid_results if r['early_by_years'] is not None])
                
                print(f"Portfolio Totals:")
                print(f"  Total CAPEX: ${total_capex:,.2f}M")
                print(f"  Total Debt: ${total_debt:,.2f}M")
                print(f"  Average Gearing: {avg_gearing:.1%}")
                print(f"  Average Early Payoff: {avg_early_by:.2f} years")
                print()
        
        print("="*80)
        print("ANALYSIS COMPLETE")
        print("="*80)

if __name__ == "__main__":
    debug_zebre_all_assets()




