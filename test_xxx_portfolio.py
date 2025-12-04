#!/usr/bin/env python3
"""
Test script to run XXX portfolio through the model with full debugging
"""

import os
import sys
from dotenv import load_dotenv

# Add src directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, 'src')
sys.path.insert(0, src_dir)
sys.path.insert(0, current_dir)

load_dotenv()

from src.main import run_cashflow_model
from src.core.database import database_lifecycle, get_data_from_mongodb
from src.core.input_processor import load_price_data

def main():
    print("="*80)
    print("TESTING XXX PORTFOLIO")
    print("="*80)
    
    portfolio_name = "xxx"
    
    try:
        with database_lifecycle():
            print(f"\n[OK] Database connection established")
            
            # Load assets from MongoDB
            print(f"\nLoading assets from MongoDB for portfolio: {portfolio_name}")
            query = {'PlatformName': portfolio_name}
            config_data = get_data_from_mongodb('CONFIG_Inputs', query=query)
            
            if not config_data:
                print(f"[ERROR] No config data found for portfolio: {portfolio_name}")
                return
            
            print(f"[OK] Found {len(config_data)} config document(s)")
            selected_config = config_data[-1]  # Use most recent
            assets = selected_config.get('asset_inputs', [])
            actual_portfolio_name = selected_config.get('PlatformName', portfolio_name)
            
            print(f"[OK] Loaded {len(assets)} assets")
            print(f"[OK] Portfolio name: {actual_portfolio_name}")
            
            if not assets:
                print(f"[ERROR] No assets found in config data")
                return
            
            # Load price data
            print(f"\nLoading price data...")
            monthly_price_path = os.path.join(current_dir, 'data', 'raw_inputs', 'merchant_price_monthly.csv')
            yearly_spread_path = os.path.join(current_dir, 'data', 'raw_inputs', 'merchant_yearly_spreads.csv')
            
            if not os.path.exists(monthly_price_path):
                print(f"[ERROR] Monthly price file not found: {monthly_price_path}")
                return
                
            if not os.path.exists(yearly_spread_path):
                print(f"[ERROR] Yearly spread file not found: {yearly_spread_path}")
                return
            
            monthly_prices, yearly_spreads = load_price_data(monthly_price_path, yearly_spread_path)
            print(f"[OK] Price data loaded")
            print(f"   - Monthly prices shape: {monthly_prices.shape if monthly_prices is not None else 'None'}")
            print(f"   - Yearly spreads shape: {yearly_spreads.shape if yearly_spreads is not None else 'None'}")
            
            if monthly_prices is None or yearly_spreads is None:
                print(f"[ERROR] Failed to load price data")
                return
            
            # Run the model
            print(f"\nRunning cashflow model...")
            print("="*80)
            
            # Check if portfolio_name parameter exists in function signature
            import inspect
            sig = inspect.signature(run_cashflow_model)
            has_portfolio_param = 'portfolio_name' in sig.parameters
            
            if has_portfolio_param:
                result = run_cashflow_model(
                    assets,
                    monthly_prices,
                    yearly_spreads,
                    actual_portfolio_name,
                    scenario_file=None,
                    scenario_id=None,
                    run_sensitivity=False,
                    replace_data=True
                )
            else:
                # Fallback for old signature
                print("[WARNING] Using old function signature - portfolio_name will not be tagged")
                result = run_cashflow_model(
                    assets,
                    monthly_prices,
                    yearly_spreads,
                    scenario_file=None,
                    scenario_id=None,
                    run_sensitivity=False,
                    replace_data=True
                )
            
            print("="*80)
            print("[SUCCESS] MODEL RUN COMPLETED SUCCESSFULLY")
            print("="*80)
            print(f"Result: {result}")
            
    except Exception as e:
        print("="*80)
        print(f"[ERROR] {type(e).__name__}: {str(e)}")
        print("="*80)
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == '__main__':
    sys.exit(main())

