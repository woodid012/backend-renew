# scripts/extract_sensitivity_fixed.py

import pandas as pd
import json
import os
import sys
from datetime import datetime

# Add the project root and src directory to the Python path for module imports
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)  # Go up one level to project root
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, project_root)
sys.path.insert(0, src_dir)

from src.core.database import get_mongo_client
from src.config import MONGO_ASSET_OUTPUT_COLLECTION, MONGO_ASSET_INPUTS_SUMMARY_COLLECTION
from src.core.equity_irr import calculate_equity_irr

def extract_sensitivity_summary(sensitivity_prefix="sensitivity_results"):
    """
    Extract key metrics from all sensitivity scenarios in MongoDB
    """
    print(f"=== EXTRACTING SENSITIVITY RESULTS FROM MONGODB ===")
    
    client = None
    results = []
    
    try:
        client = get_mongo_client()
        db = client.get_database()
        collection = db[MONGO_ASSET_OUTPUT_COLLECTION]
        
        # Find all scenario_ids that start with our prefix
        scenario_ids = collection.distinct("scenario_id", {
            "scenario_id": {"$regex": f"^{sensitivity_prefix}"}
        })
        
        print(f"Found {len(scenario_ids)} sensitivity scenarios")
        
        for scenario_id in sorted(scenario_ids):
            print(f"\nProcessing: {scenario_id}")
            
            # Get all data for this scenario
            scenario_data = list(collection.find({"scenario_id": scenario_id}))
            
            if not scenario_data:
                print(f"  No data found for {scenario_id}")
                continue
            
            df = pd.DataFrame(scenario_data)
            df['date'] = pd.to_datetime(df['date'])
            
            # Extract parameter info from scenario_id
            param_info = parse_scenario_id(scenario_id)
            
            # Calculate portfolio metrics
            portfolio_metrics = calculate_portfolio_metrics(df)
            
            # Calculate asset-level metrics
            asset_metrics = calculate_asset_metrics(df)
            
            result = {
                "scenario_id": scenario_id,
                "parameter": param_info.get("parameter"),
                "value": param_info.get("value"),
                **portfolio_metrics,
                "asset_details": asset_metrics
            }
            
            results.append(result)
            
            # Print quick summary
            irr_pct = portfolio_metrics.get('portfolio_irr', 0) * 100 if portfolio_metrics.get('portfolio_irr') else 'N/A'
            gearing_pct = portfolio_metrics.get('portfolio_gearing', 0) * 100 if portfolio_metrics.get('portfolio_gearing') else 'N/A'
            print(f"  Portfolio IRR: {irr_pct}{'%' if irr_pct != 'N/A' else ''}, Gearing: {gearing_pct}{'%' if gearing_pct != 'N/A' else ''}")
        
        if results:
            # Save results
            save_results(results, sensitivity_prefix)
            
        return results
        
    except Exception as e:
        print(f"Error: {e}")
        return []
    
    finally:
        if client:
            client.close()

def parse_scenario_id(scenario_id):
    """
    Parse scenario_id to extract parameter and value
    Example: 'sensitivity_results_electricity_price_-5.0000' -> {'parameter': 'electricity_price', 'value': -5.0}
    """
    parts = scenario_id.split('_')
    
    if len(parts) >= 4 and parts[0] == 'sensitivity' and parts[1] == 'results':
        # Handle multi-word parameters like 'electricity_price'
        param_parts = parts[2:-1]  # Everything except prefix and last value
        value_part = parts[-1]
        
        parameter = '_'.join(param_parts)
        
        try:
            value = float(value_part)
            return {"parameter": parameter, "value": value}
        except ValueError:
            pass
    
    return {"parameter": "unknown", "value": scenario_id}

def calculate_portfolio_metrics(df):
    """
    Calculate key portfolio-level metrics - FIXED VERSION
    """
    # Aggregate by date to get portfolio cash flows
    portfolio_cf = df.groupby('date').agg({
        'revenue': 'sum',
        'opex': 'sum', 
        'capex': 'sum',
        'debt_capex': 'sum',
        'equity_capex': 'sum',
        'equity_cash_flow': 'sum',
        'cfads': 'sum',
        'terminal_value': 'sum'
    }).reset_index()
    
    # Calculate IRR using your existing function
    portfolio_cf_for_irr = portfolio_cf[['date', 'equity_cash_flow']].copy()
    portfolio_irr = calculate_equity_irr(portfolio_cf_for_irr)
    
    # FIXED: Calculate total CAPEX correctly
    # Only sum CAPEX where it's actually occurring (non-zero values during construction)
    # This avoids summing zeros across the entire model period
    construction_capex = df[df['capex'] > 0]['capex'].sum()
    construction_debt = df[df['debt_capex'] > 0]['debt_capex'].sum()
    
    # Calculate gearing based on actual CAPEX spent
    portfolio_gearing = construction_debt / construction_capex if construction_capex > 0 else 0
    
    return {
        'portfolio_irr': portfolio_irr if not pd.isna(portfolio_irr) else None,
        'portfolio_gearing': portfolio_gearing,
        'total_capex': construction_capex,  # FIXED: Only actual CAPEX spent
        'total_debt': construction_debt,    # FIXED: Only actual debt drawn
        'total_equity': construction_capex - construction_debt,  # FIXED: Derived correctly
        'total_revenue': portfolio_cf['revenue'].sum(),
        'total_opex': portfolio_cf['opex'].sum(),
        'total_cfads': portfolio_cf['cfads'].sum(),
        'total_equity_cash_flow': portfolio_cf['equity_cash_flow'].sum(),
        'total_terminal_value': portfolio_cf['terminal_value'].sum()
    }

def calculate_asset_metrics(df):
    """
    Calculate metrics for each asset - FIXED VERSION
    """
    asset_metrics = {}
    
    for asset_id in df['asset_id'].unique():
        asset_df = df[df['asset_id'] == asset_id].copy()
        asset_df = asset_df.sort_values('date')
        
        # Calculate asset IRR
        asset_cf_for_irr = asset_df[['date', 'equity_cash_flow']].copy()
        asset_irr = calculate_equity_irr(asset_cf_for_irr)
        
        # FIXED: Calculate asset CAPEX and gearing correctly
        # Only sum CAPEX during construction periods (where capex > 0)
        asset_capex = asset_df[asset_df['capex'] > 0]['capex'].sum()
        asset_debt = asset_df[asset_df['debt_capex'] > 0]['debt_capex'].sum()
        asset_gearing = asset_debt / asset_capex if asset_capex > 0 else 0
        
        asset_metrics[str(asset_id)] = {
            'irr': asset_irr if not pd.isna(asset_irr) else None,
            'gearing': asset_gearing,
            'total_capex': asset_capex,  # FIXED: Only construction CAPEX
            'total_debt': asset_debt,    # FIXED: Only construction debt
            'total_revenue': asset_df['revenue'].sum(),
            'total_cfads': asset_df['cfads'].sum()
        }
    
    return asset_metrics

def save_results(results, sensitivity_prefix):
    """
    Save results to JSON and CSV files
    """
    # Get output directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    output_dir = os.path.join(project_root, "output", "sensitivity_analysis")
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save detailed JSON
    json_file = os.path.join(output_dir, f"{sensitivity_prefix}_detailed_{timestamp}.json")
    with open(json_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n✓ Saved detailed results: {json_file}")
    
    # Create and save CSV summary
    csv_data = []
    for result in results:
        row = {
            'scenario_id': result['scenario_id'],
            'parameter': result['parameter'],
            'value': result['value'],
            'portfolio_irr': result['portfolio_irr'],
            'portfolio_gearing': result['portfolio_gearing'],
            'total_capex': result['total_capex'],
            'total_debt': result['total_debt'],
            'total_revenue': result['total_revenue'],
            'total_cfads': result['total_cfads'],
            'total_equity_cash_flow': result['total_equity_cash_flow']
        }
        
        # Add asset metrics
        for asset_id, metrics in result['asset_details'].items():
            row[f'asset_{asset_id}_irr'] = metrics['irr']
            row[f'asset_{asset_id}_gearing'] = metrics['gearing']
            row[f'asset_{asset_id}_capex'] = metrics['total_capex']
        
        csv_data.append(row)
    
    df_summary = pd.DataFrame(csv_data)
    csv_file = os.path.join(output_dir, f"{sensitivity_prefix}_summary_{timestamp}.csv")
    df_summary.to_csv(csv_file, index=False)
    print(f"✓ Saved CSV summary: {csv_file}")
    
    # Print summary statistics
    print(f"\n=== SENSITIVITY ANALYSIS SUMMARY ===")
    print(f"Total scenarios: {len(results)}")
    
    # Group by parameter
    param_groups = {}
    for result in results:
        param = result['parameter']
        if param not in param_groups:
            param_groups[param] = []
        param_groups[param].append(result)
    
    for param, param_results in param_groups.items():
        print(f"\nParameter: {param}")
        irrs = [r['portfolio_irr'] for r in param_results if r['portfolio_irr'] is not None]
        gearings = [r['portfolio_gearing'] for r in param_results if r['portfolio_gearing'] is not None]
        capexs = [r['total_capex'] for r in param_results if r['total_capex'] is not None]
        
        if irrs:
            print(f"  IRR range: {min(irrs):.2%} to {max(irrs):.2%}")
        if gearings:
            print(f"  Gearing range: {min(gearings):.1%} to {max(gearings):.1%}")
        if capexs:
            print(f"  CAPEX range: ${min(capexs):,.0f}M to ${max(capexs):,.0f}M")
        print(f"  Scenarios: {len(param_results)}")
    
    return json_file, csv_file

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description="Extract sensitivity analysis results from MongoDB")
    parser.add_argument('--prefix', type=str, default='sensitivity_results',
                       help='Scenario ID prefix to search for (default: sensitivity_results)')
    
    args = parser.parse_args()
    
    results = extract_sensitivity_summary(args.prefix)
    
    if results:
        print(f"\n✓ Successfully processed {len(results)} sensitivity scenarios")
        
        # Quick validation check
        if results:
            base_result = next((r for r in results if 'base' in r['scenario_id']), None)
            if base_result:
                print(f"\n=== VALIDATION CHECK ===")
                print(f"Base case CAPEX: ${base_result['total_capex']:,.0f}M")
                print(f"Expected CAPEX: ~$2,597M")
                if 2400 <= base_result['total_capex'] <= 2700:
                    print("✓ CAPEX looks correct!")
                else:
                    print("⚠ CAPEX still looks incorrect")
    else:
        print("\n✗ No sensitivity results found")