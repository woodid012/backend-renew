# scripts/generate_sensitivity_summary.py

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
from src.config import MONGO_ASSET_OUTPUT_COLLECTION
from src.core.equity_irr import calculate_equity_irr

# Sensitivity collection
SENSITIVITY_COLLECTION = "SENS_Asset_Outputs"

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

def calculate_portfolio_irr_from_data(scenario_data):
    """
    Calculate portfolio IRR from raw scenario data
    """
    if not scenario_data:
        return None
    
    df = pd.DataFrame(scenario_data)
    df['date'] = pd.to_datetime(df['date'])
    
    # Filter for Construction + Operations periods only
    co_df = df[df['period_type'].isin(['C', 'O'])].copy() if 'period_type' in df.columns else df.copy()
    
    if co_df.empty:
        return None
    
    # Group by date and sum equity cash flows across all assets
    portfolio_cf = co_df.groupby('date')['equity_cash_flow'].sum().reset_index()
    
    # Filter non-zero cash flows
    portfolio_cf = portfolio_cf[portfolio_cf['equity_cash_flow'] != 0]
    
    if portfolio_cf.empty:
        return None
    
    # Calculate IRR
    irr = calculate_equity_irr(portfolio_cf)
    return irr if not pd.isna(irr) else None

def get_base_case_irr():
    """
    Get base case IRR from main collection for comparison
    """
    client = None
    try:
        client = get_mongo_client()
        db = client.get_database()
        main_collection = db[MONGO_ASSET_OUTPUT_COLLECTION]
        
        # Look for base case data (no scenario_id or scenario_id is None)
        base_query = {"$or": [{"scenario_id": {"$exists": False}}, {"scenario_id": None}]}
        base_data = list(main_collection.find(base_query))
        
        if not base_data:
            # Try looking for any scenario that might be the base case
            all_scenarios = main_collection.distinct("scenario_id")
            base_candidates = [s for s in all_scenarios if s and ('base' in s.lower() or 'baseline' in s.lower())]
            
            if base_candidates:
                print(f"Found potential base case scenarios: {base_candidates}")
                base_data = list(main_collection.find({"scenario_id": base_candidates[0]}))
            else:
                print("Warning: No base case found in main collection")
                return None
        
        base_irr = calculate_portfolio_irr_from_data(base_data)
        return base_irr
    
    except Exception as e:
        print(f"Error getting base case IRR: {e}")
        return None
    finally:
        if client:
            client.close()

def generate_sensitivity_summary(sensitivity_prefix="sensitivity_results", output_format="both"):
    """
    Generate comprehensive sensitivity analysis summary
    
    Args:
        sensitivity_prefix (str): Prefix for sensitivity scenarios
        output_format (str): "excel", "csv", or "both"
    """
    print(f"=== GENERATING SENSITIVITY ANALYSIS SUMMARY ===")
    print(f"Searching for scenarios with prefix: {sensitivity_prefix}")
    
    client = None
    try:
        client = get_mongo_client()
        db = client.get_database()
        collection = db[SENSITIVITY_COLLECTION]
        
        # Find all sensitivity scenario IDs
        scenario_ids = collection.distinct("scenario_id", {
            "scenario_id": {"$regex": f"^{sensitivity_prefix}"}
        })
        
        if not scenario_ids:
            print(f"No sensitivity scenarios found with prefix: {sensitivity_prefix}")
            return
        
        print(f"Found {len(scenario_ids)} sensitivity scenarios")
        
        # Get base case IRR for comparison
        base_case_irr = get_base_case_irr()
        print(f"Base case IRR: {base_case_irr:.2%}" if base_case_irr else "Base case IRR: Not found")
        
        # Process each scenario
        results = []
        
        for i, scenario_id in enumerate(sorted(scenario_ids)):
            print(f"Processing {i+1}/{len(scenario_ids)}: {scenario_id}")
            
            # Get scenario data
            scenario_data = list(collection.find({"scenario_id": scenario_id}))
            
            if not scenario_data:
                print(f"  No data found for {scenario_id}")
                continue
            
            # Parse scenario parameters
            param_info = parse_scenario_id(scenario_id)
            parameter = param_info.get("parameter", "unknown")
            input_value = param_info.get("value", 0)
            
            # Calculate portfolio IRR
            portfolio_irr = calculate_portfolio_irr_from_data(scenario_data)
            
            # Calculate summary metrics
            df = pd.DataFrame(scenario_data)
            
            # Portfolio totals
            total_capex = df[df['capex'] > 0]['capex'].sum() if 'capex' in df.columns else 0
            total_debt = df[df['debt_capex'] > 0]['debt_capex'].sum() if 'debt_capex' in df.columns else 0
            total_equity = total_capex - total_debt
            portfolio_gearing = total_debt / total_capex if total_capex > 0 else 0
            
            total_revenue = df['revenue'].sum() if 'revenue' in df.columns else 0
            total_opex = df['opex'].sum() if 'opex' in df.columns else 0
            
            # IRR comparison
            irr_difference = None
            irr_difference_bps = None
            if portfolio_irr is not None and base_case_irr is not None:
                irr_difference = portfolio_irr - base_case_irr
                irr_difference_bps = irr_difference * 10000  # Convert to basis points
            
            # Determine parameter type and units
            parameter_type = "Unknown"
            parameter_units = ""
            
            if "multiplier" in parameter or parameter in ["volume", "capex", "opex", "terminal_value"]:
                parameter_type = "Multiplier"
                parameter_units = "x"
            elif "price" in parameter:
                parameter_type = "Price Adjustment"
                parameter_units = "$/MWh"
            elif "interest_rate" in parameter:
                parameter_type = "Interest Rate Adjustment"
                parameter_units = "bps"
            
            # Create result record
            result = {
                'scenario_id': scenario_id,
                'parameter': parameter,
                'parameter_type': parameter_type,
                'input_value': input_value,
                'input_units': parameter_units,
                'portfolio_irr': portfolio_irr,
                'irr_percentage': portfolio_irr * 100 if portfolio_irr else None,
                'base_case_irr': base_case_irr,
                'irr_difference': irr_difference,
                'irr_difference_bps': irr_difference_bps,
                'portfolio_gearing': portfolio_gearing * 100 if portfolio_gearing else 0,
                'total_capex_m': total_capex,
                'total_debt_m': total_debt,
                'total_equity_m': total_equity,
                'total_revenue_m': total_revenue,
                'total_opex_m': total_opex,
                'net_cfads_m': total_revenue - total_opex
            }
            
            results.append(result)
            
            # Quick status
            irr_str = f"{portfolio_irr:.2%}" if portfolio_irr else "N/A"
            diff_str = f"{irr_difference_bps:+.0f}bps" if irr_difference_bps else "N/A"
            print(f"  {parameter} = {input_value} → IRR: {irr_str} ({diff_str})")
        
        if not results:
            print("No valid results to summarize")
            return
        
        # Create summary DataFrame
        summary_df = pd.DataFrame(results)
        
        # Sort by parameter then by input value
        summary_df = summary_df.sort_values(['parameter', 'input_value'])
        
        # Generate outputs
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = os.path.join(project_root, "output", "sensitivity_analysis")
        os.makedirs(output_dir, exist_ok=True)
        
        # Save detailed summary
        if output_format in ["excel", "both"]:
            excel_file = os.path.join(output_dir, f"sensitivity_summary_{timestamp}.xlsx")
            
            with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                # Main summary
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
                
                # Parameter-specific sheets
                for param in summary_df['parameter'].unique():
                    param_df = summary_df[summary_df['parameter'] == param].copy()
                    sheet_name = param.replace('_', ' ').title()[:31]  # Excel sheet name limit
                    param_df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                # IRR sensitivity analysis
                irr_pivot = summary_df.pivot_table(
                    index='parameter', 
                    columns='input_value', 
                    values='irr_percentage', 
                    aggfunc='first'
                ).round(2)
                irr_pivot.to_excel(writer, sheet_name='IRR Matrix')
                
                # Base case comparison
                if base_case_irr:
                    comparison_df = summary_df[['parameter', 'input_value', 'irr_percentage', 'irr_difference_bps']].copy()
                    comparison_df['base_case_irr_pct'] = base_case_irr * 100
                    comparison_df.to_excel(writer, sheet_name='vs Base Case', index=False)
            
            print(f"\n✓ Excel summary saved: {excel_file}")
        
        if output_format in ["csv", "both"]:
            csv_file = os.path.join(output_dir, f"sensitivity_summary_{timestamp}.csv")
            summary_df.to_csv(csv_file, index=False)
            print(f"✓ CSV summary saved: {csv_file}")
        
        # Print key insights
        print(f"\n=== KEY INSIGHTS ===")
        print(f"Total scenarios analyzed: {len(summary_df)}")
        print(f"Parameters tested: {summary_df['parameter'].nunique()}")
        print(f"Parameter list: {', '.join(sorted(summary_df['parameter'].unique()))}")
        
        if base_case_irr and not summary_df['irr_difference_bps'].isna().all():
            best_scenario = summary_df.loc[summary_df['irr_difference_bps'].idxmax()]
            worst_scenario = summary_df.loc[summary_df['irr_difference_bps'].idxmin()]
            
            print(f"\nBest scenario: {best_scenario['parameter']} = {best_scenario['input_value']}")
            print(f"  IRR: {best_scenario['irr_percentage']:.2f}% (+{best_scenario['irr_difference_bps']:.0f}bps vs base)")
            
            print(f"\nWorst scenario: {worst_scenario['parameter']} = {worst_scenario['input_value']}")
            print(f"  IRR: {worst_scenario['irr_percentage']:.2f}% ({worst_scenario['irr_difference_bps']:.0f}bps vs base)")
            
            # Parameter sensitivity ranking
            print(f"\nParameter sensitivity (max IRR impact):")
            param_sensitivity = summary_df.groupby('parameter')['irr_difference_bps'].apply(
                lambda x: max(x.max(), abs(x.min()))
            ).sort_values(ascending=False)
            
            for param, max_impact in param_sensitivity.items():
                print(f"  {param}: ±{max_impact:.0f}bps max impact")
        
        print(f"\n=== SUMMARY COMPLETE ===")
        return summary_df
        
    except Exception as e:
        print(f"Error generating summary: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        if client:
            client.close()

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate sensitivity analysis summary")
    parser.add_argument('--prefix', type=str, default='sensitivity_results',
                       help='Sensitivity scenario prefix to analyze')
    parser.add_argument('--format', type=str, choices=['excel', 'csv', 'both'], default='both',
                       help='Output format (default: both)')
    
    args = parser.parse_args()
    
    summary_df = generate_sensitivity_summary(args.prefix, args.format)