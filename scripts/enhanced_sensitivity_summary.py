# scripts/generate_enhanced_sensitivity_summary.py

import pandas as pd
import json
import os
import sys
from datetime import datetime
import numpy as np

# Add the project root and src directory to the Python path for module imports
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, project_root)
sys.path.insert(0, src_dir)

from src.core.database import database_lifecycle, mongo_session
from src.core.equity_irr import calculate_equity_irr

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

def get_parameter_info(parameter):
   """Get parameter metadata for better reporting"""
   param_metadata = {
       'volume': {
           'name': 'Volume Multiplier',
           'units': 'x',
           'description': 'Capacity factor / generation volume adjustment',
           'type': 'multiplier'
       },
       'capex': {
           'name': 'CAPEX Multiplier', 
           'units': 'x',
           'description': 'Capital expenditure cost adjustment',
           'type': 'multiplier'
       },
       'opex': {
           'name': 'OPEX Multiplier',
           'units': 'x', 
           'description': 'Operating expenditure cost adjustment',
           'type': 'multiplier'
       },
       'electricity_price': {
           'name': 'Electricity Price Adjustment',
           'units': '$/MWh',
           'description': 'Adjustment to electricity market prices',
           'type': 'price_adjustment'
       },
       'green_price': {
           'name': 'Green Certificate Price Adjustment', 
           'units': '$/MWh',
           'description': 'Adjustment to green certificate prices',
           'type': 'price_adjustment'
       },
       'interest_rate': {
           'name': 'Interest Rate Adjustment',
           'units': 'bps',
           'description': 'Debt interest rate adjustment in basis points', 
           'type': 'rate_adjustment'
       },
       'terminal_value': {
           'name': 'Terminal Value Multiplier',
           'units': 'x',
           'description': 'End-of-life asset valuation adjustment',
           'type': 'multiplier'
       }
   }
   
   return param_metadata.get(parameter, {
       'name': parameter.replace('_', ' ').title(),
       'units': '',
       'description': f'Sensitivity parameter: {parameter}',
       'type': 'unknown'
   })

def calculate_portfolio_irr_from_data(scenario_data):
   """Calculate portfolio IRR from raw scenario data"""
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

def calculate_asset_irr_from_data(scenario_data, asset_id):
   """Calculate IRR for a specific asset"""
   if not scenario_data:
       return None
   
   df = pd.DataFrame(scenario_data)
   df['date'] = pd.to_datetime(df['date'])
   
   # Filter for this asset and Construction + Operations periods
   asset_df = df[df['asset_id'] == asset_id].copy()
   co_df = asset_df[asset_df['period_type'].isin(['C', 'O'])].copy() if 'period_type' in asset_df.columns else asset_df.copy()
   
   if co_df.empty:
       return None
   
   # Get equity cash flows for this asset
   asset_cf = co_df[['date', 'equity_cash_flow']].copy()
   asset_cf = asset_cf[asset_cf['equity_cash_flow'] != 0]
   
   if asset_cf.empty:
       return None
   
   # Calculate IRR
   irr = calculate_equity_irr(asset_cf)
   return irr if not pd.isna(irr) else None

def get_base_case_metrics():
   """Get base case metrics from main collection for comparison"""
   try:
       with mongo_session() as db_mgr:
           main_collection = db_mgr.get_collection("ASSET_cash_flows")
           
           # Look for base case data (no scenario_id or scenario_id is None)
           base_query = {"$or": [{"scenario_id": {"$exists": False}}, {"scenario_id": None}]}
           base_data = list(main_collection.find(base_query))
           
           if not base_data:
               print("Warning: No base case found in main collection")
               return None, {}, 0, 0, 0, {}
           
           df = pd.DataFrame(base_data)
           unique_assets = df['asset_id'].unique() if 'asset_id' in df.columns else []

           # Calculate portfolio IRR
           portfolio_irr = calculate_portfolio_irr_from_data(base_data)
           
           # Calculate portfolio metrics
           total_capex = df[df['capex'] > 0]['capex'].sum() if 'capex' in df.columns else 0
           total_debt = df[df['debt_capex'] > 0]['debt_capex'].sum() if 'debt_capex' in df.columns else 0
           portfolio_gearing = total_debt / total_capex if total_capex > 0 else 0
           total_revenue = df['revenue'].sum() if 'revenue' in df.columns else 0

           # Calculate asset IRRs and metrics
           asset_irrs = {}
           asset_metrics = {}
           
           for asset_id in unique_assets:
               asset_irr = calculate_asset_irr_from_data(base_data, asset_id)
               if asset_irr is not None:
                   asset_irrs[asset_id] = asset_irr
               
               asset_df = df[df['asset_id'] == asset_id]
               asset_capex = asset_df[asset_df['capex'] > 0]['capex'].sum()
               asset_debt = asset_df[asset_df['debt_capex'] > 0]['debt_capex'].sum()
               asset_gearing = asset_debt / asset_capex if asset_capex > 0 else 0
               asset_revenue = asset_df['revenue'].sum()
               
               asset_metrics[asset_id] = {
                   'capex': asset_capex,
                   'debt': asset_debt,
                   'gearing': asset_gearing,
                   'revenue': asset_revenue
               }
           
           return portfolio_irr, asset_irrs, total_capex, total_revenue, portfolio_gearing, asset_metrics
           
   except Exception as e:
       print(f"Error getting base case metrics: {e}")
       return None, {}, 0, 0, 0, {}

def generate_enhanced_sensitivity_summary(sensitivity_prefix="sensitivity_results", output_format="excel"):
   """
   Generate comprehensive sensitivity analysis summary with parametric arrays and IRR differences
   """
   print("=== GENERATING ENHANCED SENSITIVITY SUMMARY ===")
   print(f"Searching for scenarios with prefix: {sensitivity_prefix}")
   
   try:
       with mongo_session() as db_mgr:
           collection = db_mgr.get_collection("SENS_Asset_Outputs")
           
           # Find all sensitivity scenario IDs
           scenario_ids = collection.distinct("scenario_id", {
               "scenario_id": {"$regex": f"^{sensitivity_prefix}"}
           })
           
           if not scenario_ids:
               print(f"No sensitivity scenarios found with prefix: {sensitivity_prefix}")
               return
           
           print(f"Found {len(scenario_ids)} sensitivity scenarios")
           
           # Get base case metrics for comparison
           base_case_portfolio_irr, base_case_asset_irrs, base_case_total_capex, base_case_total_revenue, base_case_portfolio_gearing, base_case_asset_metrics = get_base_case_metrics()
           print(f"Base case portfolio IRR: {base_case_portfolio_irr:.2%}" if base_case_portfolio_irr else "Base case portfolio IRR: Not found")
           print(f"Base case asset IRRs keys: {base_case_asset_irrs.keys()}")

           # Collect all unique asset IDs across all scenarios (including base case)
           all_asset_ids = set(base_case_asset_irrs.keys())
           for scenario_id in scenario_ids:
               scenario_data = list(collection.find({"scenario_id": scenario_id}))
               if scenario_data:
                   df_temp = pd.DataFrame(scenario_data)
                   if 'asset_id' in df_temp.columns:
                       all_asset_ids.update(df_temp['asset_id'].unique())
           all_asset_ids = sorted(list(all_asset_ids))

           # Add base case to results
           base_case_result = {
               'scenario_id': 'base_case',
               'parameter': 'base_case',
               'parameter_name': 'Base Case',
               'parameter_units': '',
               'parameter_description': 'Reference base case scenario',
               'input_value': 0,
               'portfolio_irr': base_case_portfolio_irr,
               'portfolio_irr_pct': base_case_portfolio_irr * 100 if base_case_portfolio_irr else None,
               'base_case_portfolio_irr': base_case_portfolio_irr,
               'portfolio_irr_diff': 0,
               'portfolio_irr_diff_bps': 0,
               'portfolio_gearing_pct': base_case_portfolio_gearing * 100,
               'total_capex_m': base_case_total_capex,
               'total_debt_m': sum(am.get('debt', 0) for am in base_case_asset_metrics.values()),
               'total_revenue_m': base_case_total_revenue,
               'asset_irrs': base_case_asset_irrs,
               'asset_metrics': base_case_asset_metrics
           }
           results = [base_case_result]
           
           # Process each scenario
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
               
               # Get parameter metadata
               param_meta = get_parameter_info(parameter)
               
               # Calculate portfolio IRR
               portfolio_irr = calculate_portfolio_irr_from_data(scenario_data)
               
               # Calculate portfolio metrics
               df = pd.DataFrame(scenario_data)
               total_capex = df[df['capex'] > 0]['capex'].sum() if 'capex' in df.columns else 0
               total_debt = df[df['debt_capex'] > 0]['debt_capex'].sum() if 'debt_capex' in df.columns else 0
               portfolio_gearing = total_debt / total_capex if total_capex > 0 else 0
               total_revenue = df['revenue'].sum() if 'revenue' in df.columns else 0
               
               # Calculate asset-level IRRs
               unique_assets = df['asset_id'].unique() if 'asset_id' in df.columns else []
               all_asset_ids = sorted(list(set(all_asset_ids) | set(unique_assets)))
               asset_irrs = {}
               asset_metrics = {}
               
               for asset_id in unique_assets:
                   # Calculate asset IRR
                   asset_irr = calculate_asset_irr_from_data(scenario_data, asset_id)
                   if asset_irr is not None:
                       asset_irrs[asset_id] = asset_irr
                   
                   # Calculate asset metrics
                   asset_df = df[df['asset_id'] == asset_id]
                   asset_capex = asset_df[asset_df['capex'] > 0]['capex'].sum()
                   asset_debt = asset_df[asset_df['debt_capex'] > 0]['debt_capex'].sum()
                   asset_gearing = asset_debt / asset_capex if asset_capex > 0 else 0
                   asset_revenue = asset_df['revenue'].sum()
                   
                   asset_metrics[asset_id] = {
                       'capex': asset_capex,
                       'debt': asset_debt,
                       'gearing': asset_gearing,
                       'revenue': asset_revenue
                   }
               
               # Calculate differences from base case
               portfolio_irr_diff = None
               portfolio_irr_diff_bps = None
               if portfolio_irr is not None and base_case_portfolio_irr is not None:
                   portfolio_irr_diff = portfolio_irr - base_case_portfolio_irr
                   portfolio_irr_diff_bps = portfolio_irr_diff * 10000
               
               # Create result record
               result = {
                   'scenario_id': scenario_id,
                   'parameter': parameter,
                   'parameter_name': param_meta['name'],
                   'parameter_units': param_meta['units'],
                   'parameter_description': param_meta['description'],
                   'input_value': input_value,
                   'portfolio_irr': portfolio_irr,
                   'portfolio_irr_pct': portfolio_irr * 100 if portfolio_irr else None,
                   'base_case_portfolio_irr': base_case_portfolio_irr,
                   'portfolio_irr_diff': portfolio_irr_diff,
                   'portfolio_irr_diff_bps': portfolio_irr_diff_bps,
                   'portfolio_gearing_pct': portfolio_gearing * 100,
                   'total_capex_m': total_capex,
                   'total_debt_m': total_debt,
                   'total_revenue_m': total_revenue,
                   'asset_irrs': asset_irrs,
                   'asset_metrics': asset_metrics
               }
               
               results.append(result)
               
               # Quick status
               irr_str = f"{portfolio_irr:.2%}" if portfolio_irr else "N/A"
               diff_str = f"{portfolio_irr_diff_bps:+.0f}bps" if portfolio_irr_diff_bps else "N/A"
               print(f"  {param_meta['name']} = {input_value} {param_meta['units']} → Portfolio IRR: {irr_str} ({diff_str})")
           
           if not results:
               print("No valid results to summarize")
               return
           
           # Create output directory
           timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
           output_dir = os.path.join(project_root, "output", "sensitivity_analysis")
           os.makedirs(output_dir, exist_ok=True)
           
           # Generate comprehensive Excel output
           if output_format in ["excel", "both"]:
               excel_file = os.path.join(output_dir, f"enhanced_sensitivity_summary_{timestamp}.xlsx")
               
               with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                   # 1. MAIN SUMMARY SHEET
                   summary_data = []
                   # Define all possible asset columns in the correct order (interleaved)
                   all_asset_cols = []
                   for asset_id in sorted(all_asset_ids):
                       all_asset_cols.append(f'asset_{asset_id}_irr_pct')
                       all_asset_cols.append(f'asset_{asset_id}_irr_diff_bps')

                   # Combine all expected columns for consistent DataFrame creation
                   base_cols = ['scenario_id', 'parameter', 'parameter_name', 'parameter_units', 'input_value',
                                'portfolio_irr_pct', 'portfolio_irr_diff_bps', 'portfolio_gearing_pct',
                                'total_capex_m', 'total_revenue_m']
                   all_expected_cols = base_cols + all_asset_cols

                   for result in results:
                       # Initialize row with all expected columns set to None
                       row = {col: None for col in all_expected_cols}

                       # Populate common fields
                       row['scenario_id'] = result['scenario_id']
                       row['parameter'] = result['parameter']
                       row['parameter_name'] = result['parameter_name']
                       row['parameter_units'] = result['parameter_units']
                       row['input_value'] = result['input_value']
                       row['portfolio_irr_pct'] = result['portfolio_irr_pct']
                       row['portfolio_irr_diff_bps'] = result['portfolio_irr_diff_bps']
                       row['portfolio_gearing_pct'] = result['portfolio_gearing_pct']
                       row['total_capex_m'] = result['total_capex_m']
                       row['total_revenue_m'] = result['total_revenue_m']
                       
                       # Populate asset-specific fields
                       for asset_id in sorted(all_asset_ids):
                           asset_irr = result['asset_irrs'].get(asset_id)
                           asset_irr_pct = asset_irr * 100 if asset_irr else None
                           row[f'asset_{asset_id}_irr_pct'] = asset_irr_pct
                           
                           # Calculate IRR difference for all scenarios
                           base_asset_irr = base_case_asset_irrs.get(asset_id)
                           asset_irr_diff_bps = None
                           if result['scenario_id'] == 'base_case':
                               asset_irr_diff_bps = 0
                           elif asset_irr is not None and base_asset_irr is not None:
                               asset_irr_diff_bps = (asset_irr - base_asset_irr) * 10000
                           elif asset_irr is None and base_asset_irr is not None:
                               # Handle case where scenario has no IRR but base case does
                               asset_irr_diff_bps = None
                           elif asset_irr is not None and base_asset_irr is None:
                               # Handle case where scenario has IRR but base case doesn't
                               asset_irr_diff_bps = None
                           
                           row[f'asset_{asset_id}_irr_diff_bps'] = asset_irr_diff_bps
                       
                       summary_data.append(row)
                   
                   main_summary_df = pd.DataFrame(summary_data)
                   # Ensure all expected asset IRR and diff columns are present, filling missing with NaN
                   main_summary_df = main_summary_df.reindex(columns=all_expected_cols)
                   main_summary_df = main_summary_df.sort_values(['parameter', 'input_value'])
                   main_summary_df.to_excel(writer, sheet_name='Main Summary', index=False)
                   
                   # 2. PARAMETRIC ARRAY SHEET - Portfolio IRR Matrix
                   parameters = main_summary_df['parameter'].unique()
                   
                   # Create pivot table for Portfolio IRR
                   if len(parameters) > 0:
                       portfolio_irr_pivot = main_summary_df.pivot_table(
                           index='parameter',
                           columns='input_value', 
                           values='portfolio_irr_pct',
                           aggfunc='first'
                       ).round(2)
                       portfolio_irr_pivot.to_excel(writer, sheet_name='Portfolio IRR Matrix')
                       
                       # Portfolio IRR Difference Matrix (vs base case)
                       portfolio_irr_diff_pivot = main_summary_df.pivot_table(
                           index='parameter',
                           columns='input_value',
                           values='portfolio_irr_diff_bps',
                           aggfunc='first'
                       ).round(0)
                       portfolio_irr_diff_pivot.to_excel(writer, sheet_name='Portfolio IRR Diff (bps)')
                   
                   # 3. ASSET-LEVEL IRR MATRICES
                   for asset_id in sorted(all_asset_ids):
                       asset_irr_col = f'asset_{asset_id}_irr_pct'
                       asset_diff_col = f'asset_{asset_id}_irr_diff_bps'
                       
                       if asset_irr_col in main_summary_df.columns:
                           # Asset IRR Matrix
                           asset_irr_pivot = main_summary_df.pivot_table(
                               index='parameter',
                               columns='input_value',
                               values=asset_irr_col,
                               aggfunc='first'
                           ).round(2)
                           sheet_name = f'Asset {asset_id} IRR'[:31]  # Excel sheet name limit
                           asset_irr_pivot.to_excel(writer, sheet_name=sheet_name)
                           
                           # Asset IRR Difference Matrix
                           if asset_diff_col in main_summary_df.columns:
                               asset_diff_pivot = main_summary_df.pivot_table(
                                   index='parameter',
                                   columns='input_value',
                                   values=asset_diff_col,
                                   aggfunc='first'
                               ).round(0)
                               sheet_name = f'Asset {asset_id} IRR Diff'[:31]
                               asset_diff_pivot.to_excel(writer, sheet_name=sheet_name)
                   
                   # 4. PARAMETER IMPACT RANKING
                   impact_data = []
                   for param in parameters:
                       param_results = [r for r in results if r['parameter'] == param]
                       if param_results:
                           param_meta = get_parameter_info(param)
                           
                           # Portfolio impact
                           portfolio_diffs = [r['portfolio_irr_diff_bps'] for r in param_results if r['portfolio_irr_diff_bps'] is not None]
                           if portfolio_diffs:
                               portfolio_max_impact = max(abs(max(portfolio_diffs)), abs(min(portfolio_diffs)))
                               portfolio_range = max(portfolio_diffs) - min(portfolio_diffs)
                           else:
                               portfolio_max_impact = 0
                               portfolio_range = 0
                           
                           # Asset impacts
                           asset_impacts = {}
                           for asset_id in sorted(all_asset_ids):
                               asset_diffs = []
                               for r in param_results:
                                   asset_irr = r['asset_irrs'].get(asset_id)
                                   base_asset_irr = base_case_asset_irrs.get(asset_id)
                                   if asset_irr and base_asset_irr:
                                       diff_bps = (asset_irr - base_asset_irr) * 10000
                                       asset_diffs.append(diff_bps)
                               
                               if asset_diffs:
                                   asset_max_impact = max(abs(max(asset_diffs)), abs(min(asset_diffs)))
                                   asset_impacts[asset_id] = asset_max_impact
                               else:
                                   asset_impacts[asset_id] = 0
                           
                           impact_row = {
                               'parameter': param,
                               'parameter_name': param_meta['name'],
                               'portfolio_max_impact_bps': portfolio_max_impact,
                               'portfolio_range_bps': portfolio_range,
                           }
                           
                           for asset_id in sorted(all_asset_ids):
                               impact_row[f'asset_{asset_id}_max_impact_bps'] = asset_impacts.get(asset_id, 0)
                           
                           impact_data.append(impact_row)
                   
                   impact_df = pd.DataFrame(impact_data)
                   impact_df = impact_df.sort_values('portfolio_max_impact_bps', ascending=False)
                   impact_df.to_excel(writer, sheet_name='Parameter Impact Ranking', index=False)
                   
                   # 5. BASE CASE COMPARISON
                   if base_case_portfolio_irr or base_case_asset_irrs:
                       base_case_data = {
                           'metric': ['Portfolio IRR (%)'] + [f'Asset {aid} IRR (%)' for aid in sorted(all_asset_ids)],
                           'base_case_value': [base_case_portfolio_irr * 100 if base_case_portfolio_irr else None] + 
                                            [base_case_asset_irrs.get(aid, None) * 100 if base_case_asset_irrs.get(aid) else None 
                                             for aid in sorted(all_asset_ids)]
                       }
                       base_case_df = pd.DataFrame(base_case_data)
                       base_case_df.to_excel(writer, sheet_name='Base Case Reference', index=False)
               
               print(f"\n✓ Enhanced Excel summary saved: {excel_file}")
           
           # Generate CSV if requested
           if output_format in ["csv", "both"]:
               csv_file = os.path.join(output_dir, f"enhanced_sensitivity_summary_{timestamp}.csv")
               main_summary_df.to_csv(csv_file, index=False)
               print(f"✓ CSV summary saved: {csv_file}")
           
           # === SAVE TO MONGODB FOR WEB ACCESS ===
           print(f"\n=== SAVING TO MONGODB ===")
           
           # Save main summary to database
           if 'main_summary_df' in locals() and not main_summary_df.empty:
               try:
                   # Clear existing sensitivity summaries
                   summary_collection = db_mgr.get_collection("SENS_Summary_Main")
                   summary_collection.delete_many({"sensitivity_prefix": sensitivity_prefix})
                   
                   # Add metadata to each record
                   # Convert NaN to None for MongoDB compatibility
                   main_summary_df = main_summary_df.replace({np.nan: None})
                   summary_records = main_summary_df.to_dict('records')
                   for record in summary_records:
                       record['sensitivity_prefix'] = sensitivity_prefix
                       record['created_at'] = datetime.now()
                       record['analysis_type'] = 'main_summary'
                   
                   summary_collection.insert_many(summary_records)
                   print(f"✓ Saved {len(summary_records)} main summary records to SENS_Summary_Main")
               except Exception as e:
                   print(f"Error saving main summary to MongoDB: {e}")
           
           # Save portfolio IRR matrix
           if 'portfolio_irr_pivot' in locals() and not portfolio_irr_pivot.empty:
               try:
                   matrix_collection = db_mgr.get_collection("SENS_IRR_Matrix")
                   matrix_collection.delete_many({
                       "sensitivity_prefix": sensitivity_prefix,
                       "matrix_type": "portfolio_irr"
                   })
                   
                   # Convert pivot table to records
                   matrix_records = []
                   for param_idx, row in portfolio_irr_pivot.iterrows():
                       for input_val, irr_val in row.items():
                           if not pd.isna(irr_val):
                               matrix_records.append({
                                   'sensitivity_prefix': sensitivity_prefix,
                                   'matrix_type': 'portfolio_irr',
                                   'parameter': param_idx,
                                   'input_value': float(input_val),
                                   'irr_percentage': float(irr_val),
                                   'created_at': datetime.now()
                               })
                   
                   if matrix_records:
                       matrix_collection.insert_many(matrix_records)
                       print(f"✓ Saved {len(matrix_records)} portfolio IRR matrix records to SENS_IRR_Matrix")
               except Exception as e:
                   print(f"Error saving portfolio IRR matrix to MongoDB: {e}")
           
           # Save portfolio IRR difference matrix
           if 'portfolio_irr_diff_pivot' in locals() and not portfolio_irr_diff_pivot.empty:
               try:
                   # Clear existing diff matrix
                   matrix_collection.delete_many({
                       "sensitivity_prefix": sensitivity_prefix,
                       "matrix_type": "portfolio_irr_diff_bps"
                   })
                   
                   # Convert pivot table to records
                   diff_records = []
                   for param_idx, row in portfolio_irr_diff_pivot.iterrows():
                       for input_val, diff_val in row.items():
                           if not pd.isna(diff_val):
                               diff_records.append({
                                   'sensitivity_prefix': sensitivity_prefix,
                                   'matrix_type': 'portfolio_irr_diff_bps',
                                   'parameter': param_idx,
                                   'input_value': float(input_val),
                                   'irr_diff_bps': float(diff_val),
                                   'created_at': datetime.now()
                               })
                   
                   if diff_records:
                       matrix_collection.insert_many(diff_records)
                       print(f"✓ Saved {len(diff_records)} portfolio IRR diff matrix records to SENS_IRR_Matrix")
               except Exception as e:
                   print(f"Error saving portfolio IRR diff matrix to MongoDB: {e}")
           
           # Save asset-level IRR matrices
           for asset_id in sorted(all_asset_ids):
               try:
                   # Clear existing asset matrices
                   matrix_collection.delete_many({
                       "sensitivity_prefix": sensitivity_prefix,
                       "matrix_type": {"$in": [f"asset_{asset_id}_irr", f"asset_{asset_id}_irr_diff_bps"]},
                   })
                   
                   asset_irr_col = f'asset_{asset_id}_irr_pct'
                   asset_diff_col = f'asset_{asset_id}_irr_diff_bps'
                   
                   # Asset IRR matrix
                   if asset_irr_col in main_summary_df.columns:
                       asset_irr_pivot = main_summary_df.pivot_table(
                           index='parameter',
                           columns='input_value',
                           values=asset_irr_col,
                           aggfunc='first'
                       )
                       
                       asset_irr_records = []
                       for param_idx, row in asset_irr_pivot.iterrows():
                           for input_val, irr_val in row.items():
                               if not pd.isna(irr_val):
                                   asset_irr_records.append({
                                       'sensitivity_prefix': sensitivity_prefix,
                                       'matrix_type': f'asset_{asset_id}_irr',
                                       'asset_id': asset_id,
                                       'parameter': param_idx,
                                       'input_value': float(input_val),
                                       'irr_percentage': float(irr_val),
                                       'created_at': datetime.now()
                                   })
                       
                       if asset_irr_records:
                           matrix_collection.insert_many(asset_irr_records)
                           print(f"✓ Saved {len(asset_irr_records)} Asset {asset_id} IRR matrix records")
                   
                   # Asset IRR difference matrix
                   if asset_diff_col in main_summary_df.columns:
                       asset_diff_pivot = main_summary_df.pivot_table(
                           index='parameter',
                           columns='input_value',
                           values=asset_diff_col,
                           aggfunc='first'
                       )
                       
                       asset_diff_records = []
                       for param_idx, row in asset_diff_pivot.iterrows():
                           for input_val, diff_val in row.items():
                               if not pd.isna(diff_val):
                                   asset_diff_records.append({
                                       'sensitivity_prefix': sensitivity_prefix,
                                       'matrix_type': f'asset_{asset_id}_irr_diff_bps',
                                       'asset_id': asset_id,
                                       'parameter': param_idx,
                                       'input_value': float(input_val),
                                       'irr_diff_bps': float(diff_val),
                                       'created_at': datetime.now()
                                   })
                       
                       if asset_diff_records:
                           matrix_collection.insert_many(asset_diff_records)
                           print(f"✓ Saved {len(asset_diff_records)} Asset {asset_id} IRR diff matrix records")
               
               except Exception as e:
                   print(f"Error saving Asset {asset_id} matrices to MongoDB: {e}")
           
           # Save parameter impact ranking
           if 'impact_df' in locals() and not impact_df.empty:
               try:
                   ranking_collection = db_mgr.get_collection("SENS_Parameter_Impact")
                   ranking_collection.delete_many({"sensitivity_prefix": sensitivity_prefix})
                   
                   ranking_records = impact_df.to_dict('records')
                   for record in ranking_records:
                       record['sensitivity_prefix'] = sensitivity_prefix
                       record['created_at'] = datetime.now()
                   
                   ranking_collection.insert_many(ranking_records)
                   print(f"✓ Saved {len(ranking_records)} parameter impact records to SENS_Parameter_Impact")
               except Exception as e:
                   print(f"Error saving parameter impact ranking to MongoDB: {e}")
           
           # Save base case reference
           if base_case_portfolio_irr or base_case_asset_irrs:
               try:
                   base_case_collection = db_mgr.get_collection("SENS_Base_Case")
                   base_case_collection.delete_many({"sensitivity_prefix": sensitivity_prefix})
                   
                   base_case_records = []
                   
                   # Portfolio base case
                   if base_case_portfolio_irr:
                       base_case_records.append({
                           'sensitivity_prefix': sensitivity_prefix,
                           'metric_type': 'portfolio',
                           'asset_id': None,
                           'metric_name': 'Portfolio IRR',
                           'irr_percentage': base_case_portfolio_irr * 100,
                           'created_at': datetime.now()
                       })
                   
                   # Asset base cases
                   for asset_id, asset_irr in base_case_asset_irrs.items():
                       if asset_irr:
                           base_case_records.append({
                               'sensitivity_prefix': sensitivity_prefix,
                               'metric_type': 'asset',
                               'asset_id': asset_id,
                               'metric_name': f'Asset {asset_id} IRR',
                               'irr_percentage': asset_irr * 100,
                               'created_at': datetime.now()
                           })
                   
                   if base_case_records:
                       base_case_collection.insert_many(base_case_records)
                       print(f"✓ Saved {len(base_case_records)} base case records to SENS_Base_Case")
               except Exception as e:
                   print(f"Error saving base case reference to MongoDB: {e}")
           
           # Save metadata summary
           try:
               metadata_collection = db_mgr.get_collection("SENS_Metadata")
               metadata_collection.delete_many({"sensitivity_prefix": sensitivity_prefix})
               
               metadata_record = {
                   'sensitivity_prefix': sensitivity_prefix,
                   'total_scenarios': len(results),
                   'parameters_tested': list(parameters),
                   'assets_analyzed': sorted(list(all_asset_ids)),
                   'base_case_portfolio_irr_pct': base_case_portfolio_irr * 100 if base_case_portfolio_irr else None,
                   'analysis_timestamp': datetime.now(),
                   'created_at': datetime.now()
               }
               
               # Add best/worst scenario info
               valid_results = [r for r in results if r['portfolio_irr_diff_bps'] is not None]
               if valid_results:
                   best_scenario = max(valid_results, key=lambda x: x['portfolio_irr_diff_bps'])
                   worst_scenario = min(valid_results, key=lambda x: x['portfolio_irr_diff_bps'])
                   
                   metadata_record.update({
                       'best_scenario': {
                           'parameter': best_scenario['parameter'],
                           'parameter_name': best_scenario['parameter_name'],
                           'input_value': best_scenario['input_value'],
                           'portfolio_irr_pct': best_scenario['portfolio_irr_pct'],
                           'portfolio_irr_diff_bps': best_scenario['portfolio_irr_diff_bps']
                       },
                       'worst_scenario': {
                           'parameter': worst_scenario['parameter'],
                           'parameter_name': worst_scenario['parameter_name'],
                           'input_value': worst_scenario['input_value'],
                           'portfolio_irr_pct': worst_scenario['portfolio_irr_pct'],
                           'portfolio_irr_diff_bps': worst_scenario['portfolio_irr_diff_bps']
                       }
                   })
               
               metadata_collection.insert_one(metadata_record)
               print(f"✓ Saved metadata summary to SENS_Metadata")
           except Exception as e:
               print(f"Error saving metadata to MongoDB: {e}")
           
           print(f"=== MONGODB SAVE COMPLETE ===")
           
           # Print key insights
           print(f"\n=== KEY INSIGHTS ===")
           print(f"Total scenarios analyzed: {len(results)}")
           print(f"Parameters tested: {len(parameters)}")
           print(f"Assets analyzed: {len(all_asset_ids)}")
           print(f"Assets found: {sorted(all_asset_ids)}")
           
           if base_case_portfolio_irr:
               # Best and worst scenarios
               valid_results = [r for r in results if r['portfolio_irr_diff_bps'] is not None]
               if valid_results:
                   best_scenario = max(valid_results, key=lambda x: x['portfolio_irr_diff_bps'])
                   worst_scenario = min(valid_results, key=lambda x: x['portfolio_irr_diff_bps'])
                   
                   print(f"\nBest scenario: {best_scenario['parameter_name']} = {best_scenario['input_value']} {best_scenario['parameter_units']}")
                   print(f"  Portfolio IRR: {best_scenario['portfolio_irr_pct']:.2f}% (+{best_scenario['portfolio_irr_diff_bps']:.0f}bps vs base)")
                   
                   print(f"\nWorst scenario: {worst_scenario['parameter_name']} = {worst_scenario['input_value']} {worst_scenario['parameter_units']}")
                   print(f"  Portfolio IRR: {worst_scenario['portfolio_irr_pct']:.2f}% ({worst_scenario['portfolio_irr_diff_bps']:.0f}bps vs base)")
                   
                   # Parameter sensitivity ranking
                   if 'impact_df' in locals() and not impact_df.empty:
                       print(f"\nParameter sensitivity ranking (max portfolio IRR impact):")
                       for _, row in impact_df.head(5).iterrows():
                           print(f"  {row['parameter_name']}: ±{row['portfolio_max_impact_bps']:.0f}bps max impact")
           
           print(f"\n=== ENHANCED SENSITIVITY SUMMARY COMPLETE ===")
           print(f"\n=== DATABASE COLLECTIONS CREATED ===")
           print(f"✓ SENS_Summary_Main - Main summary data")
           print(f"✓ SENS_IRR_Matrix - IRR matrices for web charts")
           print(f"✓ SENS_Parameter_Impact - Parameter sensitivity ranking")
           print(f"✓ SENS_Base_Case - Base case reference values")
           print(f"✓ SENS_Metadata - Analysis metadata and key insights")
           print(f"Use sensitivity_prefix='{sensitivity_prefix}' to query these collections")
           
           return main_summary_df if 'main_summary_df' in locals() else None
           
   except Exception as e:
       print(f"Error generating enhanced summary: {e}")
       import traceback
       traceback.print_exc()
       return None

if __name__ == '__main__':
   import argparse
   
   parser = argparse.ArgumentParser(description="Generate enhanced sensitivity analysis summary")
   parser.add_argument('--prefix', type=str, default='sensitivity_results',
                      help='Sensitivity scenario prefix to analyze')
   parser.add_argument('--format', type=str, choices=['excel', 'csv', 'both'], default='excel',
                      help='Output format (default: excel)')
   
   args = parser.parse_args()
   
   # Use the database lifecycle context manager
   with database_lifecycle():
       summary_df = generate_enhanced_sensitivity_summary(args.prefix, args.format)