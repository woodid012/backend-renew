# scripts/enhanced_sensitivity_summary.py

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

from src.core.database import database_lifecycle, mongo_session, insert_dataframe_to_mongodb
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
   """Calculate portfolio IRR from raw scenario data - FIXED to include terminal value"""
   if not scenario_data:
       return None
   
   df = pd.DataFrame(scenario_data)
   df['date'] = pd.to_datetime(df['date'])
   
   # CRITICAL FIX: Include ALL periods with equity cash flows, not just C+O
   # Terminal value is typically included in equity_cash_flow but may not have period_type
   # Filter for Construction + Operations periods, OR any period with terminal_value > 0
   if 'period_type' in df.columns:
       co_df = df[
           df['period_type'].isin(['C', 'O']) | 
           (df['terminal_value'] > 0) |
           (df['equity_cash_flow'] != 0)  # Include any non-zero equity cash flows
       ].copy()
   else:
       # If no period_type, include all data
       co_df = df.copy()
   
   if co_df.empty:
       return None
   
   # Group by date and sum equity cash flows across all assets
   portfolio_cf = co_df.groupby('date')['equity_cash_flow'].sum().reset_index()
   
   # Filter non-zero cash flows
   portfolio_cf = portfolio_cf[portfolio_cf['equity_cash_flow'] != 0]
   
   if portfolio_cf.empty:
       return None
   
   # Debug: Check for terminal values
   terminal_value_total = df['terminal_value'].sum() if 'terminal_value' in df.columns else 0
   if terminal_value_total > 0:
       print(f"    Including terminal value: ${terminal_value_total:,.0f}M")
   
   # Calculate IRR
   irr = calculate_equity_irr(portfolio_cf)
   return irr if not pd.isna(irr) else None

def calculate_asset_irr_from_data(scenario_data, asset_id):
   """Calculate IRR for a specific asset - FIXED for consistent asset_id handling AND terminal value inclusion"""
   if not scenario_data:
       return None
   
   try:
       df = pd.DataFrame(scenario_data)
       
       # Validate required columns
       if 'date' not in df.columns:
           print(f"      ERROR: Asset {asset_id} - 'date' column missing in scenario data")
           return None
       if 'asset_id' not in df.columns:
           print(f"      ERROR: Asset {asset_id} - 'asset_id' column missing in scenario data")
           return None
       if 'equity_cash_flow' not in df.columns:
           print(f"      ERROR: Asset {asset_id} - 'equity_cash_flow' column missing in scenario data")
           return None
       
       df['date'] = pd.to_datetime(df['date'])
       
       # CRITICAL FIX: Ensure asset_id comparison is consistent (convert both sides to string)
       # Convert asset_id to string for consistent comparison
       asset_id_str = str(asset_id)
       
       # Handle both string and numeric asset_id types in the dataframe
       if df['asset_id'].dtype == 'object':
           # If it's object type, try converting to string
           asset_df = df[df['asset_id'].astype(str) == asset_id_str].copy()
       else:
           # If numeric, try both numeric and string comparison
           try:
               asset_id_num = int(asset_id_str) if asset_id_str.isdigit() else None
               if asset_id_num is not None:
                   asset_df = df[df['asset_id'] == asset_id_num].copy()
               else:
                   asset_df = df[df['asset_id'].astype(str) == asset_id_str].copy()
           except (ValueError, TypeError):
               asset_df = df[df['asset_id'].astype(str) == asset_id_str].copy()
       
       if asset_df.empty:
           # Debug: Log when asset is not found
           unique_asset_ids = []
           if 'asset_id' in df.columns:
               try:
                   unique_asset_ids = [str(aid) for aid in df['asset_id'].unique()]
               except Exception:
                   unique_asset_ids = list(df['asset_id'].unique())
           print(f"      WARNING: Asset {asset_id_str} not found in scenario data. Available assets: {unique_asset_ids}")
           return None
       
       # CRITICAL FIX: Include ALL periods with cash flows, not just C+O
       # Terminal value needs to be included in IRR calculation
       if 'period_type' in asset_df.columns:
           co_df = asset_df[
               asset_df['period_type'].isin(['C', 'O']) | 
               (asset_df.get('terminal_value', pd.Series([0]) * len(asset_df)) > 0) |
               (asset_df['equity_cash_flow'] != 0)  # Include any non-zero equity cash flows
           ].copy()
       else:
           # If no period_type, include all data
           co_df = asset_df.copy()
       
       if co_df.empty:
           print(f"      WARNING: Asset {asset_id_str} - No periods found after filtering")
           return None
       
       # Get equity cash flows for this asset
       if 'equity_cash_flow' not in co_df.columns:
           print(f"      ERROR: Asset {asset_id_str} - 'equity_cash_flow' column missing after filtering")
           return None
       
       asset_cf = co_df[['date', 'equity_cash_flow']].copy()
       asset_cf = asset_cf[asset_cf['equity_cash_flow'] != 0]
       
       if asset_cf.empty:
           print(f"      WARNING: Asset {asset_id_str} - No non-zero equity cash flows found")
           # Debug: Check total cash flows
           total_cf = co_df['equity_cash_flow'].sum()
           print(f"      DEBUG: Asset {asset_id_str} - Total equity cash flow: ${total_cf:,.2f}M")
           return None
       
       # Validate cash flow data
       if len(asset_cf) < 2:
           print(f"      WARNING: Asset {asset_id_str} - Insufficient cash flow periods ({len(asset_cf)}), need at least 2")
           return None
       
       # Debug: Check for terminal values for this asset
       asset_terminal_value = asset_df['terminal_value'].sum() if 'terminal_value' in asset_df.columns else 0
       if asset_terminal_value > 0:
           print(f"      Asset {asset_id_str} terminal value: ${asset_terminal_value:,.0f}M")
       
       # Calculate IRR
       irr = calculate_equity_irr(asset_cf)
       
       if pd.isna(irr):
           print(f"      WARNING: Asset {asset_id_str} - IRR calculation returned NaN")
           # Debug: Print cash flow summary
           print(f"      DEBUG: Asset {asset_id_str} - Cash flows: {len(asset_cf)} periods, Total: ${asset_cf['equity_cash_flow'].sum():,.2f}M")
           return None
       
       return irr
       
   except Exception as e:
       print(f"      ERROR: Asset {asset_id_str} - Exception in IRR calculation: {e}")
       import traceback
       traceback.print_exc()
       return None

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

           # FIXED: Calculate asset IRRs and metrics with proper key consistency
           asset_irrs = {}
           asset_metrics = {}
           
           for asset_id in unique_assets:
               # Ensure consistent string keys
               asset_key = str(asset_id)
               asset_id_str = str(asset_id)  # Use consistent string conversion
               
               asset_irr = calculate_asset_irr_from_data(base_data, asset_id)
               if asset_irr is not None:
                   asset_irrs[asset_key] = asset_irr
               else:
                   print(f"      WARNING: Could not calculate IRR for asset {asset_key} in base case")
               
               # Use consistent filtering for metrics
               asset_df = df[df['asset_id'].astype(str) == asset_id_str]
               asset_capex = asset_df[asset_df['capex'] > 0]['capex'].sum()
               asset_debt = asset_df[asset_df['debt_capex'] > 0]['debt_capex'].sum()
               asset_gearing = asset_debt / asset_capex if asset_capex > 0 else 0
               asset_revenue = asset_df['revenue'].sum()
               
               asset_metrics[asset_key] = {
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
   FIXED: Asset processing loops to prevent asset mixing
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
           
           # Validation: Check for expected scenarios based on common sensitivity parameters
           # This helps identify if scenarios were not executed
           expected_scenarios = []
           common_params = ['volume', 'capex', 'opex', 'electricity_price', 'green_price', 'interest_rate', 'terminal_value']
           
           for param in common_params:
               # Check for common values that should exist
               if param == 'volume':
                   # Volume typically has values like 0.9, 1.0 (base), 1.1
                   expected_scenarios.extend([
                       f"{sensitivity_prefix}_volume_0.9000",
                       f"{sensitivity_prefix}_volume_1.1000"
                   ])
               elif param == 'capex':
                   # CAPEX typically has values like 0.95, 1.0 (base), 1.05
                   expected_scenarios.extend([
                       f"{sensitivity_prefix}_capex_0.9500",
                       f"{sensitivity_prefix}_capex_1.0500"
                   ])
           
           # Check which expected scenarios are missing
           missing_scenarios = [s for s in expected_scenarios if s not in scenario_ids]
           if missing_scenarios:
               print(f"  WARNING: Expected scenarios not found: {missing_scenarios}")
               print(f"  This may indicate scenarios failed to execute or were not configured")
           else:
               print(f"  ✓ All expected common scenarios found")
           
           # Log all scenario IDs for debugging
           print(f"  Scenario IDs found: {sorted(scenario_ids)[:10]}..." if len(scenario_ids) > 10 else f"  Scenario IDs found: {sorted(scenario_ids)}")
           
           # Get base case metrics for comparison
           base_case_portfolio_irr, base_case_asset_irrs, base_case_total_capex, base_case_total_revenue, base_case_portfolio_gearing, base_case_asset_metrics = get_base_case_metrics()
           print(f"Base case portfolio IRR: {base_case_portfolio_irr:.2%}" if base_case_portfolio_irr else "Base case portfolio IRR: Not found")
           print(f"Base case asset IRRs keys: {list(base_case_asset_irrs.keys())}")

           # Extract unique_id from sensitivity data (should be the same for all scenarios)
           portfolio_unique_id = None
           if scenario_ids:
               sample_scenario_data = list(collection.find({"scenario_id": scenario_ids[0]}, limit=1))
               if sample_scenario_data and 'unique_id' in sample_scenario_data[0]:
                   portfolio_unique_id = sample_scenario_data[0]['unique_id']
                   print(f"Found portfolio unique_id: {portfolio_unique_id}")
               else:
                   print("Warning: No unique_id found in sensitivity data")

           # FIXED: Build complete asset list with consistent string handling
           all_asset_ids = set()
           
           # Add base case assets
           all_asset_ids.update(base_case_asset_irrs.keys())
           print(f"Base case assets: {sorted(list(base_case_asset_irrs.keys()))}")
           
           # Add sensitivity scenario assets - check ALL scenarios to ensure we get all assets
           scenario_assets_found = set()
           for scenario_id in scenario_ids:
               scenario_data = list(collection.find({"scenario_id": scenario_id}))
               if scenario_data:
                   df_temp = pd.DataFrame(scenario_data)
                   if 'asset_id' in df_temp.columns:
                       # Convert all asset IDs to strings for consistency
                       try:
                           scenario_asset_ids = [str(aid) for aid in df_temp['asset_id'].unique()]
                           all_asset_ids.update(scenario_asset_ids)
                           scenario_assets_found.update(scenario_asset_ids)
                       except Exception as e:
                           print(f"  WARNING: Error extracting asset IDs from scenario {scenario_id}: {e}")
                           # Try alternative method
                           try:
                               unique_ids = df_temp['asset_id'].unique()
                               scenario_asset_ids = [str(aid) for aid in unique_ids]
                               all_asset_ids.update(scenario_asset_ids)
                               scenario_assets_found.update(scenario_asset_ids)
                           except Exception as e2:
                               print(f"  ERROR: Failed to extract asset IDs from scenario {scenario_id}: {e2}")
           
           all_asset_ids = sorted(list(all_asset_ids))
           print(f"All unique assets found across all scenarios: {all_asset_ids}")
           
           # Validation: Check if we found assets in scenarios
           if not scenario_assets_found:
               print("  WARNING: No assets found in any sensitivity scenarios!")
           else:
               print(f"Assets found in sensitivity scenarios: {sorted(list(scenario_assets_found))}")
           
           # Validation: Check for assets in base case but not in scenarios (or vice versa)
           base_case_only = set(base_case_asset_irrs.keys()) - scenario_assets_found
           scenario_only = scenario_assets_found - set(base_case_asset_irrs.keys())
           if base_case_only:
               print(f"  WARNING: Assets in base case but not in any scenario: {sorted(list(base_case_only))}")
           if scenario_only:
               print(f"  WARNING: Assets in scenarios but not in base case: {sorted(list(scenario_only))}")

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
           
           # FIXED: Process each scenario with proper asset isolation
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
               
               # CRITICAL FIX: Calculate asset-level IRRs with proper isolation
               unique_assets_in_scenario = []
               if 'asset_id' in df.columns:
                   try:
                       unique_assets_in_scenario = [str(aid) for aid in df['asset_id'].unique()]
                   except Exception as e:
                       print(f"      ERROR: Failed to extract unique asset IDs from scenario {scenario_id}: {e}")
                       # Try alternative extraction
                       try:
                           unique_assets_in_scenario = list(df['asset_id'].astype(str).unique())
                       except Exception as e2:
                           print(f"      ERROR: Alternative extraction also failed: {e2}")
               
               asset_irrs = {}
               asset_metrics = {}
               
               # Debug: Log assets found in scenario
               print(f"      Assets found in scenario {scenario_id}: {unique_assets_in_scenario}")
               
               # Process only assets that exist in this scenario
               assets_with_irr = []
               assets_without_irr = []
               
               for asset_id in unique_assets_in_scenario:
                   # Ensure consistent string conversion
                   asset_id_str = str(asset_id)
                   
                   # Calculate asset IRR for this specific asset in this specific scenario
                   asset_irr = calculate_asset_irr_from_data(scenario_data, asset_id)
                   if asset_irr is not None:
                       asset_irrs[asset_id_str] = asset_irr
                       assets_with_irr.append(asset_id_str)
                   else:
                       assets_without_irr.append(asset_id_str)
                       print(f"      WARNING: Could not calculate IRR for asset {asset_id_str} in scenario {scenario_id}")
               
               # Summary for this scenario
               if assets_without_irr:
                   print(f"      Scenario {scenario_id}: {len(assets_with_irr)} assets with IRR, {len(assets_without_irr)} assets without IRR")
                   print(f"      Assets without IRR: {assets_without_irr}")
               else:
                   print(f"      Scenario {scenario_id}: All {len(assets_with_irr)} assets have calculated IRRs")
                   
                   # Calculate asset metrics with consistent filtering
                   asset_df = df[df['asset_id'].astype(str) == asset_id_str]
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
               asset_count = len([k for k, v in asset_irrs.items() if v is not None])
               print(f"  {param_meta['name']} = {input_value} {param_meta['units']} → Portfolio IRR: {irr_str} ({diff_str}), {asset_count} assets with IRR")
           
           if not results:
               print("No valid results to summarize")
               return
           
           # Create output directory
           timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
           output_dir = os.path.join(project_root, "output", "sensitivity_analysis")
           os.makedirs(output_dir, exist_ok=True)
           
           # Create summary DataFrame (always, regardless of output format)
           summary_data = []
           
           # Create asset columns in deterministic order
           all_asset_cols = []
           for asset_id in sorted(all_asset_ids):
               all_asset_cols.append(f'asset_{asset_id}_irr_pct')
               all_asset_cols.append(f'asset_{asset_id}_irr_diff_bps')

           # Combine all expected columns
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
               
               # CRITICAL FIX: Populate asset-specific fields with consistent key handling
               # Ensure ALL assets from all_asset_ids are included, even if they don't have data
               for asset_id in sorted(all_asset_ids):
                   asset_key = str(asset_id)
                   
                   # Get asset IRR from the result (may be None if not calculated)
                   asset_irr = result['asset_irrs'].get(asset_key)
                   asset_irr_pct = asset_irr * 100 if asset_irr is not None else None
                   row[f'asset_{asset_id}_irr_pct'] = asset_irr_pct
                   
                   # Calculate IRR difference
                   base_asset_irr = base_case_asset_irrs.get(asset_key)
                   asset_irr_diff_bps = None
                   
                   if result['scenario_id'] == 'base_case':
                       # Base case: difference is always 0
                       asset_irr_diff_bps = 0
                   elif asset_irr is not None and base_asset_irr is not None:
                       # Both values exist: calculate difference
                       asset_irr_diff_bps = (asset_irr - base_asset_irr) * 10000
                   elif asset_irr is None and base_asset_irr is not None:
                       # Asset has base case but no scenario value: leave as None (will show as null)
                       asset_irr_diff_bps = None
                   elif asset_irr is not None and base_asset_irr is None:
                       # Asset has scenario value but no base case: can't calculate difference
                       asset_irr_diff_bps = None
                       print(f"      WARNING: Asset {asset_key} has IRR in scenario but not in base case")
                   
                   row[f'asset_{asset_id}_irr_diff_bps'] = asset_irr_diff_bps
               
               summary_data.append(row)
           
           main_summary_df = pd.DataFrame(summary_data)
           main_summary_df = main_summary_df.reindex(columns=all_expected_cols)
           main_summary_df = main_summary_df.sort_values(['parameter', 'input_value'])
           
           # Add unique_id to DataFrame for MongoDB filtering
           if portfolio_unique_id:
               main_summary_df['unique_id'] = portfolio_unique_id
           
           # FIXED: Generate comprehensive Excel output with proper asset column handling
           if output_format in ["excel", "both"]:
               excel_file = os.path.join(output_dir, f"enhanced_sensitivity_summary_{timestamp}.xlsx")
               
               with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
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
               
               print(f"\n✓ Enhanced Excel summary saved: {excel_file}")
           
           # Generate CSV if requested
           if output_format in ["csv", "both"]:
               csv_file = os.path.join(output_dir, f"enhanced_sensitivity_summary_{timestamp}.csv")
               main_summary_df.to_csv(csv_file, index=False)
               print(f"✓ CSV summary saved: {csv_file}")
           
           # Write summary to MongoDB SENS_Summary_Main collection
           if not main_summary_df.empty:
               print(f"\nWriting sensitivity summary to MongoDB collection: SENS_Summary_Main")
               try:
                   summary_collection = db_mgr.get_collection("SENS_Summary_Main")
                   
                   # Delete existing records for this unique_id and sensitivity_prefix
                   if portfolio_unique_id:
                       # Delete records matching this unique_id and sensitivity prefix
                       delete_query = {
                           "unique_id": portfolio_unique_id,
                           "scenario_id": {"$regex": f"^{sensitivity_prefix}"}
                       }
                       delete_result = summary_collection.delete_many(delete_query)
                       print(f"Deleted {delete_result.deleted_count} existing records from SENS_Summary_Main for unique_id: {portfolio_unique_id}")
                       
                       # Also delete base_case if it exists for this unique_id
                       base_case_delete_query = {
                           "unique_id": portfolio_unique_id,
                           "scenario_id": "base_case"
                       }
                       base_case_delete_result = summary_collection.delete_many(base_case_delete_query)
                       if base_case_delete_result.deleted_count > 0:
                           print(f"Deleted {base_case_delete_result.deleted_count} base_case records from SENS_Summary_Main")
                   else:
                       # If no unique_id, delete by scenario_id pattern only (less safe, but better than nothing)
                       print("⚠️  Warning: No portfolio_unique_id found, deleting by scenario_id pattern only")
                       delete_query = {
                           "scenario_id": {"$regex": f"^{sensitivity_prefix}"}
                       }
                       delete_result = summary_collection.delete_many(delete_query)
                       print(f"Deleted {delete_result.deleted_count} existing records from SENS_Summary_Main (by scenario_id only)")
                   
                   # Write the summary DataFrame to MongoDB
                   # Convert DataFrame to records and insert
                   records = main_summary_df.to_dict(orient='records')
                   if records:
                       insert_result = summary_collection.insert_many(records)
                       print(f"✓ Successfully wrote {len(insert_result.inserted_ids)} records to SENS_Summary_Main collection")
                   else:
                       print("⚠️  Warning: No records to insert into SENS_Summary_Main")
               except Exception as e:
                   print(f"⚠️  Warning: Failed to write to MongoDB SENS_Summary_Main: {e}")
                   import traceback
                   traceback.print_exc()
           
           # Final validation summary
           print(f"\n=== VALIDATION SUMMARY ===")
           print(f"Total scenarios processed: {len(results)}")
           print(f"Total assets in summary: {len(all_asset_ids)}")
           
           # Check for assets with missing IRRs across scenarios
           assets_with_issues = {}
           for result in results:
               if result['scenario_id'] == 'base_case':
                   continue
               for asset_id in all_asset_ids:
                   asset_key = str(asset_id)
                   if asset_key not in result['asset_irrs'] or result['asset_irrs'][asset_key] is None:
                       if asset_key not in assets_with_issues:
                           assets_with_issues[asset_key] = []
                       assets_with_issues[asset_key].append(result['scenario_id'])
           
           if assets_with_issues:
               print(f"  WARNING: {len(assets_with_issues)} asset(s) have missing IRRs in some scenarios:")
               for asset_key, scenarios in assets_with_issues.items():
                   print(f"    Asset {asset_key}: Missing in {len(scenarios)} scenario(s) (e.g., {scenarios[0]})")
           else:
               print(f"  ✓ All assets have IRRs calculated for all scenarios")
           
           # Check for missing expected scenarios
           if missing_scenarios:
               print(f"  WARNING: {len(missing_scenarios)} expected scenario(s) not found")
           else:
               print(f"  ✓ All expected scenarios found")
           
           print(f"\n=== ENHANCED SENSITIVITY SUMMARY COMPLETE ===")
           
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