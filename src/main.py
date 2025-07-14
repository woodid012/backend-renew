# src/main.py

import sys
import os
import argparse

# Add the parent directory to the Python path for module imports
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# Import from src modules
from src.config import (
    DATE_FORMAT, OUTPUT_DATE_FORMAT, DEFAULT_CAPEX_FUNDING_TYPE, 
    DEFAULT_DEBT_REPAYMENT_FREQUENCY, DEFAULT_DEBT_GRACE_PERIOD, 
    USER_MODEL_START_DATE, USER_MODEL_END_DATE, DEFAULT_DEBT_SIZING_METHOD, 
    DSCR_CALCULATION_FREQUENCY, ENABLE_TERMINAL_VALUE, 
    MERCHANT_PRICE_ESCALATION_RATE, MERCHANT_PRICE_ESCALATION_REFERENCE_DATE, 
    MONGO_ASSET_OUTPUT_COLLECTION, MONGO_ASSET_INPUTS_SUMMARY_COLLECTION, 
    TAX_RATE, DEFAULT_ASSET_LIFE_YEARS
)
from src.core.input_processor import load_price_data
from src.calculations.revenue import calculate_revenue_timeseries
from src.calculations.opex import calculate_opex_timeseries
from src.calculations.construction_capex import calculate_capex_timeseries
from src.calculations.debt import calculate_debt_schedule
from src.calculations.cashflow import aggregate_cashflows
from src.calculations.depreciation import calculate_d_and_a
from src.core.output_generator import generate_asset_and_platform_output, export_three_way_financials_to_excel
from src.core.summary_generator import generate_summary_data
from src.core.equity_irr import calculate_equity_irr, calculate_asset_equity_irrs
from src.core.database import (
    insert_dataframe_to_mongodb, clear_base_case_data, clear_all_scenario_data, 
    get_data_from_mongodb, database_lifecycle, ensure_connection
)
from src.core.scenario_manager import load_scenario, apply_all_scenarios_to_timeseries, apply_post_debt_sizing_capex_scenarios


def generate_asset_output_summary(final_cash_flow, irr, asset_irrs, ASSETS, updated_capex_df, scenario_id=None):
    """
    Generate Asset Output Summary with key metrics for each asset and portfolio total.
    
    Args:
        final_cash_flow (pd.DataFrame): Final cash flow DataFrame
        irr (float): Portfolio equity IRR
        asset_irrs (dict): Dictionary of asset IRRs by asset_id
        ASSETS (list): List of asset dictionaries
        updated_capex_df (pd.DataFrame): Updated CAPEX DataFrame with debt/equity split
        scenario_id (str, optional): Scenario identifier
    
    Returns:
        pd.DataFrame: Summary DataFrame
    """
    from src.config import MONGO_ASSET_OUTPUT_SUMMARY_COLLECTION
    from src.core.database import insert_dataframe_to_mongodb
    import pandas as pd
    from datetime import datetime
    
    print("\n=== GENERATING ASSET OUTPUT SUMMARY ===")
    
    summary_records = []
    
    # Process each asset
    for asset in ASSETS:
        asset_id = asset['id']
        asset_name = asset.get('name', f'Asset_{asset_id}')
        
        # Get asset-specific cash flows
        asset_cf = final_cash_flow[final_cash_flow['asset_id'] == asset_id].copy()
        
        # Extract key dates
        cons_start = asset.get('constructionStartDate', '')
        ops_start = asset.get('OperatingStartDate', '')
        
        # Calculate operations end date
        ops_end = ''
        if ops_start and asset.get('assetLife'):
            try:
                ops_start_date = pd.to_datetime(ops_start)
                asset_life_years = int(asset.get('assetLife', 25))
                ops_end_date = ops_start_date + pd.DateOffset(years=asset_life_years)
                ops_end = ops_end_date.strftime('%Y-%m-%d')
            except:
                ops_end = ''
        
        # Get terminal value from cash flows
        terminal_value = asset_cf['terminal_value'].sum() if 'terminal_value' in asset_cf.columns else 0.0
        
        # Get CAPEX breakdown
        asset_capex = updated_capex_df[updated_capex_df['asset_id'] == asset_id]
        total_capex = asset_capex['capex'].sum()
        total_debt = asset_capex['debt_capex'].sum()
        total_equity = asset_capex['equity_capex'].sum()
        
        # Get asset IRR
        asset_irr = asset_irrs.get(asset_id)
        if pd.isna(asset_irr):
            asset_irr = None
        
        # Calculate totals
        total_revenue = asset_cf['revenue'].sum() if 'revenue' in asset_cf.columns else 0.0
        total_opex = asset_cf['opex'].sum() if 'opex' in asset_cf.columns else 0.0
        total_cfads = asset_cf['cfads'].sum() if 'cfads' in asset_cf.columns else 0.0
        total_equity_cash_flow = asset_cf['equity_cash_flow'].sum() if 'equity_cash_flow' in asset_cf.columns else 0.0
        
        summary_record = {
            'asset_id': asset_id,
            'asset_name': asset_name,
            'construction_start_date': cons_start,
            'operations_start_date': ops_start,
            'operations_end_date': ops_end,
            'terminal_value': terminal_value,
            'total_capex': total_capex,
            'total_debt': total_debt,
            'total_equity': total_equity,
            'equity_irr': asset_irr,
            'total_revenue': total_revenue,
            'total_opex': total_opex,
            'total_cfads': total_cfads,
            'total_equity_cash_flow': total_equity_cash_flow
        }
        
        summary_records.append(summary_record)
        print(f"  Asset {asset_name}: IRR {asset_irr:.2%}" if asset_irr else f"  Asset {asset_name}: IRR N/A")
    
    # Add Platform/Portfolio summary row
    portfolio_asset_id = len(ASSETS) + 1
    
    # Calculate portfolio totals
    portfolio_terminal_value = final_cash_flow['terminal_value'].sum() if 'terminal_value' in final_cash_flow.columns else 0.0
    portfolio_capex = updated_capex_df['capex'].sum()
    portfolio_debt = updated_capex_df['debt_capex'].sum()
    portfolio_equity = updated_capex_df['equity_capex'].sum()
    portfolio_revenue = final_cash_flow['revenue'].sum() if 'revenue' in final_cash_flow.columns else 0.0
    portfolio_opex = final_cash_flow['opex'].sum() if 'opex' in final_cash_flow.columns else 0.0
    portfolio_cfads = final_cash_flow['cfads'].sum() if 'cfads' in final_cash_flow.columns else 0.0
    portfolio_equity_cash_flow = final_cash_flow['equity_cash_flow'].sum() if 'equity_cash_flow' in final_cash_flow.columns else 0.0
    
    # Get earliest construction start and latest operations end for portfolio
    earliest_cons_start = ''
    latest_ops_start = ''
    latest_ops_end = ''
    
    try:
        cons_dates = [pd.to_datetime(asset.get('constructionStartDate')) for asset in ASSETS 
                     if asset.get('constructionStartDate')]
        if cons_dates:
            earliest_cons_start = min(cons_dates).strftime('%Y-%m-%d')
        
        ops_dates = [pd.to_datetime(asset.get('OperatingStartDate')) for asset in ASSETS 
                    if asset.get('OperatingStartDate')]
        if ops_dates:
            latest_ops_start = max(ops_dates).strftime('%Y-%m-%d')
        
        # Calculate latest operations end
        ops_end_dates = []
        for asset in ASSETS:
            if asset.get('OperatingStartDate') and asset.get('assetLife'):
                ops_start_date = pd.to_datetime(asset['OperatingStartDate'])
                asset_life_years = int(asset.get('assetLife', 25))
                ops_end_date = ops_start_date + pd.DateOffset(years=asset_life_years)
                ops_end_dates.append(ops_end_date)
        
        if ops_end_dates:
            latest_ops_end = max(ops_end_dates).strftime('%Y-%m-%d')
    except:
        pass
    
    portfolio_record = {
        'asset_id': portfolio_asset_id,
        'asset_name': 'Platform',
        'construction_start_date': earliest_cons_start,
        'operations_start_date': latest_ops_start,
        'operations_end_date': latest_ops_end,
        'terminal_value': portfolio_terminal_value,
        'total_capex': portfolio_capex,
        'total_debt': portfolio_debt,
        'total_equity': portfolio_equity,
        'equity_irr': irr if not pd.isna(irr) else None,
        'total_revenue': portfolio_revenue,
        'total_opex': portfolio_opex,
        'total_cfads': portfolio_cfads,
        'total_equity_cash_flow': portfolio_equity_cash_flow
    }
    
    summary_records.append(portfolio_record)
    print(f"  Platform: IRR {irr:.2%}" if not pd.isna(irr) else f"  Platform: IRR N/A")
    
    # Create DataFrame
    summary_df = pd.DataFrame(summary_records)
    
    # Write to MongoDB
    try:
        print("Writing asset output summary to MongoDB...")
        insert_dataframe_to_mongodb(
            summary_df, 
            MONGO_ASSET_OUTPUT_SUMMARY_COLLECTION, 
            scenario_id=scenario_id,
            replace_scenario=True
        )
        print(f"Successfully wrote {len(summary_df)} records to {MONGO_ASSET_OUTPUT_SUMMARY_COLLECTION}")
    except Exception as e:
        print(f"Error writing asset output summary to MongoDB: {e}")
        raise
    
    print(f"=== ASSET OUTPUT SUMMARY COMPLETE ===\n")
    
    return summary_df


def run_cashflow_model(scenario_file=None, scenario_id=None, run_sensitivity=False, replace_data=True):
    """
    Main function to run the cash flow model.
    
    NOTE: This function assumes database connection is already established by the caller.
    Use database_lifecycle context manager when calling this function.

    Args:
        scenario_file (str, optional): Path to scenario JSON file
        scenario_id (str, optional): Unique identifier for the scenario run
        run_sensitivity (bool): Whether to run sensitivity analysis
        replace_data (bool): Whether to replace existing data (default: True)

    Returns:
        str: JSON representation of the final cash flow DataFrame.
    """
    print("=== STARTING CASHFLOW MODEL ===")
        
    # Load real data
    # Construct the absolute path to the data directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)

    monthly_price_path = os.path.join(project_root, 'data', 'raw_inputs', 'merchant_price_monthly.csv')
    yearly_spread_path = os.path.join(project_root, 'data', 'raw_inputs', 'merchant_yearly_spreads.csv')

    config_data = get_data_from_mongodb('CONFIG_Inputs')
    if not config_data:
        raise ValueError("Could not load config data from MongoDB")
    config_data = config_data[0]  # Get the first document
    ASSETS = config_data.get('asset_inputs', [])
    ASSET_COST_ASSUMPTIONS = {}
    for asset in ASSETS:
        asset_name = asset.get('name')
        if asset_name and 'costAssumptions' in asset:
            ASSET_COST_ASSUMPTIONS[asset_name] = asset.get('costAssumptions')
    MONTHLY_PRICES, YEARLY_SPREADS = load_price_data(monthly_price_path, yearly_spread_path)

    # Load scenario data if provided
    scenario_data = None
    if scenario_file:
        scenario_data = load_scenario(scenario_file)
        print(f"Loaded scenario: {scenario_data.get('scenario_name', 'Unnamed')}")

    # Determine model start and end dates
    if USER_MODEL_START_DATE and USER_MODEL_END_DATE:
        start_date = datetime.strptime(USER_MODEL_START_DATE, DATE_FORMAT)
        end_date = datetime.strptime(USER_MODEL_END_DATE, DATE_FORMAT)
    else:
        earliest_construction_start = pd.to_datetime('2050-01-01') # Initialize with a future date
        latest_ops_end = pd.to_datetime('1900-01-01') # Initialize with a past date

        for asset in ASSETS:
            # Ensure 'OperatingStartDate' is set, defaulting to 'assetStartDate' if not present
            if 'OperatingStartDate' not in asset and 'assetStartDate' in asset:
                asset['OperatingStartDate'] = asset['assetStartDate']

            # Use 'constructionStartDate' for the earliest start
            if 'constructionStartDate' in asset and asset['constructionStartDate']:
                current_start = pd.to_datetime(asset['constructionStartDate'])
                if current_start < earliest_construction_start:
                    earliest_construction_start = current_start
            
            # Calculate end date based on OperatingStartDate + assetLife
            if 'OperatingStartDate' in asset and asset['OperatingStartDate'] and 'assetLife' in asset and asset['assetLife']:
                ops_start_date = pd.to_datetime(asset['OperatingStartDate'])
                asset_life_years = int(asset['assetLife'])
                current_ops_end = ops_start_date + relativedelta(years=asset_life_years)
                
                if current_ops_end > latest_ops_end:
                    latest_ops_end = current_ops_end
            elif 'operationsEndDate' in asset and asset['operationsEndDate']:
                current_end = pd.to_datetime(asset['operationsEndDate'])
                if current_end > latest_ops_end:
                    latest_ops_end = current_end

        start_date = earliest_construction_start
        end_date = latest_ops_end

        if start_date == pd.to_datetime('2050-01-01') or end_date == pd.to_datetime('1900-01-01'):
            raise ValueError("Could not determine valid model start or end dates from asset data. Please check 'constructionStartDate', 'assetStartDate' and 'assetLife' (or 'operationsEndDate') in your asset data, or set USER_MODEL_START_DATE and USER_MODEL_END_DATE in config.py.")

    print(f"Model period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    # 1. Calculate Revenue
    output_directory = os.path.join(project_root, 'output', 'model_results')
    revenue_df = calculate_revenue_timeseries(ASSETS, MONTHLY_PRICES, YEARLY_SPREADS, start_date, end_date, output_directory)

    # 2. Calculate OPEX and CAPEX (initial CAPEX with assumed funding split)
    opex_df = calculate_opex_timeseries(ASSETS, ASSET_COST_ASSUMPTIONS, start_date, end_date)
    initial_capex_df = calculate_capex_timeseries(ASSETS, ASSET_COST_ASSUMPTIONS, start_date, end_date, capex_funding_type=DEFAULT_CAPEX_FUNDING_TYPE)

    # 2b. Apply ALL scenario overrides to calculated timeseries (NEW APPROACH)
    if scenario_file:
        print(f"Applying scenario overrides from {scenario_file}...")
        revenue_df, opex_df, initial_capex_df, ASSET_COST_ASSUMPTIONS = apply_all_scenarios_to_timeseries(
            revenue_df, opex_df, initial_capex_df, ASSETS, ASSET_COST_ASSUMPTIONS, 
            MONTHLY_PRICES, YEARLY_SPREADS, start_date, end_date, scenario_data
        )

    # 3. Calculate preliminary CFADS for debt sizing
    prelim_cash_flow = pd.merge(revenue_df, opex_df, on=['asset_id', 'date'])
    prelim_cash_flow['cfads'] = prelim_cash_flow['revenue'] - prelim_cash_flow['opex']

    # Calculate Depreciation & Amortization (D&A)
    d_and_a_df = calculate_d_and_a(initial_capex_df, pd.DataFrame(columns=['asset_id', 'date', 'intangible_capex']), ASSETS, DEFAULT_ASSET_LIFE_YEARS, DEFAULT_ASSET_LIFE_YEARS, start_date, end_date)

    # 4. Size debt based on operational cash flows and update CAPEX funding
    debt_df, updated_capex_df = calculate_debt_schedule(ASSETS, ASSET_COST_ASSUMPTIONS, initial_capex_df, prelim_cash_flow, start_date, end_date, repayment_frequency=DEFAULT_DEBT_REPAYMENT_FREQUENCY, grace_period=DEFAULT_DEBT_GRACE_PERIOD, debt_sizing_method=DEFAULT_DEBT_SIZING_METHOD, dscr_calculation_frequency=DSCR_CALCULATION_FREQUENCY)

    # 4b. Apply CAPEX scenarios to debt-sized CAPEX schedule (FINAL CAPEX ADJUSTMENT)
    if scenario_file:
        updated_capex_df = apply_post_debt_sizing_capex_scenarios(updated_capex_df, scenario_data)

    # 5. Aggregate into Final Cash Flow using updated CAPEX with correct debt/equity split
    final_cash_flow = aggregate_cashflows(revenue_df, opex_df, updated_capex_df, debt_df, d_and_a_df, end_date, ASSETS, ASSET_COST_ASSUMPTIONS)

    # Assign period type (Construction or Operations)
    def assign_period_type(df, assets_data):
        df['period_type'] = ''
        df['date'] = pd.to_datetime(df['date'])

        for asset_info in assets_data:
            asset_id = asset_info['id']
            
            # Use consistent date field names - only OperatingStartDate
            construction_start = None
            ops_start = None
            
            if 'constructionStartDate' in asset_info and asset_info['constructionStartDate']:
                construction_start = pd.to_datetime(asset_info['constructionStartDate'])
            
            # Only use OperatingStartDate
            if 'OperatingStartDate' in asset_info and asset_info['OperatingStartDate']:
                ops_start = pd.to_datetime(asset_info['OperatingStartDate'])
            
            if construction_start and ops_start:
                # Construction period: from construction start to operations start
                construction_mask = (df['asset_id'] == asset_id) & (df['date'] >= construction_start) & (df['date'] < ops_start)
                operations_mask = (df['asset_id'] == asset_id) & (df['date'] >= ops_start)
                
                df.loc[construction_mask, 'period_type'] = 'C'
                df.loc[operations_mask, 'period_type'] = 'O'
                
            elif ops_start:
                # If no construction start, assume all periods from ops start are operations
                operations_mask = (df['asset_id'] == asset_id) & (df['date'] >= ops_start)
                df.loc[operations_mask, 'period_type'] = 'O'

        return df

    final_cash_flow = assign_period_type(final_cash_flow, ASSETS)

    # Print debt sizing summary
    print("\n=== DEBT SIZING SUMMARY ===")
    for asset in ASSETS:
        asset_id = asset['id']
        asset_name = asset.get('name', f'Asset_{asset_id}')
        
        # Get total CAPEX for this asset
        asset_capex = updated_capex_df[updated_capex_df['asset_id'] == asset_id]
        total_capex = asset_capex['capex'].sum()
        total_debt = asset_capex['debt_capex'].sum()
        total_equity = asset_capex['equity_capex'].sum()
        
        if total_capex > 0:
            gearing = total_debt / total_capex
            print(f"{asset_name}: CAPEX ${total_capex:,.0f}M = Debt ${total_debt:,.0f}M ({gearing:.1%}) + Equity ${total_equity:,.0f}M ({1-gearing:.1%})")
        else:
            print(f"{asset_name}: No CAPEX")
    
    total_portfolio_capex = updated_capex_df['capex'].sum()
    total_portfolio_debt = updated_capex_df['debt_capex'].sum()
    total_portfolio_equity = updated_capex_df['equity_capex'].sum()
    
    if total_portfolio_capex > 0:
        portfolio_gearing = total_portfolio_debt / total_portfolio_capex
        print(f"\nPORTFOLIO TOTAL: CAPEX ${total_portfolio_capex:,.0f}M = Debt ${total_portfolio_debt:,.0f}M ({portfolio_gearing:.1%}) + Equity ${total_portfolio_equity:,.0f}M ({1-portfolio_gearing:.1%})")
    else:
        portfolio_gearing = 0
    print("========================\n")
    
    # Calculate Equity IRR - ONLY for Construction + Operations + Terminal periods
 
    print("=== CALCULATING EQUITY IRR ===")
    
    # Filter cash flows to include only Construction ('C') and Operations ('O') periods
    co_periods_df = final_cash_flow[final_cash_flow['period_type'].isin(['C', 'O'])].copy()
    
    if not co_periods_df.empty:
        # CRITICAL FIX: Use equity_cash_flow_pre_distributions for IRR calculation
        # This represents the cash available to equity before accounting distributions
        # IRR should reflect investor returns, not internal distribution accounting
        
        # Filter for non-zero equity cash flows (pre-distributions)
        equity_irr_df = co_periods_df[co_periods_df['equity_cash_flow_pre_distributions'] != 0].copy()
        
        if not equity_irr_df.empty:
            # Group by date to get total equity cash flows across all assets for each date
            equity_irr_summary = equity_irr_df.groupby('date')['equity_cash_flow_pre_distributions'].sum().reset_index()
            
            # Rename column for IRR function compatibility
            equity_irr_summary = equity_irr_summary.rename(columns={'equity_cash_flow_pre_distributions': 'equity_cash_flow'})
            
            # Calculate XIRR using the updated function with dates
            irr = calculate_equity_irr(equity_irr_summary)
            
            if pd.isna(irr):
                print("Warning: Could not calculate Equity IRR")
            else:
                print(f"Equity IRR: {irr:.2%}")
                print(f"  Based on equity_cash_flow_pre_distributions (cash available to equity)")
        else:
            irr = float('nan')
            print("Warning: No equity cash flows found")
    else:
        irr = float('nan')
        print("Warning: No Construction + Operations periods found")

    # Calculate individual asset IRRs - ALSO FIXED
    print("=== CALCULATING INDIVIDUAL ASSET IRRs ===")
    
    # Fix the asset IRR calculation function as well
    def calculate_asset_equity_irrs_fixed(final_cash_flow_df):
        """
        Calculates the Equity IRR for each unique asset using equity_cash_flow_pre_distributions.
        """
        asset_irrs = {}
        if 'asset_id' not in final_cash_flow_df.columns:
            print("Warning: 'asset_id' column not found in cash flow DataFrame. Cannot calculate asset-level IRRs.")
            return asset_irrs

        unique_assets = final_cash_flow_df['asset_id'].unique()
        print(f"Calculating asset-level IRRs for {len(unique_assets)} assets...")

        for asset_id in unique_assets:
            # Filter cash flows for the current asset
            asset_df = final_cash_flow_df[final_cash_flow_df['asset_id'] == asset_id].copy()

            # Filter for Construction ('C') and Operations ('O') periods
            if 'period_type' in asset_df.columns:
                co_periods_df = asset_df[asset_df['period_type'].isin(['C', 'O'])].copy()
            else:
                co_periods_df = asset_df.copy()

            # Use equity_cash_flow_pre_distributions for consistency
            equity_irr_df = co_periods_df[co_periods_df['equity_cash_flow_pre_distributions'] != 0].copy()

            if not equity_irr_df.empty:
                # Group by date and sum equity cash flows (pre-distributions)
                equity_irr_summary = equity_irr_df.groupby('date')['equity_cash_flow_pre_distributions'].sum().reset_index()
                
                # Rename for function compatibility
                equity_irr_summary = equity_irr_summary.rename(columns={'equity_cash_flow_pre_distributions': 'equity_cash_flow'})
                
                irr = calculate_equity_irr(equity_irr_summary)
                asset_irrs[asset_id] = irr
                print(f"  Asset {asset_id} IRR: {irr:.2%}" if not pd.isna(irr) else f"  Asset {asset_id} IRR: Could not calculate")
            else:
                asset_irrs[asset_id] = float('nan')
                print(f"  Asset {asset_id} IRR: No equity cash flows found")

        return asset_irrs
    
    # Use the fixed function
    asset_irrs = calculate_asset_equity_irrs_fixed(final_cash_flow)


    # Calculate missing variables for the summary function
    asset_type_map = {asset['id']: asset.get('assetType', 'unknown') for asset in ASSETS}

    # Generate summary data
    summary_data = generate_summary_data(final_cash_flow)

    # Save to files
    print("\n=== SAVING OUTPUTS ===")
    generate_asset_and_platform_output(final_cash_flow, irr, output_directory, scenario_id=scenario_id)
    export_three_way_financials_to_excel(final_cash_flow, output_directory, scenario_id=scenario_id)

    # === CRITICAL: WRITE TO MONGODB ===
    print("\n=== WRITING TO MONGODB ===")
    print(f"Debug: replace_data={replace_data}, scenario_id={scenario_id}")
    try:
        # Handle data replacement
        if replace_data:
            if scenario_id:
                print(f"Clearing existing data for scenario: {scenario_id}")
                clear_all_scenario_data(scenario_id)
            else:
                print(f"Clearing existing base case data")
                clear_base_case_data()
        else:
            print(f"Appending new data (replace_data=False)")

        # Write main cash flow data with replace option
        print("Writing main cash flow data to MongoDB...")
        insert_dataframe_to_mongodb(
            final_cash_flow, 
            MONGO_ASSET_OUTPUT_COLLECTION, 
            scenario_id=scenario_id,
            replace_scenario=True  # Always replace for clean data
        )
        print(f"Successfully wrote {len(final_cash_flow)} records to {MONGO_ASSET_OUTPUT_COLLECTION}")
        
        
    except Exception as e:
        print(f"Error writing to MongoDB: {e}")
        print("Model completed but data not saved to database!")
        raise  # Re-raise the error so user knows something went wrong

    def generate_asset_inputs_summary(assets, asset_cost_assumptions, config_values, debt_summary, output_dir, irr_value, asset_irrs, asset_type_map, total_portfolio_capex, total_portfolio_debt, portfolio_gearing, scenario_id=None):
        # Determine the actual output directory based on scenario_id
        if scenario_id:
            actual_output_dir = os.path.join(output_dir, 'scenarios', scenario_id)
        else:
            actual_output_dir = output_dir

        output_path = os.path.join(actual_output_dir, "asset_inputs_summary.xlsx")

        # Ensure output directory exists
        os.makedirs(actual_output_dir, exist_ok=True)
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Sheet 1: Asset Inputs Summary
            asset_summaries = []
            for asset in assets:
                asset_id = asset.get('id')
                asset_name = asset.get('name')
                
                # Flatten asset data for Excel
                flat_asset_data = {'asset_id': asset_id, 'asset_name': asset_name}
                for k, v in asset.items():
                    if isinstance(v, (dict, list)):
                        flat_asset_data[k] = str(v) # Convert complex types to string
                    else:
                        flat_asset_data[k] = v
                
                # Include cost assumptions for the asset
                cost_assumptions = asset_cost_assumptions.get(asset_name, {})
                for k, v in cost_assumptions.items():
                    flat_asset_data[f'cost_{k}'] = v
                
                # Include debt sizing results
                debt_results = debt_summary.get(asset_name, {})
                for k, v in debt_results.items():
                    flat_asset_data[f'debt_{k}'] = v
                
                # Include asset IRR
                asset_irr = asset_irrs.get(asset_id, float('nan'))
                flat_asset_data['asset_equity_irr'] = asset_irr
                
                asset_summaries.append(flat_asset_data)
            
            if asset_summaries:
                asset_summary_df = pd.DataFrame(asset_summaries)
                asset_summary_df.to_excel(writer, sheet_name='Asset Inputs', index=False)
                
                # Also write to MongoDB with replace option
                try:
                    print("Writing asset inputs summary to MongoDB...")
                    insert_dataframe_to_mongodb(
                        asset_summary_df, 
                        MONGO_ASSET_INPUTS_SUMMARY_COLLECTION, 
                        scenario_id=scenario_id,
                        replace_scenario=True  # Always replace for clean data
                    )
                    print(f"Successfully wrote asset inputs summary to {MONGO_ASSET_INPUTS_SUMMARY_COLLECTION}")
                except Exception as e:
                    print(f"Warning: Could not write asset inputs to MongoDB: {e}")
            else:
                pd.DataFrame().to_excel(writer, sheet_name='Asset Inputs', index=False) # Empty DataFrame
            
            # Sheet 2: General Configuration
            config_df = pd.DataFrame.from_dict(config_values, orient='index', columns=['Value'])
            config_df.index.name = 'Parameter'
            config_df.to_excel(writer, sheet_name='General Config')

            # Sheet 3: Portfolio Debt Summary
            portfolio_debt_df = pd.DataFrame.from_dict(debt_summary, orient='index')
            portfolio_debt_df.index.name = 'Asset Name'
            portfolio_debt_df.to_excel(writer, sheet_name='Portfolio Debt Summary')

            # Sheet 4: Equity IRR
            irr_df = pd.DataFrame([{'Portfolio_Equity_IRR': irr_value}])
            irr_df.to_excel(writer, sheet_name='Equity IRR', index=False)
            
            # Sheet 5: Asset IRRs
            asset_irr_df = pd.DataFrame.from_dict(asset_irrs, orient='index', columns=['Asset_Equity_IRR'])
            asset_irr_df.index.name = 'Asset ID'
            asset_irr_df.to_excel(writer, sheet_name='Asset IRRs')
            
        print(f"Saved asset inputs summary to {output_path}")

    # Extract debt sizing summary
    debt_summary = {}
    for asset in ASSETS:
        asset_id = asset['id']
        asset_capex = updated_capex_df[updated_capex_df['asset_id'] == asset_id]
        total_capex = asset_capex['capex'].sum()
        total_debt = asset_capex['debt_capex'].sum()
        total_equity = asset_capex['equity_capex'].sum()
        
        asset_name = asset.get('name', f'Asset_{asset_id}')
        debt_summary[asset_name] = {
            'total_capex': total_capex,
            'debt_amount': total_debt,
            'equity_amount': total_equity,
            'gearing': total_debt / total_capex if total_capex > 0 else 0
        }

    # Extract all relevant config values
    config_values = {
        "DATE_FORMAT": DATE_FORMAT,
        "OUTPUT_DATE_FORMAT": OUTPUT_DATE_FORMAT,
        "DEFAULT_CAPEX_FUNDING_TYPE": DEFAULT_CAPEX_FUNDING_TYPE,
        "DEFAULT_DEBT_REPAYMENT_FREQUENCY": DEFAULT_DEBT_REPAYMENT_FREQUENCY,
        "DEFAULT_DEBT_GRACE_PERIOD": DEFAULT_DEBT_GRACE_PERIOD,
        "USER_MODEL_START_DATE": USER_MODEL_START_DATE,
        "USER_MODEL_END_DATE": USER_MODEL_END_DATE,
        "DEFAULT_DEBT_SIZING_METHOD": DEFAULT_DEBT_SIZING_METHOD,
        "DSCR_CALCULATION_FREQUENCY": DSCR_CALCULATION_FREQUENCY,
        "ENABLE_TERMINAL_VALUE": ENABLE_TERMINAL_VALUE,
        "MERCHANT_PRICE_ESCALATION_RATE": MERCHANT_PRICE_ESCALATION_RATE,
        "MERCHANT_PRICE_ESCALATION_REFERENCE_DATE": MERCHANT_PRICE_ESCALATION_REFERENCE_DATE
    }
    
    # Only generate asset inputs summary if not running sensitivity analysis
    if not run_sensitivity:
        generate_asset_inputs_summary(ASSETS, ASSET_COST_ASSUMPTIONS, config_values, debt_summary, output_directory, irr, asset_irrs, asset_type_map, total_portfolio_capex, total_portfolio_debt, portfolio_gearing, scenario_id=scenario_id)

    # Generate Asset Output Summary
    generate_asset_output_summary(final_cash_flow, irr, asset_irrs, ASSETS, updated_capex_df, scenario_id=scenario_id)

    print("\n=== CASHFLOW MODEL COMPLETE ===")
    print(f"Equity IRR: {irr:.2%}" if not pd.isna(irr) else "Equity IRR: Could not calculate")
    print("All data successfully written to MongoDB!")
    
    return "Cash flow model run complete. Outputs saved and summaries generated."


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run the cash flow model with optional scenario overrides.")
    parser.add_argument('--scenario', type=str, help="Path to a JSON scenario file for overrides.")
    parser.add_argument('--scenario_id', type=str, help="A unique identifier for the scenario run.")
    parser.add_argument('--run_sensitivity', action='store_true', help="Run sensitivity analysis after model execution.")
    parser.add_argument('--append', action='store_true', help="Append to existing data instead of replacing (default: replace)")
    args = parser.parse_args()

    # Default behavior is to replace data, unless --append flag is used
    replace_data = not args.append

    # Use the database lifecycle context manager to manage connection efficiently
    with database_lifecycle():
        final_cashflows_json = run_cashflow_model(
            scenario_file=args.scenario, 
            scenario_id=args.scenario_id,
            replace_data=replace_data
        )

        if args.run_sensitivity:
            print("\n=== RUNNING SENSITIVITY ANALYSIS ===")
            from scripts.run_sensitivity_analysis import run_sensitivity_analysis_improved
            run_sensitivity_analysis_improved()

        print(final_cashflows_json)