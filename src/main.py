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
from src.core.input_processor import load_asset_data, load_price_data
from src.calculations.revenue import calculate_revenue_timeseries
from src.calculations.opex import calculate_opex_timeseries
from src.calculations.construction_capex import calculate_capex_timeseries
from src.calculations.debt import calculate_debt_schedule
from src.calculations.cashflow import aggregate_cashflows
from src.calculations.depreciation import calculate_d_and_a
from src.core.output_generator import generate_asset_and_platform_output, export_three_way_financials_to_excel
from src.core.summary_generator import generate_summary_data
from src.core.equity_irr import calculate_equity_irr
from src.core.database import insert_dataframe_to_mongodb, get_mongo_client, clear_base_case_data, clear_all_scenario_data
from src.core.scenario_manager import load_scenario, apply_all_scenarios_to_timeseries, apply_post_debt_sizing_capex_scenarios


def run_cashflow_model(scenario_file=None, scenario_id=None, run_sensitivity=False, replace_data=True):
    """
    Main function to run the cash flow model.

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

    zebre_json_path = os.path.join(project_root, 'data', 'raw_inputs', 'zebre_2025-01-13.json')
    monthly_price_path = os.path.join(project_root, 'data', 'raw_inputs', 'merchant_price_monthly.csv')
    yearly_spread_path = os.path.join(project_root, 'data', 'raw_inputs', 'merchant_yearly_spreads.csv')

    ASSETS, ASSET_COST_ASSUMPTIONS = load_asset_data(zebre_json_path)
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
    print("========================\n")
    
    # Calculate Equity IRR - ONLY for Construction + Operations + Terminal periods
    print("=== CALCULATING EQUITY IRR ===")
    
    # Filter cash flows to include only Construction ('C') and Operations ('O') periods
    co_periods_df = final_cash_flow[final_cash_flow['period_type'].isin(['C', 'O'])].copy()
    
    if not co_periods_df.empty:
        # Further filter for non-zero equity cash flows
        equity_irr_df = co_periods_df[co_periods_df['equity_cash_flow'] != 0].copy()
        
        if not equity_irr_df.empty:
            # Group by date to get total equity cash flows across all assets for each date
            equity_irr_summary = equity_irr_df.groupby('date')['equity_cash_flow'].sum().reset_index()
            
            # Calculate XIRR using the updated function with dates
            irr = calculate_equity_irr(equity_irr_summary)
            
            if pd.isna(irr):
                print("Warning: Could not calculate Equity IRR")
            else:
                print(f"Equity IRR: {irr:.2%}")
        else:
            irr = float('nan')
            print("Warning: No equity cash flows found")
    else:
        irr = float('nan')
        print("Warning: No Construction + Operations periods found")

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

    def generate_asset_inputs_summary(assets, asset_cost_assumptions, config_values, debt_summary, output_dir, irr_value, scenario_id=None):
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
                debt_results = debt_summary.get(asset_id, {})
                for k, v in debt_results.items():
                    flat_asset_data[f'debt_{k}'] = v
                
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
            portfolio_debt_df.index.name = 'Asset ID'
            portfolio_debt_df.to_excel(writer, sheet_name='Portfolio Debt Summary')

            # Sheet 4: Equity IRR
            irr_df = pd.DataFrame([{'Equity IRR': irr_value}])
            irr_df.to_excel(writer, sheet_name='Equity IRR', index=False)
            
        print(f"Saved asset inputs summary to {output_path}")

    # Extract debt sizing summary
    debt_summary = {}
    for asset in ASSETS:
        asset_id = asset['id']
        asset_capex = updated_capex_df[updated_capex_df['asset_id'] == asset_id]
        total_capex = asset_capex['capex'].sum()
        total_debt = asset_capex['debt_capex'].sum()
        total_equity = asset_capex['equity_capex'].sum()
        
        debt_summary[asset_id] = {
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
        generate_asset_inputs_summary(ASSETS, ASSET_COST_ASSUMPTIONS, config_values, debt_summary, output_directory, irr, scenario_id=scenario_id)

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