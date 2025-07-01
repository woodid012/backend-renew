# backend/main.py

import sys
import os

# Add the backend directory to the Python path for module imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import json
import os
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from config import DATE_FORMAT, OUTPUT_DATE_FORMAT, DEFAULT_CAPEX_FUNDING_TYPE, DEFAULT_DEBT_REPAYMENT_FREQUENCY, DEFAULT_DEBT_GRACE_PERIOD, USER_MODEL_START_DATE, USER_MODEL_END_DATE, DEFAULT_DEBT_SIZING_METHOD, DSCR_CALCULATION_FREQUENCY, ENABLE_TERMINAL_VALUE, MERCHANT_PRICE_ESCALATION_RATE, MERCHANT_PRICE_ESCALATION_REFERENCE_DATE
from core.input_processor import load_asset_data, load_price_data
from calculations.revenue import calculate_revenue_timeseries
from calculations.expenses import calculate_opex_timeseries, calculate_capex_timeseries
from calculations.debt import calculate_debt_schedule
from calculations.cashflow import aggregate_cashflows
from core.output_generator import generate_asset_and_platform_output
from core.summary_generator import generate_summary_data
from core.equity_irr import calculate_equity_irr


# Load real data
# Construct the absolute path to the public directory
current_dir = os.path.dirname(os.path.abspath(__file__))
# project_root = os.path.abspath(os.path.join(current_dir, '..')) # Go up one level from 'backend' to 'renew-asset-platform'

zebre_json_path = os.path.join(current_dir, 'inputs', 'zebre_2025-01-13.json')
monthly_price_path = os.path.join(current_dir, 'inputs', 'merchant_price_monthly.csv')
yearly_spread_path = os.path.join(current_dir, 'inputs', 'merchant_yearly_spreads.csv')

ASSETS, ASSET_COST_ASSUMPTIONS = load_asset_data(zebre_json_path)
MONTHLY_PRICES, YEARLY_SPREADS = load_price_data(monthly_price_path, yearly_spread_path)

def run_cashflow_model():
    """
    Main function to run the cash flow model.

    Returns:
        str: JSON representation of the final cash flow DataFrame.
    """
    print("=== STARTING CASHFLOW MODEL ===")
    
    
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
    print("\n=== CALCULATING REVENUE ===")
    output_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'results')
    revenue_df = calculate_revenue_timeseries(ASSETS, MONTHLY_PRICES, YEARLY_SPREADS, start_date, end_date, output_directory)

    # 2. Calculate Expenses (initial CAPEX with assumed funding split)
    print("\n=== CALCULATING EXPENSES ===")
    opex_df = calculate_opex_timeseries(ASSETS, ASSET_COST_ASSUMPTIONS, start_date, end_date)
    initial_capex_df = calculate_capex_timeseries(ASSETS, ASSET_COST_ASSUMPTIONS, start_date, end_date, capex_funding_type=DEFAULT_CAPEX_FUNDING_TYPE)

    # 3. Calculate preliminary CFADS for debt sizing
    print("\n=== CALCULATING PRELIMINARY CASH FLOWS ===")
    prelim_cash_flow = pd.merge(revenue_df, opex_df, on=['asset_id', 'date'])
    prelim_cash_flow['cfads'] = prelim_cash_flow['revenue'] - prelim_cash_flow['opex']

    # 4. Size debt based on operational cash flows and update CAPEX funding
    print("\n=== DEBT SIZING ===")
    debt_df, updated_capex_df = calculate_debt_schedule(ASSETS, ASSET_COST_ASSUMPTIONS, initial_capex_df, prelim_cash_flow, start_date, end_date, repayment_frequency=DEFAULT_DEBT_REPAYMENT_FREQUENCY, grace_period=DEFAULT_DEBT_GRACE_PERIOD, debt_sizing_method=DEFAULT_DEBT_SIZING_METHOD, dscr_calculation_frequency=DSCR_CALCULATION_FREQUENCY)

    # 5. Aggregate into Final Cash Flow using updated CAPEX with correct debt/equity split
    print("\n=== AGGREGATING FINAL CASH FLOWS ===")
    final_cash_flow = aggregate_cashflows(revenue_df, opex_df, updated_capex_df, debt_df, end_date, ASSETS, ASSET_COST_ASSUMPTIONS)

    # Assign period type (Construction or Operations)
    def assign_period_type(df, assets_data):
        df['period_type'] = ''
        df['date'] = pd.to_datetime(df['date'])

        for asset_info in assets_data:
            asset_id = asset_info['id']
            asset_df = df[df['asset_id'] == asset_id]
            
            construction_start = pd.to_datetime(asset_info.get('constructionStartDate')) if asset_info.get('constructionStartDate') else None
            ops_start = pd.to_datetime(asset_info.get('assetStartDate')) if asset_info.get('assetStartDate') else None

            if construction_start and ops_start:
                df.loc[(df['asset_id'] == asset_id) & (df['date'] >= construction_start) & (df['date'] < ops_start), 'period_type'] = 'C'
                df.loc[(df['asset_id'] == asset_id) & (df['date'] >= ops_start), 'period_type'] = 'O'
            elif ops_start:
                # If no construction start, assume operations from assetStartDate
                df.loc[(df['asset_id'] == asset_id) & (df['date'] >= ops_start), 'period_type'] = 'O'
            # If neither is available, period_type remains empty or can be set to a default like 'U' for unknown

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
            print(f"{asset_name}: CAPEX ${total_capex:,.0f} = Debt ${total_debt:,.0f} ({gearing:.1%}) + Equity ${total_equity:,.0f} ({1-gearing:.1%})")
        else:
            print(f"{asset_name}: No CAPEX")
    
    total_portfolio_capex = updated_capex_df['capex'].sum()
    total_portfolio_debt = updated_capex_df['debt_capex'].sum()
    total_portfolio_equity = updated_capex_df['equity_capex'].sum()
    
    if total_portfolio_capex > 0:
        portfolio_gearing = total_portfolio_debt / total_portfolio_capex
        print(f"\nPORTFOLIO TOTAL: CAPEX ${total_portfolio_capex:,.0f} = Debt ${total_portfolio_debt:,.0f} ({portfolio_gearing:.1%}) + Equity ${total_portfolio_equity:,.0f} ({1-portfolio_gearing:.1%})")
    print("========================\n")
    
    # Calculate Equity IRR - ONLY for Construction + Operations + Terminal periods
    print("=== CALCULATING EQUITY XIRR ===")
    print("Including Construction + Operations + Terminal periods...")
    
    # Filter cash flows to include only Construction ('C') and Operations ('O') periods
    equity_irr_df = final_cash_flow[
        (final_cash_flow['period_type'].isin(['C', 'O'])) & 
        (final_cash_flow['equity_cash_flow'] != 0)
    ].copy()
    
    if not equity_irr_df.empty:
        # Group by date to get total equity cash flows across all assets for each date
        equity_irr_summary = equity_irr_df.groupby('date')['equity_cash_flow'].sum().reset_index()
        
        # Calculate XIRR using the updated function with dates
        irr = calculate_equity_irr(equity_irr_summary)
        
        if pd.isna(irr):
            print("Warning: Could not calculate Equity XIRR")
        else:
            print(f"Calculated Equity XIRR: {irr:.2%}")
    else:
        irr = float('nan')
        print("Warning: No equity cash flows found for Construction + Operations periods")

    # Generate summary data
    print("\n=== GENERATING SUMMARY DATA ===")
    summary_data = generate_summary_data(final_cash_flow)
    print("Generated summary data:")
    for key, df in summary_data.items():
        print(f"  {key}: {len(df)} periods")

    # Save to JSON file
    print("\n=== SAVING OUTPUTS ===")
    generate_asset_and_platform_output(final_cash_flow, irr, output_directory)

    def generate_asset_inputs_summary(assets, asset_cost_assumptions, config_values, debt_summary, output_dir):
        asset_summaries = []
        for asset in assets:
            asset_id = asset.get('id')
            asset_name = asset.get('name')
            
            # Include all direct asset properties
            asset_data = {k: v for k, v in asset.items()}
            
            # Include cost assumptions for the asset
            asset_data['costAssumptions'] = asset_cost_assumptions.get(asset_name, {})
            
            # Include debt sizing results (calculated outcome)
            asset_data['debtSizingResults'] = debt_summary.get(asset_id, {})
            
            asset_summaries.append(asset_data)
        
        full_summary = {
            "asset_inputs": asset_summaries,
            "general_config": config_values,
            "portfolio_debt_summary": debt_summary,
            "equity_irr": irr
        }
        
        output_path = os.path.join(output_dir, "asset_inputs_summary.json")
        with open(output_path, 'w') as f:
            json.dump(full_summary, f, indent=4, default=str)  # default=str handles datetime serialization
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
    generate_asset_inputs_summary(ASSETS, ASSET_COST_ASSUMPTIONS, config_values, debt_summary, output_directory)

    print("\n=== CASHFLOW MODEL COMPLETE ===")
    print(f"Equity XIRR: {irr:.2%}" if not pd.isna(irr) else "Equity XIRR: Could not calculate")
    
    return "Cash flow model run complete. Outputs saved and summaries generated."

import sys

if __name__ == '__main__':
    final_cashflows_json = run_cashflow_model()

    print(final_cashflows_json)