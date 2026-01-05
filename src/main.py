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
from src.core.price_curve_manager import load_price_data_from_mongo, get_price_curves_list
from src.calculations.revenue import calculate_revenue_timeseries
from src.calculations.opex import calculate_opex_timeseries
from src.calculations.construction_capex import calculate_capex_timeseries
from src.calculations.debt import calculate_debt_schedule
from src.calculations.cashflow import aggregate_cashflows
from src.calculations.depreciation import calculate_d_and_a
from src.core.output_generator import generate_asset_and_platform_output
from src.core.summary_generator import generate_summary_data
from src.core.equity_irr import calculate_equity_irr, calculate_asset_equity_irrs
from src.core.database import (
    insert_dataframe_to_mongodb, clear_base_case_data, clear_all_scenario_data,
    get_data_from_mongodb, database_lifecycle, ensure_connection, get_mongo_client
)
from src.core.scenario_manager import load_scenario, apply_all_scenarios_to_timeseries
from src.core.hybrid_assets import add_hybrid_asset_summaries


def generate_asset_output_summary(final_cash_flow, irr, asset_irrs, ASSETS, updated_capex_df, scenario_id=None, portfolio_unique_id=None):
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
    
    # Extract portfolio name from final_cash_flow (should be consistent across all rows)
    portfolio_name = None
    if 'portfolio' in final_cash_flow.columns and not final_cash_flow['portfolio'].empty:
        portfolio_name = final_cash_flow['portfolio'].iloc[0]
        print(f"Portfolio name from cash flows: {portfolio_name}")
    else:
        print("Warning: No portfolio field found in final_cash_flow")
    
    summary_records = []
    
    # Import hybrid assets utility
    from src.core.hybrid_assets import get_hybrid_groups
    
    # Get hybrid groups
    hybrid_groups = get_hybrid_groups(ASSETS)
    processed_hybrid_assets = set()
    
    # Process each asset
    for asset in ASSETS:
        asset_id = asset['id']
        asset_name = asset.get('name', f'Asset_{asset_id}')
        hybrid_group = asset.get('hybridGroup')
        display_asset_id = asset_id  # Default to asset_id
        
        # Skip if this asset is part of a hybrid group (we'll process the group separately)
        if hybrid_group and hybrid_group in hybrid_groups:
            if asset_id not in processed_hybrid_assets:
                # Process the hybrid group as a combined asset
                group_asset_ids = hybrid_groups[hybrid_group]
                primary_asset_id = group_asset_ids[0]
                display_asset_id = primary_asset_id
                
                # Get combined cash flows for the hybrid group
                hybrid_mask = final_cash_flow['asset_id'].isin(group_asset_ids)
                asset_cf = final_cash_flow[hybrid_mask].copy()
                
                # If there's a combined row (from add_hybrid_asset_summaries), use that
                if 'hybrid_group' in final_cash_flow.columns:
                    combined_row = final_cash_flow[
                        (final_cash_flow['hybrid_group'] == hybrid_group) & 
                        (final_cash_flow['asset_id'] == primary_asset_id)
                    ]
                    if not combined_row.empty:
                        asset_cf = combined_row.copy()
                
                # Get combined asset name
                component_names = []
                for a in ASSETS:
                    if a.get('id') in group_asset_ids:
                        component_names.append(a.get('name', f'Asset_{a.get("id")}'))
                asset_name = f"{hybrid_group} (Hybrid)"
                
                # Get combined CAPEX
                asset_capex = updated_capex_df[updated_capex_df['asset_id'].isin(group_asset_ids)]
                total_capex = asset_capex['capex'].sum()
                total_debt = asset_capex['debt_capex'].sum()
                total_equity = asset_capex['equity_capex'].sum()
                
                # For hybrid assets, use earliest/latest dates from components
                cons_dates = []
                ops_dates = []
                ops_end_dates = []
                for a in ASSETS:
                    if a.get('id') in group_asset_ids:
                        if a.get('constructionStartDate'):
                            cons_dates.append(pd.to_datetime(a.get('constructionStartDate')))
                        if a.get('OperatingStartDate'):
                            ops_dates.append(pd.to_datetime(a.get('OperatingStartDate')))
                            if a.get('assetLife'):
                                ops_end_dates.append(
                                    pd.to_datetime(a.get('OperatingStartDate')) + 
                                    pd.DateOffset(years=int(a.get('assetLife', 25)))
                                )
                
                cons_start = min(cons_dates).strftime('%Y-%m-%d') if cons_dates else ''
                ops_start = min(ops_dates).strftime('%Y-%m-%d') if ops_dates else ''
                ops_end = max(ops_end_dates).strftime('%Y-%m-%d') if ops_end_dates else ''
                
                # Mark all assets in group as processed
                processed_hybrid_assets.update(group_asset_ids)
            else:
                # Skip individual asset if already processed as part of hybrid group
                continue
        else:
            # Regular asset processing
            asset_cf = final_cash_flow[final_cash_flow['asset_id'] == asset_id].copy()
            
            # Get CAPEX breakdown
            asset_capex = updated_capex_df[updated_capex_df['asset_id'] == asset_id]
            total_capex = asset_capex['capex'].sum()
            total_debt = asset_capex['debt_capex'].sum()
            total_equity = asset_capex['equity_capex'].sum()
            
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
        
        # Get asset IRR
        asset_irr = asset_irrs.get(display_asset_id)
        if pd.isna(asset_irr):
            asset_irr = None
        
        # Calculate totals
        total_revenue = asset_cf['revenue'].sum() if 'revenue' in asset_cf.columns else 0.0
        total_opex = asset_cf['opex'].sum() if 'opex' in asset_cf.columns else 0.0
        total_cfads = asset_cf['cfads'].sum() if 'cfads' in asset_cf.columns else 0.0
        total_equity_cash_flow = asset_cf['equity_cash_flow'].sum() if 'equity_cash_flow' in asset_cf.columns else 0.0
        
        summary_record = {
            'asset_id': display_asset_id,
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
            'total_equity_cash_flow': total_equity_cash_flow,
            'portfolio': portfolio_name,  # Add portfolio field
            'unique_id': portfolio_unique_id  # Add portfolio unique_id
        }
        
        summary_records.append(summary_record)
        print(f"  Asset {asset_name} (ID: {display_asset_id}): IRR {asset_irr:.2%}" if asset_irr else f"  Asset {asset_name} (ID: {display_asset_id}): IRR N/A")
    
    # Add Platform/Portfolio summary row
    # Only add portfolio row if there's more than 1 asset (otherwise it's redundant)
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
    
    # Only add portfolio summary row if there's more than 1 asset
    # For single-asset portfolios, the portfolio row is redundant (same as the asset)
    if len(ASSETS) > 1:
        portfolio_record = {
            'asset_id': portfolio_asset_id,
            'asset_name': 'Platform',
            'unique_id': portfolio_unique_id,  # Portfolio-level unique_id
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
            'total_equity_cash_flow': portfolio_equity_cash_flow,
            'portfolio': portfolio_name  # Add portfolio field
        }
        
        summary_records.append(portfolio_record)
        print(f"  Platform: IRR {irr:.2%}" if not pd.isna(irr) else f"  Platform: IRR N/A")
    else:
        print(f"  Single asset portfolio - skipping redundant portfolio summary row")
    
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


def run_cashflow_model(assets, monthly_prices, yearly_spreads, portfolio_name, scenario_file=None, scenario_id=None, run_sensitivity=False, replace_data=True, model_settings=None, portfolio_unique_id=None, progress_callback=None):
    """
    Main function to run the cash flow model.
    
    NOTE: This function assumes database connection is already established by the caller.
    Use database_lifecycle context manager when calling this function.

    Args:
        assets (list): List of asset dictionaries
        monthly_prices (pd.DataFrame): Monthly price data
        yearly_spreads (pd.DataFrame): Yearly spread data
        portfolio_name (str): Portfolio name to tag results in database
        scenario_file (str, optional): Path to scenario JSON file
        scenario_id (str, optional): Unique identifier for the scenario run
        run_sensitivity (bool): Whether to run sensitivity analysis
        replace_data (bool): Whether to replace existing data (default: True)
        model_settings (dict, optional): Model configuration settings from frontend. If None, uses config.py defaults.
        portfolio_unique_id (str, optional): Portfolio unique identifier
        progress_callback (callable, optional): Function to call with progress updates (message, type='info')

    Returns:
        str: JSON representation of the final cash flow DataFrame.
    """
    def log_progress(message, progress_type='info'):
        """Helper to log progress via callback or print"""
        if progress_callback:
            progress_callback(message, progress_type)
        else:
            print(message, flush=True)
    # Use model_settings if provided, otherwise fall back to config.py defaults
    if model_settings is None:
        model_settings = {}
    
    # Extract settings with fallback to config defaults
    use_asset_start_dates = model_settings.get('useAssetStartDates', True)  # Default to True for backward compatibility
    user_model_start_date = model_settings.get('userModelStartDate') or USER_MODEL_START_DATE
    user_model_end_date = model_settings.get('userModelEndDate') or USER_MODEL_END_DATE
    minimum_model_start_date = model_settings.get('minimumModelStartDate', '2025-01-01')  # Default to 2025-01-01 to ensure price curve data availability
    default_capex_funding_type = model_settings.get('defaultCapexFundingType', DEFAULT_CAPEX_FUNDING_TYPE)
    # Handle combined frequency field with backward compatibility
    debt_repayment_dscr_frequency = model_settings.get('debtRepaymentDscrFrequency')
    if not debt_repayment_dscr_frequency:
        # Fallback to old fields for backward compatibility
        debt_repayment_dscr_frequency = model_settings.get('dscrCalculationFrequency') or \
                                        model_settings.get('defaultDebtRepaymentFrequency') or \
                                        DEFAULT_DEBT_REPAYMENT_FREQUENCY
    default_debt_repayment_frequency = debt_repayment_dscr_frequency
    dscr_calculation_frequency = debt_repayment_dscr_frequency
    default_debt_grace_period = model_settings.get('defaultDebtGracePeriod', DEFAULT_DEBT_GRACE_PERIOD)
    default_debt_sizing_method = model_settings.get('defaultDebtSizingMethod', DEFAULT_DEBT_SIZING_METHOD)
    tax_rate = model_settings.get('taxRate', TAX_RATE)
    default_asset_life_years = model_settings.get('defaultAssetLifeYears', DEFAULT_ASSET_LIFE_YEARS)
    enable_terminal_value = model_settings.get('enableTerminalValue', ENABLE_TERMINAL_VALUE)
    merchant_price_escalation_rate = model_settings.get('merchantPriceEscalationRate', MERCHANT_PRICE_ESCALATION_RATE)
    merchant_price_escalation_reference_date = model_settings.get('merchantPriceEscalationReferenceDate', MERCHANT_PRICE_ESCALATION_REFERENCE_DATE)
    min_cash_balance_for_distribution = model_settings.get('minCashBalanceForDistribution', 2.0)
    log_progress("Initializing cash flow model...", 'info')
    print("\n" + "="*80)
    print("📊 STARTING CASHFLOW MODEL")
    print("="*80)
    print(f"  📍 Function: run_cashflow_model() in src/main.py (line 287)")
    print(f"  📦 Inputs:")
    print(f"     - Number of assets: {len(assets)}")
    print(f"     - Monthly prices shape: {monthly_prices.shape}")
    print(f"     - Yearly spreads shape: {yearly_spreads.shape}")
    print(f"     - Scenario file: {scenario_file}")
    print(f"     - Scenario ID: {scenario_id}")
    print(f"     - Portfolio name: {portfolio_name}")
    print(f"     - Portfolio unique_id: {portfolio_unique_id}")
    print("="*80)
        
    # Construct the absolute path to the data directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)

    # Use provided assets
    ASSETS = assets
    ASSET_COST_ASSUMPTIONS = {}
    for asset in ASSETS:
        asset_name = asset.get('name')
        if asset_name and 'costAssumptions' in asset:
            ASSET_COST_ASSUMPTIONS[asset_name] = asset.get('costAssumptions')
    
    # Log asset initialization
    log_progress(f"Processing {len(ASSETS)} assets...", 'info')
    for i, asset in enumerate(ASSETS, 1):
        asset_id = asset.get('id', 'unknown')
        asset_name = asset.get('name', f'Asset_{asset_id}')
        log_progress(f"Calculating Asset {asset_id} ({asset_name})...", 'info')
    
    # Use provided price data
    MONTHLY_PRICES = monthly_prices
    YEARLY_SPREADS = yearly_spreads

    # Load scenario data if provided
    scenario_data = None
    if scenario_file:
        scenario_data = load_scenario(scenario_file)
        print(f"Loaded scenario: {scenario_data.get('scenario_name', 'Unnamed')}")

    # Determine model start and end dates
    # If use_asset_start_dates is True, ignore user-provided dates and auto-calculate from assets
    # If False, use the user-provided dates
    # In both cases, apply minimum model start date: max(calculated/user date, minimum_model_start_date)
    minimum_start = pd.to_datetime(minimum_model_start_date)
    
    if not use_asset_start_dates and user_model_start_date and user_model_end_date:
        user_start = datetime.strptime(user_model_start_date, DATE_FORMAT)
        start_date = max(user_start, minimum_start)
        end_date = datetime.strptime(user_model_end_date, DATE_FORMAT)
        print(f"  ✅ Using user-provided model dates:")
        if start_date == minimum_start and user_start < minimum_start:
            print(f"     - User start: {user_start.strftime('%Y-%m-%d')} (adjusted to minimum)")
            print(f"     - Start: {start_date.strftime('%Y-%m-%d')} (max(user date, {minimum_start.strftime('%Y-%m-%d')}))")
        else:
            print(f"     - Start: {start_date.strftime('%Y-%m-%d')} (max(user date, {minimum_start.strftime('%Y-%m-%d')}))")
        print(f"     - End: {end_date.strftime('%Y-%m-%d')}")
    else:
        earliest_construction_start = pd.to_datetime('2050-01-01') # Initialize with a future date
        earliest_ops_start = pd.to_datetime('2050-01-01') # Initialize with a future date (fallback)
        latest_ops_end = pd.to_datetime('1900-01-01') # Initialize with a past date

        # Diagnostic: Log what fields are present in assets
        print(f"  🔍 Analyzing {len(ASSETS)} assets for date fields...")
        for i, asset in enumerate(ASSETS):
            asset_id = asset.get('id', 'unknown')
            asset_name = asset.get('name', f'Asset_{asset_id}')
            print(f"     Asset {i+1} ({asset_name}):")
            print(f"       - Asset ID: {asset_id}")
            print(f"       - constructionStartDate: {asset.get('constructionStartDate', 'MISSING')}")
            print(f"       - OperatingStartDate: {asset.get('OperatingStartDate', 'MISSING')}")
            print(f"       - assetStartDate: {asset.get('assetStartDate', 'MISSING')}")
            print(f"       - assetLife: {asset.get('assetLife', 'MISSING')}")
            print(f"       - operationsEndDate: {asset.get('operationsEndDate', 'MISSING')}")

        for asset in ASSETS:
            # Ensure 'OperatingStartDate' is set, defaulting to 'assetStartDate' if not present
            if 'OperatingStartDate' not in asset and 'assetStartDate' in asset:
                asset['OperatingStartDate'] = asset['assetStartDate']

            # Use 'constructionStartDate' for the earliest start (preferred)
            if 'constructionStartDate' in asset and asset['constructionStartDate']:
                try:
                    current_start = pd.to_datetime(asset['constructionStartDate'])
                    if current_start < earliest_construction_start:
                        earliest_construction_start = current_start
                except Exception as e:
                    print(f"  ⚠️  Warning: Could not parse constructionStartDate '{asset.get('constructionStartDate')}': {e}")
            
            # Fallback: Use 'OperatingStartDate' (or 'assetStartDate') if constructionStartDate not available
            if 'OperatingStartDate' in asset and asset['OperatingStartDate']:
                try:
                    current_ops_start = pd.to_datetime(asset['OperatingStartDate'])
                    if current_ops_start < earliest_ops_start:
                        earliest_ops_start = current_ops_start
                except Exception as e:
                    print(f"  ⚠️  Warning: Could not parse OperatingStartDate '{asset.get('OperatingStartDate')}': {e}")
            
            # Calculate end date based on OperatingStartDate + assetLife
            if 'OperatingStartDate' in asset and asset['OperatingStartDate'] and 'assetLife' in asset and asset['assetLife']:
                try:
                    ops_start_date = pd.to_datetime(asset['OperatingStartDate'])
                    asset_life_years = int(asset['assetLife'])
                    current_ops_end = ops_start_date + relativedelta(years=asset_life_years)
                    
                    if current_ops_end > latest_ops_end:
                        latest_ops_end = current_ops_end
                except Exception as e:
                    print(f"  ⚠️  Warning: Could not calculate end date from OperatingStartDate + assetLife: {e}")
            elif 'operationsEndDate' in asset and asset['operationsEndDate']:
                try:
                    current_end = pd.to_datetime(asset['operationsEndDate'])
                    if current_end > latest_ops_end:
                        latest_ops_end = current_end
                except Exception as e:
                    print(f"  ⚠️  Warning: Could not parse operationsEndDate '{asset.get('operationsEndDate')}': {e}")

        # Use constructionStartDate if available, otherwise fall back to OperatingStartDate
        if earliest_construction_start != pd.to_datetime('2050-01-01'):
            earliest_asset_start = earliest_construction_start
            print(f"  ✅ Earliest asset constructionStartDate: {earliest_asset_start.strftime('%Y-%m-%d')}")
        elif earliest_ops_start != pd.to_datetime('2050-01-01'):
            earliest_asset_start = earliest_ops_start
            print(f"  ✅ Earliest asset OperatingStartDate (fallback): {earliest_asset_start.strftime('%Y-%m-%d')}")
        else:
            earliest_asset_start = pd.to_datetime('2050-01-01')  # Will trigger error below
        
        # Apply minimum model start date: max(earliest_asset_start, minimum_model_start_date)
        # This ensures price curve data is available and prevents errors
        # Note: minimum_start is already defined above for the user-provided dates case
        start_date = max(earliest_asset_start, minimum_start) if earliest_asset_start != pd.to_datetime('2050-01-01') else minimum_start
        
        if start_date == minimum_start and earliest_asset_start != pd.to_datetime('2050-01-01') and earliest_asset_start < minimum_start:
            print(f"  ⚠️  Note: Earliest asset start date ({earliest_asset_start.strftime('%Y-%m-%d')}) is before minimum model start date ({minimum_start.strftime('%Y-%m-%d')})")
            print(f"  ✅ Model start date set to minimum: {start_date.strftime('%Y-%m-%d')} (max(asset start dates, {minimum_start.strftime('%Y-%m-%d')}))")
        elif earliest_asset_start != pd.to_datetime('2050-01-01'):
            print(f"  ✅ Model start date: {start_date.strftime('%Y-%m-%d')} (max(asset start dates, {minimum_start.strftime('%Y-%m-%d')}))")
        else:
            print(f"  ✅ Model start date set to minimum: {start_date.strftime('%Y-%m-%d')}")
        
        end_date = latest_ops_end

        # Check if we have a valid start date (should not be the future date placeholder)
        if start_date == pd.to_datetime('2050-01-01') or end_date == pd.to_datetime('1900-01-01'):
            # Build detailed error message
            missing_fields = []
            for asset in ASSETS:
                asset_id = asset.get('id', 'unknown')
                asset_name = asset.get('name', f'Asset_{asset_id}')
                asset_missing = []
                if not asset.get('constructionStartDate') and not asset.get('OperatingStartDate') and not asset.get('assetStartDate'):
                    asset_missing.append("constructionStartDate/OperatingStartDate/assetStartDate")
                if not asset.get('assetLife') and not asset.get('operationsEndDate'):
                    asset_missing.append("assetLife/operationsEndDate")
                if asset_missing:
                    missing_fields.append(f"{asset_name}: {', '.join(asset_missing)}")
            
            error_msg = "Could not determine valid model start or end dates from asset data.\n"
            error_msg += "Required fields:\n"
            error_msg += "  - For start date: 'constructionStartDate' (preferred) OR 'OperatingStartDate' OR 'assetStartDate'\n"
            error_msg += "  - For end date: 'OperatingStartDate' + 'assetLife' OR 'operationsEndDate'\n\n"
            if missing_fields:
                error_msg += "Missing fields by asset:\n"
                for missing in missing_fields:
                    error_msg += f"  - {missing}\n"
            if use_asset_start_dates:
                error_msg += "\nAlternatively, uncheck 'Use asset start dates' in Model Defaults and set custom dates."
            else:
                error_msg += "\nAlternatively, check 'Use asset start dates' in Model Defaults to auto-calculate from assets."
            raise ValueError(error_msg)

    print(f"  📅 Model period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    # 1. Calculate Revenue
    log_progress("Calculating revenue timeseries...", 'info')
    print(f"\n  [STEP 1] Calculating Revenue Timeseries")
    print(f"     → Function: calculate_revenue_timeseries() from src/calculations/revenue.py")
    output_directory = os.path.join(project_root, 'output', 'model_results')
    revenue_df = calculate_revenue_timeseries(ASSETS, MONTHLY_PRICES, YEARLY_SPREADS, start_date, end_date, output_directory)
    print(f"     ✅ Revenue calculated: {len(revenue_df)} rows")

    # 2. Calculate OPEX and CAPEX (initial CAPEX with assumed funding split)
    log_progress("Calculating OPEX and CAPEX...", 'info')
    print(f"\n  [STEP 2] Calculating OPEX and CAPEX")
    print(f"     → Function: calculate_opex_timeseries() from src/calculations/opex.py")
    opex_df = calculate_opex_timeseries(ASSETS, ASSET_COST_ASSUMPTIONS, start_date, end_date)
    print(f"     ✅ OPEX calculated: {len(opex_df)} rows")
    print(f"     → Function: calculate_capex_timeseries() from src/calculations/construction_capex.py")
    initial_capex_df = calculate_capex_timeseries(ASSETS, ASSET_COST_ASSUMPTIONS, start_date, end_date, capex_funding_type=default_capex_funding_type)
    print(f"     ✅ CAPEX calculated: {len(initial_capex_df)} rows")

    # 2b. Apply ALL scenario overrides to calculated timeseries (NEW APPROACH)
    if scenario_file:
        print(f"\n  [STEP 2b] Applying Scenario Overrides")
        print(f"     → Function: apply_all_scenarios_to_timeseries() from src/core/scenario_manager.py")
        print(f"     → Scenario file: {scenario_file}")
        revenue_df, opex_df, initial_capex_df, ASSET_COST_ASSUMPTIONS = apply_all_scenarios_to_timeseries(
            revenue_df, opex_df, initial_capex_df, ASSETS, ASSET_COST_ASSUMPTIONS, 
            MONTHLY_PRICES, YEARLY_SPREADS, start_date, end_date, scenario_data
        )
    print(f"     ✅ Scenario overrides applied")

    # 2c. Build auditable per-period inputs table (check later)
    print(f"\n  [STEP 2c] Building Inputs Audit Timeseries (Skipped)")
    # from glassbox.api import router as glassbox_router
    # app.include_router(glassbox_router)

    # Configure CORS
    origins = [
        "http://localhost:3000",
        "http://localhost:3001",
        "https://renew-front.vercel.app"
    ]
    inputs_audit_cols = [
        'asset_id',
        'date',
        'market_price_green_used_$',
        'market_price_black_used_$',
        'storage_market_price_used_$',
        'pct_green_contracted',
        'pct_black_contracted',
        'pct_green_merchant',
        'pct_black_merchant',
        'vol_green_contracted_mwh',
        'vol_black_contracted_mwh',
        'vol_green_merchant_mwh',
        'vol_black_merchant_mwh',
        'monthlyGeneration',
    ]
    # Include dynamic contract columns (contract_1..N...) if present
    contract_cols = [c for c in revenue_df.columns if c.startswith('contract_')]
    existing_cols = [c for c in inputs_audit_cols if c in revenue_df.columns] + contract_cols
    inputs_audit_df = revenue_df[existing_cols].copy() if existing_cols else None
    if inputs_audit_df is None:
        print(f"     ⚠️  Inputs audit skipped (no price columns on revenue_df)")
    else:
        print(f"     ✅ Inputs audit built: {len(inputs_audit_df)} rows")

    # 3. Calculate preliminary CFADS for debt sizing
    print(f"\n  [STEP 3] Calculating Preliminary CFADS")
    print(f"     → Merging revenue and OPEX dataframes")
    prelim_cash_flow = pd.merge(revenue_df, opex_df, on=['asset_id', 'date'])
    prelim_cash_flow['cfads'] = prelim_cash_flow['revenue'] - prelim_cash_flow['opex']
    print(f"     ✅ Preliminary CFADS calculated: {len(prelim_cash_flow)} rows")

    # Calculate Depreciation & Amortization (D&A)
    print(f"\n  [STEP 3b] Calculating Depreciation & Amortization")
    print(f"     → Function: calculate_d_and_a() from src/calculations/depreciation.py")
    d_and_a_df = calculate_d_and_a(initial_capex_df, pd.DataFrame(columns=['asset_id', 'date', 'intangible_capex']), ASSETS, default_asset_life_years, default_asset_life_years, start_date, end_date)
    print(f"     ✅ D&A calculated: {len(d_and_a_df)} rows")

    # 4. Size debt based on operational cash flows and update CAPEX funding
    log_progress("Calculating debt schedule...", 'info')
    print(f"\n  [STEP 4] Calculating Debt Schedule")
    print(f"     → Function: calculate_debt_schedule() from src/calculations/debt.py")
    debt_df, updated_capex_df = calculate_debt_schedule(ASSETS, ASSET_COST_ASSUMPTIONS, initial_capex_df, prelim_cash_flow, start_date, end_date, repayment_frequency=default_debt_repayment_frequency, grace_period=default_debt_grace_period, debt_sizing_method=default_debt_sizing_method, dscr_calculation_frequency=dscr_calculation_frequency)
    print(f"     ✅ Debt schedule calculated: {len(debt_df)} rows")

    # 4b. CAPEX scenarios are now applied BEFORE debt sizing (in STEP 2b)
    # This ensures debt is sized based on the sensitivity-adjusted CAPEX
    # No post-debt-sizing CAPEX adjustment needed

    # 5. Aggregate into Final Cash Flow using updated CAPEX with correct debt/equity split
    log_progress("Aggregating final cash flow...", 'info')
    print(f"\n  [STEP 5] Aggregating Final Cash Flow")
    print(f"     → Function: aggregate_cashflows() from src/calculations/cashflow.py")
    final_cash_flow = aggregate_cashflows(revenue_df, opex_df, updated_capex_df, debt_df, d_and_a_df, end_date, ASSETS, ASSET_COST_ASSUMPTIONS, repayment_frequency=default_debt_repayment_frequency, tax_rate=tax_rate, enable_terminal_value=enable_terminal_value, min_cash_balance_for_distribution=min_cash_balance_for_distribution, start_date=start_date)
    print(f"     ✅ Final cash flow aggregated: {len(final_cash_flow)} rows")

    # Assign period type (Construction or Operations)
    def assign_period_type(df, assets_data, model_start_date):
        """
        Assigns period types ('C' for Construction, 'O' for Operations) to cash flow periods.
        Normalizes asset dates to model_start_date if they are before it, ensuring all
        periods in the model date range are properly classified.
        """
        df['period_type'] = ''
        df['date'] = pd.to_datetime(df['date'])
        model_start = pd.to_datetime(model_start_date)

        for asset_info in assets_data:
            asset_id = asset_info['id']
            
            # Use consistent date field names - only OperatingStartDate
            construction_start = None
            ops_start = None
            
            if 'constructionStartDate' in asset_info and asset_info['constructionStartDate']:
                construction_start = pd.to_datetime(asset_info['constructionStartDate'])
                # Normalize to model start date if before it
                if construction_start < model_start:
                    construction_start = model_start
            
            # Only use OperatingStartDate
            if 'OperatingStartDate' in asset_info and asset_info['OperatingStartDate']:
                ops_start = pd.to_datetime(asset_info['OperatingStartDate'])
                # Normalize to model start date if before it
                if ops_start < model_start:
                    ops_start = model_start
            
            if construction_start and ops_start:
                # If construction was completed before model start, treat as if it ended at model start
                # and operations begin at model start
                if construction_start >= ops_start:
                    # Construction already completed - all periods from model start are operations
                    operations_mask = (df['asset_id'] == asset_id) & (df['date'] >= model_start)
                    df.loc[operations_mask, 'period_type'] = 'O'
                else:
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

    final_cash_flow = assign_period_type(final_cash_flow, ASSETS, start_date)

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
            print(f"{asset_name} (ID: {asset_id}): CAPEX ${total_capex:,.0f}M = Debt ${total_debt:,.0f}M ({gearing:.1%}) + Equity ${total_equity:,.0f}M ({1-gearing:.1%})")
        else:
            print(f"{asset_name} (ID: {asset_id}): No CAPEX")
    
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
    log_progress("Calculating equity IRR...", 'info')
    print(f"\n  [STEP 6] Calculating Equity IRR")
    print(f"     → Function: calculate_equity_irr() from src/core/equity_irr.py")
    
    # Filter cash flows to include Construction ('C') and Operations ('O') periods
    # Also include any periods with terminal value (even if period_type is not set)
    co_periods_df = final_cash_flow[final_cash_flow['period_type'].isin(['C', 'O'])].copy()
    
    # Include periods with terminal value (to ensure terminal value is captured in IRR)
    terminal_value_periods = final_cash_flow[
        (final_cash_flow['terminal_value'] > 0) & 
        (~final_cash_flow.index.isin(co_periods_df.index))
    ].copy()
    
    if not terminal_value_periods.empty:
        print(f"  Including {len(terminal_value_periods)} terminal value period(s) in IRR calculation")
        co_periods_df = pd.concat([co_periods_df, terminal_value_periods], ignore_index=True)
    
    if not co_periods_df.empty:
        # CRITICAL FIX: Use equity_cash_flow_pre_distributions for IRR calculation
        # This represents the cash available to equity before accounting distributions
        # IRR should reflect investor returns, not internal distribution accounting
        
        # Filter for periods with meaningful cash flows:
        # 1. Non-zero equity_cash_flow_pre_distributions (includes all equity contributions and returns)
        # 2. Periods with terminal value (even if net cash flow is zero)
        # 3. Periods with equity_capex (to ensure all equity contributions are captured)
        equity_irr_df = co_periods_df[
            (co_periods_df['equity_cash_flow_pre_distributions'] != 0) | 
            (co_periods_df['terminal_value'] > 0) |
            (co_periods_df.get('equity_capex', 0) != 0)
        ].copy()
        
        # Validation: Verify equity contributions are included
        if 'equity_capex' in co_periods_df.columns:
            total_equity_capex = co_periods_df['equity_capex'].sum()
            equity_capex_in_irr = equity_irr_df['equity_capex'].sum() if 'equity_capex' in equity_irr_df.columns else 0
            if abs(total_equity_capex - equity_capex_in_irr) > 0.01:
                print(f"  ⚠️  WARNING: Equity CAPEX mismatch - Total: ${total_equity_capex:,.2f}M, In IRR: ${equity_capex_in_irr:,.2f}M")
        
        if not equity_irr_df.empty:
            # Group by date to get total equity cash flows across all assets for each date
            equity_irr_summary = equity_irr_df.groupby('date')['equity_cash_flow_pre_distributions'].sum().reset_index()
            
            # Rename column for IRR function compatibility
            equity_irr_summary = equity_irr_summary.rename(columns={'equity_cash_flow_pre_distributions': 'equity_cash_flow'})
            
            # Validate cash flow components
            if 'equity_capex' in equity_irr_df.columns:
                total_equity_invested = equity_irr_df['equity_capex'].sum()
                total_equity_cf = equity_irr_summary['equity_cash_flow'].sum()
                print(f"  Equity invested (CAPEX): ${total_equity_invested:,.2f}M")
                print(f"  Net equity cash flow: ${total_equity_cf:,.2f}M")
            
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
    log_progress("Calculating individual asset IRRs...", 'info')
    print(f"\n  [STEP 7] Calculating Individual Asset IRRs")
    print(f"     → Function: calculate_asset_equity_irrs_fixed() (internal function)")
    
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
            
            # Include periods with terminal value for this asset
            terminal_value_periods = asset_df[
                (asset_df['terminal_value'] > 0) & 
                (~asset_df.index.isin(co_periods_df.index))
            ].copy()
            
            if not terminal_value_periods.empty:
                co_periods_df = pd.concat([co_periods_df, terminal_value_periods], ignore_index=True)

            # Use equity_cash_flow_pre_distributions for consistency
            # Include periods with:
            # 1. Non-zero equity_cash_flow_pre_distributions
            # 2. Terminal value (even if net cash flow is zero)
            # 3. Equity CAPEX (to ensure all equity contributions are captured)
            equity_irr_df = co_periods_df[
                (co_periods_df['equity_cash_flow_pre_distributions'] != 0) | 
                (co_periods_df['terminal_value'] > 0) |
                (co_periods_df.get('equity_capex', 0) != 0)
            ].copy()

            if not equity_irr_df.empty:
                # Group by date and sum equity cash flows (pre-distributions)
                equity_irr_summary = equity_irr_df.groupby('date')['equity_cash_flow_pre_distributions'].sum().reset_index()
                
                # Rename for function compatibility
                equity_irr_summary = equity_irr_summary.rename(columns={'equity_cash_flow_pre_distributions': 'equity_cash_flow'})
                
                # Validation: Check if equity contributions are properly included
                if 'equity_capex' in equity_irr_df.columns:
                    asset_equity_capex = equity_irr_df['equity_capex'].sum()
                    if asset_equity_capex > 0.01:  # Only warn if significant equity investment
                        asset_net_cf = equity_irr_summary['equity_cash_flow'].sum()
                        # Equity cash flow should be negative during construction (equity contributions)
                        # and positive during operations (returns)
                        if asset_net_cf > asset_equity_capex * 0.5:  # If net CF is more than 50% of equity invested
                            asset_name = next((a.get('name', f'Asset_{asset_id}') for a in ASSETS if a.get('id') == asset_id), f'Asset_{asset_id}')
                            print(f"    ⚠️  Asset {asset_id} ({asset_name}): Net CF (${asset_net_cf:,.2f}M) seems high vs equity invested (${asset_equity_capex:,.2f}M)")
                
                irr = calculate_equity_irr(equity_irr_summary)
                asset_irrs[asset_id] = irr
                asset_name = next((a.get('name', f'Asset_{asset_id}') for a in ASSETS if a.get('id') == asset_id), f'Asset_{asset_id}')
                print(f"  Asset {asset_id} ({asset_name}) IRR: {irr:.2%}" if not pd.isna(irr) else f"  Asset {asset_id} ({asset_name}) IRR: Could not calculate")
            else:
                asset_irrs[asset_id] = float('nan')
                asset_name = next((a.get('name', f'Asset_{asset_id}') for a in ASSETS if a.get('id') == asset_id), f'Asset_{asset_id}')
                print(f"  Asset {asset_id} ({asset_name}) IRR: No equity cash flows found")

        return asset_irrs
    
    # Use the fixed function
    asset_irrs = calculate_asset_equity_irrs_fixed(final_cash_flow)

    # Add hybrid asset combinations to cashflow
    final_cash_flow = add_hybrid_asset_summaries(final_cash_flow, ASSETS, asset_irrs)

    # Calculate missing variables for the summary function
    asset_type_map = {asset['id']: asset.get('assetType', 'unknown') for asset in ASSETS}

    # Generate summary data
    summary_data = generate_summary_data(final_cash_flow)

    # Save to files
    log_progress("Saving outputs to files...", 'info')
    print(f"\n  [STEP 8] Saving Outputs to Files")
    print(f"     → Function: generate_asset_and_platform_output() from src/core/output_generator.py")
    generate_asset_and_platform_output(final_cash_flow, irr, output_directory, scenario_id=scenario_id, inputs_audit_df=inputs_audit_df)
    print(f"     ✅ Outputs saved to: {output_directory}")

    # === CRITICAL: WRITE TO MONGODB ===
    log_progress("Writing results to MongoDB...", 'info')
    print(f"\n  [STEP 9] Writing to MongoDB")
    print(f"     → Function: insert_dataframe_to_mongodb() from src/core/database.py")
    print(f"Debug: replace_data={replace_data}, scenario_id={scenario_id}")
    try:
        # Handle data replacement
        if replace_data:
            if scenario_id:
                print(f"Clearing existing data for scenario: {scenario_id}, portfolio unique_id: {portfolio_unique_id}")
                clear_all_scenario_data(scenario_id, portfolio_unique_id=portfolio_unique_id)
            else:
                print(f"Clearing existing base case data for portfolio unique_id: {portfolio_unique_id}")
                clear_base_case_data(portfolio_unique_id=portfolio_unique_id)
        else:
            print(f"Appending new data (replace_data=False)")

        # Write main cash flow data with replace option
        print("Writing main cash flow data to MongoDB...")
        # Add portfolio_name to DataFrame
        final_cash_flow['portfolio'] = portfolio_name
        
        # Add portfolio unique_id to DataFrame (same value for all rows of the same portfolio)
        if portfolio_unique_id:
            final_cash_flow['unique_id'] = portfolio_unique_id
            print(f"Added portfolio unique_id to cash flow data: {portfolio_unique_id}")
        else:
            print(f"⚠️  Warning: Portfolio unique_id not provided, skipping unique_id column")
        
        print(f"Tagging results with portfolio: {portfolio_name}")
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

    def generate_asset_inputs_summary(assets, asset_cost_assumptions, config_values, debt_summary, output_dir, irr_value, asset_irrs, asset_type_map, total_portfolio_capex, total_portfolio_debt, portfolio_gearing, portfolio_name=None, scenario_id=None, portfolio_unique_id=None):
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
                
                # Add portfolio field (use 'portfolio' for consistency with database operations)
                if portfolio_name:
                    flat_asset_data['portfolio'] = portfolio_name
                
                # Add portfolio unique_id (same value for all assets in the portfolio)
                if portfolio_unique_id:
                    flat_asset_data['unique_id'] = portfolio_unique_id
                
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
            
            # Sheet 6: WIP - Build Excel Model
            wip_df = pd.DataFrame()  # Empty DataFrame as placeholder
            wip_df.to_excel(writer, sheet_name='WIP - Build Excel Model', index=False)
            
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
        generate_asset_inputs_summary(ASSETS, ASSET_COST_ASSUMPTIONS, config_values, debt_summary, output_directory, irr, asset_irrs, asset_type_map, total_portfolio_capex, total_portfolio_debt, portfolio_gearing, portfolio_name=portfolio_name, scenario_id=scenario_id, portfolio_unique_id=portfolio_unique_id)

    # Generate Asset Output Summary
    generate_asset_output_summary(final_cash_flow, irr, asset_irrs, ASSETS, updated_capex_df, scenario_id=scenario_id, portfolio_unique_id=portfolio_unique_id)

    print("\n" + "="*80)
    print("✅ CASHFLOW MODEL COMPLETE")
    print("="*80)
    print(f"  📊 Equity IRR: {irr:.2%}" if not pd.isna(irr) else "  ⚠️  Equity IRR: Could not calculate")
    print(f"  💾 All data successfully written to MongoDB!")
    print(f"  📁 Output files saved to: {output_directory}")
    print("="*80 + "\n")
    
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
        # Fetch price data from MongoDB instead of legacy CSV files
        client = get_mongo_client()
        if client is None:
            raise ValueError("Could not obtain MongoDB client. Check MONGODB_URI / MONGODB_DB in .env.local.")

        # Determine database name from environment (loaded by src.core.database)
        mongo_db_name = os.getenv('MONGODB_DB')
        if not mongo_db_name:
            raise ValueError("MONGODB_DB not set in environment. Please configure it in .env.local.")

        db = client[mongo_db_name]

        # Choose a price curve: prefer AC Nov 2024 if present, otherwise fall back to the latest name
        available_curves = get_price_curves_list(db)
        if not available_curves:
            raise ValueError("No price curves found in MongoDB collection PRICE_Curves_2.")

        preferred_curve = 'AC Nov 2024'
        if preferred_curve in available_curves:
            curve_name = preferred_curve
        else:
            available_curves_sorted = sorted(available_curves)
            curve_name = available_curves_sorted[-1]

        print(f"Using price curve for CLI run: {curve_name}")
        monthly_prices, yearly_spreads = load_price_data_from_mongo(db, curve_name)

        # Load asset configuration from MongoDB
        config_data = get_data_from_mongodb('CONFIG_Inputs')
        if not config_data:
            raise ValueError("Could not load config data from MongoDB")
        assets = config_data[0].get('asset_inputs', [])
        # Use PortfolioTitle (user-editable display name) with fallback to PlatformName
        portfolio_name = config_data[0].get('PortfolioTitle') or config_data[0].get('PlatformName')
        if not portfolio_name:
            raise ValueError("Could not find PortfolioTitle or PlatformName in config data from MongoDB")

        final_cashflows_json = run_cashflow_model(
            assets=assets,
            monthly_prices=monthly_prices,
            yearly_spreads=yearly_spreads,
            portfolio_name=portfolio_name,
            scenario_file=args.scenario, 
            scenario_id=args.scenario_id,
            replace_data=replace_data
        )

        if args.run_sensitivity:
            print("\n=== RUNNING SENSITIVITY ANALYSIS ===")
            from scripts.run_sensitivity_analysis import run_sensitivity_analysis_improved
            run_sensitivity_analysis_improved()

        print(final_cashflows_json)