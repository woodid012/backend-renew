# src/core/output_generator.py

import pandas as pd
import os
import numpy as np
import numpy as np
from ..config import OUTPUT_DATE_FORMAT

def generate_asset_and_platform_output(final_cash_flow_df, irr_value, output_dir='output/model_results', scenario_id=None, inputs_audit_df=None):
    """
    Generates asset-specific and aggregated platform cash flow outputs.

    Args:
        final_cash_flow_df (pd.DataFrame): The consolidated cash flow DataFrame.
        irr_value (float): The calculated IRR value.
        output_dir (str): The base directory where output files will be saved.
        scenario_id (str, optional): Scenario identifier for organizing outputs into subdirectories.
        inputs_audit_df (pd.DataFrame, optional): Monthly audit table of raw + used inputs by period.
    """
    # Determine the actual output directory based on scenario_id
    if scenario_id:
        actual_output_dir = os.path.join(output_dir, 'scenarios', scenario_id)
        print(f"Scenario-specific output directory: {actual_output_dir}")
    else:
        actual_output_dir = output_dir
        print(f"Base case output directory: {actual_output_dir}")

    # Ensure output directory exists
    if not os.path.exists(actual_output_dir):
        os.makedirs(actual_output_dir)
        print(f"Created output directory: {actual_output_dir}")

    # Ensure 'date' column is in datetime format
    if not pd.api.types.is_datetime64_any_dtype(final_cash_flow_df['date']):
        final_cash_flow_df['date'] = pd.to_datetime(final_cash_flow_df['date'])

    if inputs_audit_df is not None and not inputs_audit_df.empty:
        if 'date' in inputs_audit_df.columns and not pd.api.types.is_datetime64_any_dtype(inputs_audit_df['date']):
            inputs_audit_df['date'] = pd.to_datetime(inputs_audit_df['date'])

    # Get unique asset IDs
    asset_ids = final_cash_flow_df['asset_id'].unique()

    # 1. Save each asset's cash flow
    for asset_id in asset_ids:
        asset_df = final_cash_flow_df[final_cash_flow_df['asset_id'] == asset_id].copy()
        asset_output_path = os.path.join(actual_output_dir, f"asset_{asset_id}.xlsx")
        with pd.ExcelWriter(asset_output_path, engine='openpyxl') as writer:
            asset_df.to_excel(writer, sheet_name='Cash Flow', index=False)

            if inputs_audit_df is not None and not inputs_audit_df.empty:
                inputs_df = inputs_audit_df[inputs_audit_df['asset_id'] == asset_id].copy()
                if not inputs_df.empty:
                    # Keep audit table friendly: date first, then market, then contracts
                    date_cols = [c for c in ['asset_id', 'date', 'profile', 'region'] if c in inputs_df.columns]
                    market_cols = [c for c in inputs_df.columns if c.startswith('market_price_')]
                    contract_cols = [c for c in inputs_df.columns if c.startswith('contract_')]
                    other_cols = [c for c in inputs_df.columns if c not in set(date_cols + market_cols + contract_cols)]
                    ordered_cols = date_cols + market_cols + contract_cols + other_cols
                    inputs_df = inputs_df.reindex(columns=ordered_cols)

                inputs_df.to_excel(writer, sheet_name='Inputs by Period', index=False)

        print(f"Saved cash flow for asset {asset_id} to {asset_output_path}")

    # 2. Create and save combined platform cash flow
    # Sum all financial columns, keeping 'date'
    platform_cash_flow_df = final_cash_flow_df.groupby('date').sum(numeric_only=True).reset_index()
    platform_cash_flow_df['irr'] = irr_value
    
    # Recalculate DSCR for the platform level if needed, or remove if not applicable
    # For simplicity, we'll just sum the financial metrics. DSCR would need careful re-calculation.
    if 'dscr' in platform_cash_flow_df.columns:
        # DSCR at platform level is complex and usually not a simple sum.
        # For now, we'll drop it or recalculate based on platform CFADS and Debt Service
        # For a simple sum, it's better to drop it or mark as NaN
        platform_cash_flow_df.drop(columns=['dscr'], inplace=True)
    
    platform_output_path = os.path.join(actual_output_dir, "assets_combined.xlsx")
    platform_cash_flow_df.to_excel(platform_output_path, index=False)
    print(f"Saved combined platform cash flow to {platform_output_path}")

    return platform_cash_flow_df

def export_three_way_financials_to_excel(final_cash_flow_df, output_dir='output/model_results', scenario_id=None):
    """
    Exports P&L, Cash Flow Statement, and Balance Sheet to a single Excel file with multiple sheets.
    """
    # Determine the actual output directory based on scenario_id
    if scenario_id:
        actual_output_dir = os.path.join(output_dir, 'scenarios', scenario_id)
    else:
        actual_output_dir = output_dir

    output_path = os.path.join(actual_output_dir, "three_way_financials.xlsx")

    # Ensure output directory exists
    os.makedirs(actual_output_dir, exist_ok=True)

    # Ensure 'date' column is in datetime format
    if not pd.api.types.is_datetime64_any_dtype(final_cash_flow_df['date']):
        final_cash_flow_df['date'] = pd.to_datetime(final_cash_flow_df['date'])

    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # P&L Statement
        pnl_cols = ['date', 'asset_id', 'revenue', 'opex', 'd_and_a', 'ebit', 'interest', 'ebt', 'tax_expense', 'net_income']
        pnl_df = final_cash_flow_df[pnl_cols].copy()
        pnl_df.to_excel(writer, sheet_name='P&L', index=False)

        # Cash Flow Statement
        # Note: 'cfads' is simplified Cash from Operations. 'equity_cash_flow' is the net cash flow.
        cf_cols = ['date', 'asset_id', 'net_income', 'd_and_a', 'cfads', 'capex', 'principal', 'equity_capex', 'equity_cash_flow', 'distributions', 'dividends', 'redistributed_capital']
        cf_df = final_cash_flow_df[cf_cols].copy()
        cf_df.to_excel(writer, sheet_name='Cash Flow Statement', index=False)

        # Balance Sheet
        bs_cols = ['date', 'asset_id', 'cash', 'fixed_assets', 'total_assets', 'debt', 'share_capital', 'retained_earnings', 'equity', 'total_liabilities', 'net_assets']
        bs_df = final_cash_flow_df[bs_cols].copy()
        # Round numerical columns to 2 decimal places
        for col in bs_df.select_dtypes(include=np.number).columns:
            bs_df[col] = bs_df[col].round(2)
        bs_df.to_excel(writer, sheet_name='Balance Sheet', index=False)
    
    print(f"Saved 3-Way Financials to {output_path}")