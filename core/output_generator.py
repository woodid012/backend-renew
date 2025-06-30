import pandas as pd
import os
from config import OUTPUT_DATE_FORMAT

def generate_asset_and_platform_output(final_cash_flow_df, irr_value, output_dir='results'):
    """
    Generates asset-specific and aggregated platform cash flow outputs.

    Args:
        final_cash_flow_df (pd.DataFrame): The consolidated cash flow DataFrame.
        output_dir (str): The directory where output files will be saved.
    """
    # Ensure output directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Ensure 'date' column is in datetime format
    if not pd.api.types.is_datetime64_any_dtype(final_cash_flow_df['date']):
        final_cash_flow_df['date'] = pd.to_datetime(final_cash_flow_df['date'])

    # Ensure 'date' column is in a serializable format (e.g., string)
    final_cash_flow_df['date'] = final_cash_flow_df['date'].dt.strftime(OUTPUT_DATE_FORMAT)

    # Get unique asset IDs
    asset_ids = final_cash_flow_df['asset_id'].unique()

    # 1. Save each asset's cash flow
    for asset_id in asset_ids:
        asset_df = final_cash_flow_df[final_cash_flow_df['asset_id'] == asset_id].copy()
        asset_output_path = os.path.join(output_dir, f"asset_{asset_id}.json")
        asset_df.to_json(asset_output_path, orient='records', indent=4)
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
    
    platform_output_path = os.path.join(output_dir, "assets_combined.json")
    platform_cash_flow_df.to_json(platform_output_path, orient='records', indent=4)
    print(f"Saved combined platform cash flow to {platform_output_path}")

    return platform_cash_flow_df
