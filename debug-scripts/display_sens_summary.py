import sys
import os
import pandas as pd

# Add the project root to the system path to allow importing src
script_dir = os.path.dirname(__file__)
project_root = os.path.abspath(os.path.join(script_dir, '..'))
sys.path.insert(0, project_root)

from src.core.database import get_data_from_mongodb, database_lifecycle

def display_sens_summary():
    """
    Retrieves all data from the SENS_Summary_Main collection
    and prints it as a formatted table.
    """
    print("Attempting to retrieve data from SENS_Summary_Main collection...")
    try:
        with database_lifecycle():
            data = get_data_from_mongodb("SENS_Summary_Main")
            
            if data:
                df = pd.DataFrame(data)
                print(f"\nAll columns in DataFrame: {df.columns.tolist()}")
                # Drop the MongoDB _id and scenario_id columns as they're not relevant for display
                columns_to_drop = ['_id', 'scenario_id']
                for col in columns_to_drop:
                    if col in df.columns:
                        df = df.drop(columns=[col])
                
                # Get a comprehensive list of all asset IRR and diff columns from the full DataFrame
                all_asset_irr_cols = sorted([col for col in df.columns if col.startswith('asset_') and col.endswith('_irr_pct')])
                all_asset_diff_cols = sorted([col for col in df.columns if col.startswith('asset_') and col.endswith('_irr_diff_bps')])
                
                # Define base columns to always include
                base_display_cols = ['parameter_name', 'parameter_units', 'input_value',
                                     'portfolio_irr_pct', 'portfolio_irr_diff_bps',
                                     'portfolio_gearing_pct', 'total_capex_m', 'total_revenue_m']

                # Get unique parameters
                unique_parameters = df['parameter'].unique()

                for param in unique_parameters:
                    print(f"\n--- SENS_Summary_Main Data for Parameter: {param} ---")
                    param_df = df[df['parameter'] == param].copy() # Use .copy() to avoid SettingWithCopyWarning
                    
                    # Determine columns to display for this parameter, ensuring order
                    current_display_cols = [col for col in base_display_cols if col in param_df.columns]
                    current_asset_irr_cols = [col for col in all_asset_irr_cols if col in param_df.columns]
                    current_asset_diff_cols = [col for col in all_asset_diff_cols if col in param_df.columns]
                    
                    # Combine and reindex to ensure consistent column order and presence
                    final_cols_order = current_display_cols + current_asset_irr_cols + current_asset_diff_cols
                    param_df = param_df.reindex(columns=final_cols_order)

                    # Drop the 'parameter' column after filtering
                    if 'parameter' in param_df.columns:
                        param_df = param_df.drop(columns=['parameter'])

                    print(param_df.to_string())
                    print("----------------------------------------------------")
                    
                    print(param_df.to_string())
                    print("----------------------------------------------------")
            else:
                print("No data found in SENS_Summary_Main collection.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    display_sens_summary()
