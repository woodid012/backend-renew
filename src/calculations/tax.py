import pandas as pd

def calculate_tax_expense(cash_flow_df, tax_rate):
    """
    Calculates tax expense considering accumulated tax losses.
    
    Args:
        cash_flow_df (pd.DataFrame): DataFrame containing 'ebt' and 'asset_id'.
        tax_rate (float): The tax rate to apply.

    Returns:
        pd.DataFrame: Original DataFrame with 'tax_expense' and 'accumulated_tax_losses' columns added/updated.
    """
    
    # Ensure 'ebt' is numeric and fill NaNs with 0
    cash_flow_df['ebt'] = pd.to_numeric(cash_flow_df['ebt'], errors='coerce').fillna(0)
    
    # Initialize accumulated tax losses for each asset
    cash_flow_df['accumulated_tax_losses'] = 0.0
    cash_flow_df['tax_expense'] = 0.0

    # Sort by asset_id and date to ensure correct cumulative calculation
    cash_flow_df = cash_flow_df.sort_values(by=['asset_id', 'date']).reset_index(drop=True)

    for asset_id in cash_flow_df['asset_id'].unique():
        asset_df = cash_flow_df[cash_flow_df['asset_id'] == asset_id].copy()
        
        current_accumulated_tax_losses = 0.0
        
        for i, row in asset_df.iterrows():
            ebt = row['ebt']
            
            if ebt < 0:
                # Add negative EBT to accumulated tax losses
                current_accumulated_tax_losses += abs(ebt)
                tax_expense = 0.0
            else:
                # Positive EBT, try to offset with accumulated tax losses
                if current_accumulated_tax_losses > 0:
                    offset_amount = min(ebt, current_accumulated_tax_losses)
                    ebt_after_offset = ebt - offset_amount
                    current_accumulated_tax_losses -= offset_amount
                else:
                    ebt_after_offset = ebt
                
                # Calculate tax on remaining positive EBT
                tax_expense = ebt_after_offset * tax_rate
            
            # Update the original DataFrame for the specific asset and date
            cash_flow_df.loc[(cash_flow_df['asset_id'] == asset_id) & (cash_flow_df['date'] == row['date']), 'accumulated_tax_losses'] = current_accumulated_tax_losses
            cash_flow_df.loc[(cash_flow_df['asset_id'] == asset_id) & (cash_flow_df['date'] == row['date']), 'tax_expense'] = tax_expense
            
    return cash_flow_df
