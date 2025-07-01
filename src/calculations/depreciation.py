import pandas as pd
from ..config import DEFAULT_ASSET_LIFE_YEARS

def calculate_straight_line_depreciation(capex_df, asset_life_years, model_start_date, model_end_date):
    """
    Calculates straight-line depreciation for each asset based on initial CAPEX.

    Args:
        capex_df (pd.DataFrame): DataFrame containing CAPEX by asset and date.
                                 Expected columns: 'asset_id', 'date', 'capex'.
        asset_life_years (int): The useful life of the asset in years for depreciation.
        model_start_date (datetime): The start date of the financial model.
        model_end_date (datetime): The end date of the financial model.

    Returns:
        pd.DataFrame: A DataFrame with monthly depreciation for each asset.
                      Columns: 'asset_id', 'date', 'depreciation'.
    """
    depreciation_records = []
    monthly_depreciation_rate = 1 / (asset_life_years * 12)

    # Ensure capex_df has datetime objects for 'date'
    capex_df['date'] = pd.to_datetime(capex_df['date'])

    for _, row in capex_df.iterrows():
        asset_id = row['asset_id']
        capex_date = row['date']
        capex_amount = row['capex']

        if capex_amount > 0:
            # Depreciation starts the month after CAPEX is incurred
            depreciation_start_date = capex_date + pd.DateOffset(months=1)
            depreciation_end_date = depreciation_start_date + pd.DateOffset(years=asset_life_years) - pd.DateOffset(days=1)

            # Adjust depreciation end date to not exceed model end date
            if depreciation_end_date > model_end_date:
                depreciation_end_date = model_end_date
            
            # Generate monthly periods for depreciation
            depreciation_periods = pd.date_range(start=depreciation_start_date, end=depreciation_end_date, freq='MS')
            
            for period_start in depreciation_periods:
                # Ensure depreciation does not go beyond the model end date
                if period_start <= model_end_date:
                    depreciation_records.append({
                        'asset_id': asset_id,
                        'date': period_start,
                        'depreciation': capex_amount * monthly_depreciation_rate
                    })

    depreciation_df = pd.DataFrame(depreciation_records)
    if not depreciation_df.empty:
        # Aggregate by asset_id and date to handle multiple capex events in a month
        depreciation_df = depreciation_df.groupby(['asset_id', 'date'])['depreciation'].sum().reset_index()
    
    # Fill any missing months within the model period with 0 depreciation
    # Create a full date range for the model period
    full_date_range = pd.date_range(start=model_start_date, end=model_end_date, freq='MS')
    
    # Get all unique asset_ids from the original capex_df
    all_asset_ids = capex_df['asset_id'].unique()

    # Create a MultiIndex for all possible asset_id and date combinations
    if not depreciation_df.empty:
        # Use existing asset_ids if depreciation_df is not empty
        idx = pd.MultiIndex.from_product([depreciation_df['asset_id'].unique(), full_date_range], names=['asset_id', 'date'])
    else:
        # If depreciation_df is empty, use all_asset_ids from capex_df
        idx = pd.MultiIndex.from_product([all_asset_ids, full_date_range], names=['asset_id', 'date'])

    depreciation_df = depreciation_df.set_index(['asset_id', 'date']).reindex(idx, fill_value=0).reset_index()

    return depreciation_df
