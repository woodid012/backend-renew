import pandas as pd
from ..config import DEFAULT_ASSET_LIFE_YEARS
pd.set_option('future.no_silent_downcasting', True)

def calculate_depreciation(capex_df, assets_data, asset_life_years, model_start_date, model_end_date):
    """
    Calculates straight-line depreciation for tangible assets based on initial CAPEX.
    """
    depreciation_records = []
    monthly_depreciation_rate = 1 / (asset_life_years * 12)

    asset_op_start_dates = {asset['id']: pd.to_datetime(asset['OperatingStartDate']) for asset in assets_data}

    for _, row in capex_df.iterrows():
        asset_id = row['asset_id']
        capex_amount = row['capex']

        if capex_amount > 0:
            depreciation_start_date = asset_op_start_dates.get(asset_id)
            
            if depreciation_start_date is None:
                print(f"Warning: OperatingStartDate not found for asset_id {asset_id}. Skipping depreciation for this CAPEX.")
                continue

            if depreciation_start_date < model_start_date:
                depreciation_start_date = model_start_date

            depreciation_end_date = depreciation_start_date + pd.DateOffset(years=asset_life_years) - pd.DateOffset(days=1)

            if depreciation_end_date > model_end_date:
                depreciation_end_date = model_end_date
            
            depreciation_periods = pd.date_range(start=depreciation_start_date, end=depreciation_end_date, freq='MS')
            
            for period_start in depreciation_periods:
                if period_start <= model_end_date:
                    depreciation_records.append({
                        'asset_id': asset_id,
                        'date': period_start,
                        'depreciation': capex_amount * monthly_depreciation_rate
                    })

    depreciation_df = pd.DataFrame(depreciation_records)
    if not depreciation_df.empty:
        depreciation_df = depreciation_df.groupby(['asset_id', 'date'])['depreciation'].sum().reset_index()
    
    full_date_range = pd.date_range(start=model_start_date, end=model_end_date, freq='MS')
    all_asset_ids = capex_df['asset_id'].unique()

    if not depreciation_df.empty:
        idx = pd.MultiIndex.from_product([depreciation_df['asset_id'].unique(), full_date_range], names=['asset_id', 'date'])
    else:
        idx = pd.MultiIndex.from_product([all_asset_ids, full_date_range], names=['asset_id', 'date'])

    depreciation_df = depreciation_df.set_index(['asset_id', 'date']).reindex(idx, fill_value=0).reset_index()

    return depreciation_df

def calculate_amortization(intangible_capex_df, assets_data, intangible_life_years, model_start_date, model_end_date):
    """
    Calculates straight-line amortization for intangible assets.
    """
    amortization_records = []
    monthly_amortization_rate = 1 / (intangible_life_years * 12)

    asset_op_start_dates = {asset['id']: pd.to_datetime(asset['OperatingStartDate']) for asset in assets_data}

    for _, row in intangible_capex_df.iterrows():
        asset_id = row['asset_id']
        intangible_capex_amount = row['intangible_capex']

        if intangible_capex_amount > 0:
            amortization_start_date = asset_op_start_dates.get(asset_id)
            
            if amortization_start_date is None:
                print(f"Warning: OperatingStartDate not found for asset_id {asset_id}. Skipping amortization for this intangible CAPEX.")
                continue

            if amortization_start_date < model_start_date:
                amortization_start_date = model_start_date

            amortization_end_date = amortization_start_date + pd.DateOffset(years=intangible_life_years) - pd.DateOffset(days=1)

            if amortization_end_date > model_end_date:
                amortization_end_date = model_end_date
            
            amortization_periods = pd.date_range(start=amortization_start_date, end=amortization_end_date, freq='MS')
            
            for period_start in amortization_periods:
                if period_start <= model_end_date:
                    amortization_records.append({
                        'asset_id': asset_id,
                        'date': period_start,
                        'amortization': intangible_capex_amount * monthly_amortization_rate
                    })

    amortization_df = pd.DataFrame(amortization_records)
    if amortization_df.empty:
        # If no amortization records, create an empty DataFrame with the expected columns
        amortization_df = pd.DataFrame(columns=['asset_id', 'date', 'amortization'])
    else:
        amortization_df = amortization_df.groupby(['asset_id', 'date'])['amortization'].sum().reset_index()
    
    full_date_range = pd.date_range(start=model_start_date, end=model_end_date, freq='MS')
    all_asset_ids = intangible_capex_df['asset_id'].unique()

    # Create a MultiIndex for all possible asset_id and date combinations
    # Use all_asset_ids from intangible_capex_df to ensure all assets are covered
    idx = pd.MultiIndex.from_product([all_asset_ids, full_date_range], names=['asset_id', 'date'])

    amortization_df = amortization_df.set_index(['asset_id', 'date']).reindex(idx, fill_value=0).reset_index()

    return amortization_df

def calculate_d_and_a(capex_df, intangible_capex_df, assets_data, asset_life_years, intangible_life_years, model_start_date, model_end_date):
    """
    Calculates total Depreciation and Amortization (D&A).
    """
    depreciation_df = calculate_depreciation(capex_df, assets_data, asset_life_years, model_start_date, model_end_date)
    amortization_df = calculate_amortization(intangible_capex_df, assets_data, intangible_life_years, model_start_date, model_end_date)

    # Merge depreciation and amortization
    d_and_a_df = pd.merge(depreciation_df, amortization_df, on=['asset_id', 'date'], how='outer').fillna(0)
    d_and_a_df['d_and_a'] = d_and_a_df['depreciation'] + d_and_a_df['amortization']

    return d_and_a_df[['asset_id', 'date', 'd_and_a']]
