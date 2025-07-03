
import pandas as pd
import json
import os
from datetime import datetime

def process_aurora_prices():
    """
    Reads the Aurora_May.xlsx file and processes it into a structured JSON time-series file.

    The output JSON contains a list of records, each with:
    - TIME: The first day of the month (YYYY-MM-DD).
    - REGION: The electricity market region (e.g., NSW, QLD).
    - TYPE: The price type (ENERGY or GREEN).
    - PRICE: The monthly price for that type.
    - SPREAD: A dictionary of spreads for different durations (0.5HR, 1HR, 2HR, 4HR).
    """
    # Define file paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    raw_inputs_dir = os.path.join(script_dir, '..', 'data', 'raw_inputs')
    processed_outputs_dir = os.path.join(script_dir, '..', 'data', 'processed_inputs')

    aurora_path = os.path.join(raw_inputs_dir, 'Aurora_May.xlsx')
    output_path = os.path.join(processed_outputs_dir, 'aurora_prices_processed.json')

    print(f"Reading Aurora data from: {aurora_path}")

    try:
        # Read all necessary parts of the Excel file
        xls = pd.ExcelFile(aurora_path)
        # Assuming the data is in the first sheet
        sheet_name = xls.sheet_names[0]

        # Read data rows
        df_cy = pd.read_excel(xls, sheet_name=sheet_name, header=None, skiprows=1, nrows=1)
        df_green = pd.read_excel(xls, sheet_name=sheet_name, header=None, skiprows=2, nrows=1)
        df_month = pd.read_excel(xls, sheet_name=sheet_name, header=None, skiprows=8, nrows=1)
        df_baseload = pd.read_excel(xls, sheet_name=sheet_name, header=None, skiprows=9, nrows=5)
        df_solar = pd.read_excel(xls, sheet_name=sheet_name, header=None, skiprows=14, nrows=5)
        df_wind = pd.read_excel(xls, sheet_name=sheet_name, header=None, skiprows=20, nrows=5)
        df_spreads = pd.read_excel(xls, sheet_name=sheet_name, header=None, skiprows=26, nrows=5)

    except FileNotFoundError:
        print(f"Error: Input file not found at {aurora_path}")
        return

    # --- Data Processing ---

    # Create a date range from the CY and Month rows
    years = df_cy.iloc[0].ffill().astype(int)
    months = df_month.iloc[0].tolist()
    dates = [datetime(year, month, 1) for year, month in zip(years, months)]

    # Process price data
    def process_profile(df, profile_name):
        df.columns = ['REGION'] + dates
        df.set_index('REGION', inplace=True)
        df = df.T.melt(ignore_index=False, var_name='REGION', value_name='PRICE').reset_index()
        df.rename(columns={'index': 'TIME'}, inplace=True)
        df['PROFILE'] = profile_name
        df['TYPE'] = 'ENERGY'
        return df

    baseload_df = process_profile(df_baseload, 'baseload')
    solar_df = process_profile(df_solar, 'solar')
    wind_df = process_profile(df_wind, 'wind')

    # Process green prices
    green_prices = df_green.iloc[0].tolist()
    green_df = pd.DataFrame({'TIME': dates, 'PRICE': green_prices})
    green_df['TYPE'] = 'GREEN'
    green_df['PROFILE'] = 'all' # Green price is common

    # Process spreads
    df_spreads.columns = ['REGION', 'FY'] + [f'{h}HR' for h in [0.5, 1, 2, 4]]
    df_spreads.set_index(['REGION', 'FY'], inplace=True)
    spreads_dict = df_spreads.to_dict(orient='index')

    # --- Data Structuring ---

    final_data = []
    all_prices = pd.concat([baseload_df, solar_df, wind_df])

    for _, row in all_prices.iterrows():
        fy = row['TIME'].year if row['TIME'].month < 7 else row['TIME'].year + 1
        region = row['REGION']
        profile = row['PROFILE']
        
        spread_dict_out = {}
        if profile == 'baseload':
            spread_key = (region, f'FY{fy}')
            if spread_key in spreads_dict:
                spread_dict_out = spreads_dict[spread_key]

        record = {
            'PROFILE': profile,
            'TIME': row['TIME'].strftime('%Y-%m-%d'),
            'REGION': region,
            'TYPE': 'ENERGY',
            'PRICE': row['PRICE'],
            'SPREAD': spread_dict_out
        }
        final_data.append(record)

    # Add green prices for each region and profile
    for _, row in green_df.iterrows():
        for region in all_prices['REGION'].unique():
             for profile in all_prices['PROFILE'].unique():
                final_data.append({
                    'PROFILE': profile,
                    'TIME': row['TIME'].strftime('%Y-%m-%d'),
                    'REGION': region,
                    'TYPE': 'GREEN',
                    'PRICE': row['PRICE'],
                    'SPREAD': {}
                })

    # --- Save Output ---

    os.makedirs(processed_outputs_dir, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(final_data, f, indent=2)

    print(f"Successfully processed Aurora data and saved to: {output_path}")

if __name__ == '__main__':
    process_aurora_prices()
