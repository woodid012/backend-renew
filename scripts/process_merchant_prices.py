import pandas as pd
import json
import os

def process_merchant_prices():
    """
    Reads monthly merchant prices and yearly spreads, then combines them into a single
    structured JSON time-series file.

    The output JSON contains a list of records, each with:
    - TIME: The first day of the month (YYYY-MM-DD).
    - REGION: The electricity market region (e.g., NSW, QLD).
    - TYPE: The price type (ENERGY or GREEN).
    - PRICE: The monthly price for that type.
    - SPREAD: A dictionary of spreads for different durations (1HR, 2HR, 4HR, 8HR).
    """
    # Define file paths relative to the script location
    script_dir = os.path.dirname(os.path.abspath(__file__))
    raw_inputs_dir = os.path.join(script_dir, '..', 'data', 'raw_inputs')
    processed_outputs_dir = os.path.join(script_dir, '..', 'data', 'processed_inputs')

    monthly_prices_path = os.path.join(raw_inputs_dir, 'merchant_price_monthly.csv')
    yearly_spreads_path = os.path.join(raw_inputs_dir, 'merchant_yearly_spreads.csv')
    output_path = os.path.join(processed_outputs_dir, 'merchant_prices_processed.json')

    print(f"Reading monthly prices from: {monthly_prices_path}")
    print(f"Reading yearly spreads from: {yearly_spreads_path}")

    try:
        # Read the input CSV files
        monthly_df = pd.read_csv(monthly_prices_path)
        spreads_df = pd.read_csv(yearly_spreads_path)
    except FileNotFoundError as e:
        print(f"Error: Input file not found. {e}")
        return

    # --- Data Processing ---

    # Pivot the spreads DataFrame to have durations as columns
    spreads_pivot = spreads_df.pivot_table(index=['YEAR', 'REGION'], columns='DURATION', values='SPREAD').reset_index()
    spreads_pivot.columns = [str(col) + 'HR' if isinstance(col, (int, float)) else col for col in spreads_pivot.columns]
    spreads_pivot.rename(columns={'YEAR': 'Year', 'REGION': 'Region'}, inplace=True)
    spreads_pivot.set_index(['Year', 'Region'], inplace=True)

    # --- Data Structuring ---
    
    final_data = []
    
    # Process each profile type
    for profile_type in ['baseload', 'solar', 'wind']:
        profile_df = monthly_df[monthly_df['profile'] == profile_type].copy()
        
        # Pivot the monthly prices DataFrame to have ENERGY and GREEN as columns
        monthly_pivot = profile_df.pivot_table(index=['time', 'REGION'], columns='type', values='price').reset_index()
        monthly_pivot.rename(columns={'time': 'TIME', 'green': 'GREEN', 'Energy': 'ENERGY'}, inplace=True)
        monthly_pivot['TIME'] = pd.to_datetime(monthly_pivot['TIME'], format='%d/%m/%Y')

        # Melt the dataframe to turn ENERGY and GREEN columns into rows
        id_vars = ['TIME', 'REGION']
        value_vars = ['ENERGY', 'GREEN']
        prices_melted = monthly_pivot.melt(
            id_vars=id_vars,
            value_vars=value_vars,
            var_name='TYPE',
            value_name='PRICE'
        )

        for _, row in prices_melted.iterrows():
            year = row['TIME'].year
            region = row['REGION']
            price_type = row['TYPE']

            spread_dict = {}
            if profile_type == 'baseload' and price_type == 'ENERGY':
                try:
                    spread_data = spreads_pivot.loc[(year, region)]
                    spread_dict = {
                        '1HR': spread_data.get('1.0HR', 0),
                        '2HR': spread_data.get('2.0HR', 0),
                        '4HR': spread_data.get('4.0HR', 0),
                        '8HR': spread_data.get('8.0HR', 0),
                    }
                except KeyError:
                    pass # Keep spread_dict empty if no data

            record = {
                'PROFILE': profile_type,
                'TIME': row['TIME'].strftime('%Y-%m-%d'),
                'REGION': region,
                'TYPE': price_type,
                'PRICE': row['PRICE'],
                'SPREAD': spread_dict
            }
            final_data.append(record)

    # --- Save Output ---

    # Ensure the output directory exists
    os.makedirs(processed_outputs_dir, exist_ok=True)

    with open(output_path, 'w') as f:
        json.dump(final_data, f, indent=2)

    print(f"Successfully processed data and saved to: {output_path}")

if __name__ == '__main__':
    process_merchant_prices()