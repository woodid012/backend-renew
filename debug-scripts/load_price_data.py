import pandas as pd
import json
import os
import sys

# Add src directory to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
backend_renew_dir = os.path.dirname(current_dir)
src_dir = os.path.join(backend_renew_dir, 'src')
sys.path.insert(0, src_dir)

from core.database import insert_dataframe_to_mongodb

# Define the path to your JSON file
json_file_path = os.path.join(backend_renew_dir, 'data', 'processed_inputs', 'merchant_prices_processed_test.json')
collection_name = 'PRICE_Curves'

def load_and_insert_price_data():
    if not os.path.exists(json_file_path):
        print(f"Error: JSON file not found at {json_file_path}")
        return

    try:
        with open(json_file_path, 'r') as f:
            data = json.load(f)
        
        # Assuming the JSON structure is a list of dictionaries, or a dictionary that can be directly converted
        # If the JSON has a different structure, this part might need adjustment.
        df = pd.DataFrame(data)

        # Ensure 'date' column is in datetime format if it exists and is needed for sorting/charting
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values(by='date') # Sort by date for time series

        print(f"Loaded {len(df)} records from {json_file_path}")
        
        # Insert into MongoDB, replacing existing data for a 'scenario_id' if needed.
        # For this one-off load, we might not need a scenario_id, but it's good practice.
        # Let's assume no scenario_id for now, and just insert.
        insert_dataframe_to_mongodb(df, collection_name, replace_scenario=False)
        print(f"Data successfully inserted into MongoDB collection: {collection_name}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == '__main__':
    load_and_insert_price_data()
