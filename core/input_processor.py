import pandas as pd
import json
import os

def load_asset_data(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    assets_dict = data.get('assets', {})
    assets_list = []
    for asset_id, asset_data in assets_dict.items():
        asset_data['id'] = int(asset_id)
        assets_list.append(asset_data)
    return assets_list, data.get('constants', {}).get('assetCosts', {})

def load_price_data(monthly_price_path, yearly_spread_path):
    monthly_prices = pd.read_csv(monthly_price_path)
    yearly_spreads = pd.read_csv(yearly_spread_path)
    return monthly_prices, yearly_spreads

# You can add more general input loading functions here as needed in the future
