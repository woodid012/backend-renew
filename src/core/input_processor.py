import pandas as pd
import json
import os

def load_asset_data(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    assets_list = data.get('asset_inputs', [])
    asset_costs = {}
    
    for asset in assets_list:
        asset_name = asset.get('name')
        if asset_name and 'costAssumptions' in asset:
            asset_costs[asset_name] = asset.get('costAssumptions')
            
    return assets_list, asset_costs

def load_price_data(monthly_price_path, yearly_spread_path):
    monthly_prices = pd.read_csv(monthly_price_path)
    yearly_spreads = pd.read_csv(yearly_spread_path)
    return monthly_prices, yearly_spreads

# You can add more general input loading functions here as needed in the future
