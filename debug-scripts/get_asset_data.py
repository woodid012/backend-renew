import sys
import os

# Add the parent directory to the Python path for module imports
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.insert(0, project_root)

from src.core.database import get_data_from_mongodb

data = get_data_from_mongodb('CONFIG_Inputs')
asset_2_data = next((asset for asset in data[0].get('asset_inputs', []) if asset.get('id') == 2), None)
print(asset_2_data)