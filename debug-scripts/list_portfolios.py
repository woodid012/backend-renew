#!/usr/bin/env python3
"""
List all available portfolios in the database
"""

import os
import sys
from dotenv import load_dotenv

# Add parent directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

load_dotenv()

from src.core.database import database_lifecycle, get_data_from_mongodb

def main():
    print("="*80)
    print("LISTING AVAILABLE PORTFOLIOS")
    print("="*80)
    
    try:
        with database_lifecycle():
            # Get all portfolios from CONFIG_Inputs
            config_data = get_data_from_mongodb('CONFIG_Inputs', query={})
            
            if not config_data:
                print("\n[INFO] No portfolios found in CONFIG_Inputs")
                return
            
            print(f"\n[OK] Found {len(config_data)} portfolio(s):\n")
            
            for i, config in enumerate(config_data, 1):
                platform_name = config.get('PlatformName', 'Unknown')
                asset_count = len(config.get('asset_inputs', []))
                print(f"  {i}. {platform_name} ({asset_count} assets)")
            
            print("\n" + "="*80)
            
    except Exception as e:
        print(f"\n[ERROR] {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == '__main__':
    sys.exit(main())


