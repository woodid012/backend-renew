import json
import os
from datetime import datetime
import pandas as pd

import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
backend_renew_root = os.path.dirname(current_dir) # This should be C:\Projects\renew\backend-renew
sys.path.insert(0, backend_renew_root)

from src.calculations.price_curves import get_merchant_price
from src.config import MERCHANT_PRICE_ESCALATION_RATE, MERCHANT_PRICE_ESCALATION_REFERENCE_DATE

def run_test_price_curve():
    print("Running test for get_merchant_price_test...")

    # Define path to the processed JSON file
    processed_prices_path = os.path.join(backend_renew_root, 'data', 'processed_inputs', 'merchant_prices_processed_test.json')

    try:
        with open(processed_prices_path, 'r') as f:
            processed_prices_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: Processed prices file not found at {processed_prices_path}")
        return

    # --- Test Cases ---

    # Test Case 1: Energy price for a specific region and date
    profile1 = 'baseload'
    price_type1 = 'ENERGY'
    region1 = 'NSW'
    date1 = datetime(2025, 1, 1)
    price1 = get_merchant_price(profile1, price_type1, region1, date1, processed_prices_data)
    print(f"\nTest Case 1: {profile1}-{price1} in {region1} on {date1.strftime('%Y-%m-%d')}: {price1:.2f}")

    # Test Case 2: Green price (should use ALL region and profile)
    profile2 = 'solar' # This should be ignored for green prices
    price_type2 = 'GREEN'
    region2 = 'QLD' # This should be ignored for green prices
    date2 = datetime(2025, 1, 1)
    price2 = get_merchant_price(profile2, price_type2, region2, date2, processed_prices_data)
    print(f"Test Case 2: {profile2}-{price_type2} in {region2} on {date2.strftime('%Y-%m-%d')}: {price2:.2f}")

    # Test Case 3: Storage spread (0.5HR)
    profile3 = 'storage' # Profile doesn't matter for spreads in this function
    price_type3 = 0.5 # Duration
    region3 = 'VIC'
    date3 = datetime(2025, 1, 1)
    price3 = get_merchant_price(profile3, price_type3, region3, date3, processed_prices_data)
    print(f"Test Case 3: {profile3}-{price_type3}HR spread in {region3} on {date3.strftime('%Y-%m-%d')}: {price3:.2f}")

    # Test Case 4: Storage spread (interpolation for 3HR)
    profile4 = 'storage'
    price_type4 = 3.0 # Duration
    region4 = 'NSW'
    date4 = datetime(2025, 1, 1)
    price4 = get_merchant_price(profile4, price_type4, region4, date4, processed_prices_data)
    print(f"Test Case 4: {profile4}-{price_type4}HR spread in {region4} on {date4.strftime('%Y-%m-%d')}: {price4:.2f}")

    # Test Case 5: Price for a future date (escalation)
    profile5 = 'baseload'
    price_type5 = 'ENERGY'
    region5 = 'NSW'
    date5 = datetime(2030, 6, 1)
    price5 = get_merchant_price(profile5, price_type5, region5, date5, processed_prices_data)
    print(f"Test Case 5: {profile5}-{price_type5} in {region5} on {date5.strftime('%Y-%m-%d')}: {price5:.2f}")

if __name__ == '__main__':
    run_test_price_curve()
