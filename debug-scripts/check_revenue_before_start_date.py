# debug-scripts/check_revenue_before_start_date.py
"""
Diagnostic script to investigate why ASSET_cash_flows has non-zero revenue
before OperatingStartDate for Test2 portfolio.
"""

import os
import sys
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient

# Add parent directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.insert(0, project_root)

# Load environment variables
env_path = os.path.join(project_root, '.env.local')
if os.path.exists(env_path):
    load_dotenv(dotenv_path=env_path)

MONGO_URI = os.getenv('MONGODB_URI')
MONGO_DB_NAME = os.getenv('MONGODB_DB')
PORTFOLIO_NAME = 'Test2'

def main():
    print("="*80)
    print("REVENUE BEFORE OPERATING START DATE DIAGNOSTIC")
    print("="*80)
    print(f"Portfolio: {PORTFOLIO_NAME}")
    print()
    
    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    
    # Step 1: Get asset OperatingStartDate from CONFIG_Inputs
    print("Step 1: Fetching asset OperatingStartDate from CONFIG_Inputs...")
    config_collection = db['CONFIG_Inputs']
    config_data = config_collection.find_one({'PlatformName': PORTFOLIO_NAME})
    
    if not config_data:
        print(f"ERROR: No config found for portfolio '{PORTFOLIO_NAME}'")
        return
    
    assets = config_data.get('asset_inputs', [])
    print(f"Found {len(assets)} assets in portfolio")
    print()
    
    asset_start_dates = {}
    for asset in assets:
        asset_id = asset.get('id')
        asset_name = asset.get('name', f'Asset_{asset_id}')
        operating_start = asset.get('OperatingStartDate')
        asset_start_dates[asset_id] = {
            'name': asset_name,
            'OperatingStartDate': operating_start,
            'parsed_date': pd.to_datetime(operating_start) if operating_start else None
        }
        print(f"  Asset {asset_id} ({asset_name}): OperatingStartDate = {operating_start}")
    print()
    
    # Step 2: Query ASSET_cash_flows for revenue before start dates
    print("Step 2: Checking revenue in ASSET_cash_flows before OperatingStartDate...")
    cashflow_collection = db['ASSET_cash_flows']
    
    issues_found = []
    
    for asset_id, asset_info in asset_start_dates.items():
        if not asset_info['parsed_date']:
            print(f"  Asset {asset_id}: No OperatingStartDate, skipping")
            continue
        
        start_date = asset_info['parsed_date']
        asset_name = asset_info['name']
        
        print(f"\n  Asset {asset_id} ({asset_name}):")
        print(f"    OperatingStartDate: {start_date.strftime('%Y-%m-%d')}")
        
        # Query for records before OperatingStartDate with non-zero revenue
        query = {
            'asset_id': asset_id,
            'portfolio': PORTFOLIO_NAME,
            'date': {'$lt': start_date},
            'revenue': {'$ne': 0, '$exists': True}
        }
        
        # Also check for base case (no scenario_id)
        base_case_query = {**query, '$or': [
            {'scenario_id': {'$exists': False}},
            {'scenario_id': None},
            {'scenario_id': ''}
        ]}
        
        problematic_records = list(cashflow_collection.find(base_case_query).sort('date', 1).limit(10))
        
        if problematic_records:
            print(f"    ⚠️  FOUND {len(problematic_records)} records with non-zero revenue before start date:")
            for record in problematic_records[:5]:  # Show first 5
                record_date = record['date']
                revenue = record.get('revenue', 0)
                monthly_gen = record.get('monthlyGeneration', 0)
                scenario_id = record.get('scenario_id', 'base case')
                print(f"      Date: {record_date.strftime('%Y-%m-%d')}, Revenue: {revenue:.6f}, "
                      f"MonthlyGen: {monthly_gen:.2f}, Scenario: {scenario_id}")
            
            issues_found.append({
                'asset_id': asset_id,
                'asset_name': asset_name,
                'start_date': start_date,
                'count': len(problematic_records),
                'records': problematic_records[:5]
            })
        else:
            print(f"    ✅ No non-zero revenue before start date")
        
        # Also check monthlyGeneration before start date
        gen_query = {
            'asset_id': asset_id,
            'portfolio': PORTFOLIO_NAME,
            'date': {'$lt': start_date},
            'monthlyGeneration': {'$ne': 0, '$exists': True}
        }
        gen_query['$or'] = [
            {'scenario_id': {'$exists': False}},
            {'scenario_id': None},
            {'scenario_id': ''}
        ]
        
        gen_records = list(cashflow_collection.find(gen_query).sort('date', 1).limit(5))
        if gen_records:
            print(f"    ⚠️  FOUND {len(gen_records)} records with non-zero monthlyGeneration before start date")
            for record in gen_records[:3]:
                record_date = record['date']
                monthly_gen = record.get('monthlyGeneration', 0)
                print(f"      Date: {record_date.strftime('%Y-%m-%d')}, MonthlyGen: {monthly_gen:.2f}")
    
    print()
    
    # Step 3: Check for scenario_id in problematic records
    print("Step 3: Checking if scenarios were applied...")
    if issues_found:
        for issue in issues_found:
            asset_id = issue['asset_id']
            # Check all records for this asset before start date
            all_before_start = list(cashflow_collection.find({
                'asset_id': asset_id,
                'portfolio': PORTFOLIO_NAME,
                'date': {'$lt': issue['start_date']}
            }))
            
            scenario_ids = set()
            for record in all_before_start:
                scenario_id = record.get('scenario_id')
                if scenario_id:
                    scenario_ids.add(scenario_id)
            
            if scenario_ids:
                print(f"  Asset {asset_id}: Scenarios found: {list(scenario_ids)}")
            else:
                print(f"  Asset {asset_id}: Base case only (no scenarios)")
    
    print()
    
    # Step 4: Summary
    print("="*80)
    print("SUMMARY")
    print("="*80)
    if issues_found:
        print(f"⚠️  ISSUES FOUND: {len(issues_found)} asset(s) with non-zero revenue before OperatingStartDate")
        for issue in issues_found:
            print(f"  - Asset {issue['asset_id']} ({issue['asset_name']}): "
                  f"{issue['count']} records with revenue before {issue['start_date'].strftime('%Y-%m-%d')}")
    else:
        print("✅ No issues found - all revenue is zero before OperatingStartDate")
    print()
    
    # Step 5: Test date comparison
    print("Step 5: Testing date comparison logic...")
    if issues_found:
        issue = issues_found[0]
        test_start_date = issue['start_date']
        test_record_date = issue['records'][0]['date'] if issue['records'] else None
        
        if test_record_date:
            print(f"  Testing: {test_record_date} >= {test_start_date}?")
            # Test with pandas Timestamp (what date_range creates)
            pd_timestamp = pd.Timestamp(test_record_date)
            # Test with datetime (what strptime creates)
            dt_start = datetime(test_start_date.year, test_start_date.month, test_start_date.day)
            
            result1 = pd_timestamp >= dt_start
            result2 = pd.Timestamp(test_record_date) >= pd.Timestamp(test_start_date)
            
            print(f"    pandas.Timestamp >= datetime: {result1}")
            print(f"    pandas.Timestamp >= pandas.Timestamp: {result2}")
            print(f"    Expected: False (record date should be < start date)")
    
    client.close()
    print("\nDiagnostic complete!")

if __name__ == '__main__':
    main()

