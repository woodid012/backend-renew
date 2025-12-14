#!/usr/bin/env python3
"""
Diagnostic script to check if results exist in MongoDB for a given unique_id.
Usage: python scripts/diagnose_results.py <unique_id>
"""

import sys
import os
from dotenv import load_dotenv
from pymongo import MongoClient

# Load environment variables
env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env.local')
if os.path.exists(env_path):
    load_dotenv(dotenv_path=env_path)
else:
    load_dotenv()

MONGO_URI = os.getenv('MONGODB_URI')
MONGO_DB_NAME = os.getenv('MONGODB_DB')

def diagnose_results(unique_id):
    """Check if results exist for the given unique_id"""
    if not MONGO_URI:
        print("ERROR: MONGODB_URI not found in environment variables")
        return
    
    if not unique_id:
        print("ERROR: unique_id not provided")
        return
    
    print(f"\n{'='*80}")
    print(f"DIAGNOSING RESULTS FOR unique_id: {unique_id}")
    print(f"{'='*80}\n")
    
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB_NAME]
        
        # Step 1: Check CONFIG_Inputs
        print("Step 1: Checking CONFIG_Inputs...")
        config_collection = db['CONFIG_Inputs']
        config_doc = config_collection.find_one({'unique_id': unique_id})
        
        if config_doc:
            print(f"  [OK] Portfolio found in CONFIG_Inputs")
            print(f"     - PlatformName: {config_doc.get('PlatformName', 'N/A')}")
            print(f"     - unique_id: {config_doc.get('unique_id', 'N/A')}")
            asset_count = len(config_doc.get('asset_inputs', []))
            print(f"     - Asset count: {asset_count}")
            
            if asset_count > 0:
                asset_ids = [asset.get('id') for asset in config_doc.get('asset_inputs', [])]
                print(f"     - Asset IDs: {asset_ids}")
        else:
            print(f"  [ERROR] Portfolio NOT found in CONFIG_Inputs with unique_id: {unique_id}")
            # List available unique_ids
            all_configs = config_collection.find({}, {'unique_id': 1, 'PlatformName': 1}).limit(20)
            available = list(all_configs)
            if available:
                print(f"  Available portfolios:")
                for cfg in available:
                    print(f"     - unique_id: {cfg.get('unique_id')}, PlatformName: {cfg.get('PlatformName')}")
            return
        
        # Step 2: Check ASSET_cash_flows
        print(f"\nStep 2: Checking ASSET_cash_flows...")
        cashflows_collection = db['ASSET_cash_flows']
        cashflow_count = cashflows_collection.count_documents({'unique_id': unique_id})
        status = "[OK]" if cashflow_count > 0 else "[ERROR]"
        print(f"  {status} Found {cashflow_count} cash flow records with unique_id: {unique_id}")
        
        if cashflow_count > 0:
            # Get sample record
            sample = cashflows_collection.find_one({'unique_id': unique_id})
            if sample:
                print(f"  Sample record:")
                print(f"     - asset_id: {sample.get('asset_id')}")
                print(f"     - unique_id: {sample.get('unique_id')}")
                print(f"     - date: {sample.get('date')}")
                print(f"     - revenue: {sample.get('revenue', 0)}")
            
            # Get unique asset_ids in results
            asset_ids_in_results = cashflows_collection.distinct('asset_id', {'unique_id': unique_id})
            print(f"  Asset IDs in results: {sorted(asset_ids_in_results)}")
        
        # Step 3: Check ASSET_Output_Summary
        print(f"\nStep 3: Checking ASSET_Output_Summary...")
        output_summary_collection = db['ASSET_Output_Summary']
        summary_count = output_summary_collection.count_documents({'unique_id': unique_id})
        status = "[OK]" if summary_count > 0 else "[ERROR]"
        print(f"  {status} Found {summary_count} summary records with unique_id: {unique_id}")
        
        if summary_count > 0:
            summaries = list(output_summary_collection.find({'unique_id': unique_id}, {'asset_id': 1, 'asset_name': 1}).limit(10))
            print(f"  Sample summaries:")
            for s in summaries:
                print(f"     - asset_id: {s.get('asset_id')}, asset_name: {s.get('asset_name')}")
        
        # Step 4: Check ASSET_inputs_summary
        print(f"\nStep 4: Checking ASSET_inputs_summary...")
        inputs_summary_collection = db['ASSET_inputs_summary']
        inputs_count = inputs_summary_collection.count_documents({'unique_id': unique_id})
        status = "[OK]" if inputs_count > 0 else "[ERROR]"
        print(f"  {status} Found {inputs_count} input summary records with unique_id: {unique_id}")
        
        # Step 5: Check for results without unique_id (backward compatibility)
        if cashflow_count == 0:
            print(f"\nStep 5: Checking for results with portfolio name (backward compatibility)...")
            platform_name = config_doc.get('PlatformName')
            if platform_name:
                portfolio_count = cashflows_collection.count_documents({'portfolio': platform_name})
                print(f"  Found {portfolio_count} records with portfolio: {platform_name}")
                if portfolio_count > 0:
                    print(f"  [WARNING] Results exist with 'portfolio' field but not 'unique_id' field!")
                    print(f"     This suggests results were stored before unique_id was implemented.")
        
        # Summary
        print(f"\n{'='*80}")
        print("SUMMARY")
        print(f"{'='*80}")
        print(f"Portfolio Config: {'[OK] Found' if config_doc else '[ERROR] Not Found'}")
        print(f"Cash Flows: {cashflow_count} records")
        print(f"Output Summary: {summary_count} records")
        print(f"Inputs Summary: {inputs_count} records")
        
        if cashflow_count == 0 and summary_count == 0:
            print(f"\n[ERROR] NO RESULTS FOUND for unique_id: {unique_id}")
            print(f"   Possible causes:")
            print(f"   1. Model run did not complete successfully")
            print(f"   2. Results were stored with a different unique_id")
            print(f"   3. Results were stored with 'portfolio' field instead of 'unique_id'")
        else:
            print(f"\n[OK] RESULTS FOUND for unique_id: {unique_id}")
        
        client.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python scripts/diagnose_results.py <unique_id>")
        print("Example: python scripts/diagnose_results.py Kvm53lEncqicaOHMUo-yr")
        sys.exit(1)
    
    unique_id = sys.argv[1]
    diagnose_results(unique_id)

