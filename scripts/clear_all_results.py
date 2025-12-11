#!/usr/bin/env python3
"""
Script to clear all model results from MongoDB collections.
This will delete all base case and scenario data from result collections.
"""

import sys
import os

# Add parent directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from src.core.database import (
    database_lifecycle, 
    clear_base_case_data, 
    clear_all_scenario_data,
    get_data_from_mongodb
)
from src.config import (
    MONGO_ASSET_OUTPUT_COLLECTION,
    MONGO_ASSET_INPUTS_SUMMARY_COLLECTION,
    MONGO_ASSET_OUTPUT_SUMMARY_COLLECTION,
    MONGO_PNL_COLLECTION,
    MONGO_CASH_FLOW_STATEMENT_COLLECTION,
    MONGO_BALANCE_SHEET_COLLECTION,
    MONGO_SENSITIVITY_COLLECTION,
    MONGO_SENSITIVITY_PNL_COLLECTION,
    MONGO_SENSITIVITY_CASH_COLLECTION,
    MONGO_SENSITIVITY_BS_COLLECTION
)

def clear_all_results():
    """
    Clear all model results from MongoDB, including base case and all scenarios.
    """
    print("=" * 60)
    print("CLEARING ALL MODEL RESULTS FROM MONGODB")
    print("=" * 60)
    
    # All result collections
    all_collections = [
        MONGO_ASSET_OUTPUT_COLLECTION,
        MONGO_ASSET_INPUTS_SUMMARY_COLLECTION,
        MONGO_ASSET_OUTPUT_SUMMARY_COLLECTION,
        MONGO_PNL_COLLECTION,
        MONGO_CASH_FLOW_STATEMENT_COLLECTION,
        MONGO_BALANCE_SHEET_COLLECTION,
        MONGO_SENSITIVITY_COLLECTION,
        MONGO_SENSITIVITY_PNL_COLLECTION,
        MONGO_SENSITIVITY_CASH_COLLECTION,
        MONGO_SENSITIVITY_BS_COLLECTION
    ]
    
    total_deleted = 0
    
    try:
        with database_lifecycle():
            from src.core.database import db_manager
            
            print(f"\nClearing {len(all_collections)} collections...")
            
            for collection_name in all_collections:
                collection = db_manager.get_collection(collection_name)
                
                # Count existing records
                existing_count = collection.count_documents({})
                
                if existing_count > 0:
                    # Delete ALL records (both base case and scenarios)
                    delete_result = collection.delete_many({})
                    print(f"  {collection_name}: Deleted {delete_result.deleted_count} records")
                    total_deleted += delete_result.deleted_count
                else:
                    print(f"  {collection_name}: No records found (already empty)")
            
            print(f"\n{'=' * 60}")
            print(f"TOTAL RECORDS DELETED: {total_deleted}")
            print(f"{'=' * 60}")
            print("\n✅ All model results have been cleared from MongoDB.")
            print("   You can now run the model fresh without duplicate data.")
            
    except Exception as e:
        print(f"\n❌ Error clearing results: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Clear all model results from MongoDB')
    parser.add_argument('--confirm', action='store_true', 
                       help='Skip confirmation prompt (use with caution)')
    
    args = parser.parse_args()
    
    if not args.confirm:
        response = input("\n⚠️  WARNING: This will delete ALL model results from MongoDB.\n"
                        "   This includes base case and all scenario data.\n"
                        "   Continue? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("Operation cancelled.")
            sys.exit(0)
    
    success = clear_all_results()
    sys.exit(0 if success else 1)






