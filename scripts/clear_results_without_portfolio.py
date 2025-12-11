#!/usr/bin/env python3
"""
Script to clear all model results from MongoDB collections that don't have a portfolio field.
This will delete records that were created before portfolio filtering was implemented.
"""

import sys
import os

# Add parent directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from src.core.database import database_lifecycle
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

def clear_results_without_portfolio():
    """
    Clear all model results from MongoDB that don't have a portfolio field.
    """
    print("=" * 60)
    print("CLEARING RESULTS WITHOUT PORTFOLIO FIELD")
    print("=" * 60)
    
    # Collections that should have portfolio field
    collections_to_check = [
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
            
            print(f"\nChecking {len(collections_to_check)} collections...")
            
            for collection_name in collections_to_check:
                collection = db_manager.get_collection(collection_name)
                
                # Query for records without portfolio field
                # This includes: portfolio field doesn't exist, is None, or is empty string
                query = {
                    "$or": [
                        {"portfolio": {"$exists": False}},
                        {"portfolio": None},
                        {"portfolio": ""}
                    ]
                }
                
                # Count existing records without portfolio
                existing_count = collection.count_documents(query)
                
                if existing_count > 0:
                    # Delete records without portfolio field
                    delete_result = collection.delete_many(query)
                    print(f"  {collection_name}: Deleted {delete_result.deleted_count} records (without portfolio field)")
                    total_deleted += delete_result.deleted_count
                else:
                    print(f"  {collection_name}: No records without portfolio field found")
            
            print(f"\n{'=' * 60}")
            print(f"TOTAL RECORDS DELETED: {total_deleted}")
            print(f"{'=' * 60}")
            print("\n✅ All records without portfolio field have been cleared.")
            print("   You can now run portfolios fresh with proper portfolio tagging.")
            
    except Exception as e:
        print(f"\n❌ Error clearing results: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Clear model results without portfolio field from MongoDB')
    parser.add_argument('--confirm', action='store_true', 
                       help='Skip confirmation prompt (use with caution)')
    
    args = parser.parse_args()
    
    if not args.confirm:
        response = input("\n⚠️  WARNING: This will delete all model results that don't have a portfolio field.\n"
                        "   This includes old records created before portfolio filtering was implemented.\n"
                        "   Continue? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("Operation cancelled.")
            sys.exit(0)
    
    success = clear_results_without_portfolio()
    sys.exit(0 if success else 1)





