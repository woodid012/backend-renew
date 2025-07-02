# scripts/audit_all_database_duplicates.py

import pandas as pd
import os
import sys
from datetime import datetime

# Add the project root and src directory to the Python path for module imports
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)  # Go up one level to project root
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, project_root)
sys.path.insert(0, src_dir)

from src.core.database import get_mongo_client
from src.config import (
    MONGO_ASSET_OUTPUT_COLLECTION,
    MONGO_ASSET_INPUTS_SUMMARY_COLLECTION, 
    MONGO_REVENUE_COLLECTION,
    MONGO_PRICE_SERIES_COLLECTION,
    MONGO_PNL_COLLECTION,
    MONGO_CASH_FLOW_STATEMENT_COLLECTION,
    MONGO_BALANCE_SHEET_COLLECTION
)

def audit_collection_duplicates(collection_name: str, scenario_prefix: str = None):
    """
    Audit a single collection for duplicate records
    """
    client = None
    try:
        client = get_mongo_client()
        db = client.get_database()
        collection = db[collection_name]
        
        query = {}
        if scenario_prefix:
            query = {"scenario_id": {"$regex": f"^{scenario_prefix}"}}
        
        # Get all data
        data = list(collection.find(query))
        
        if not data:
            return {
                "collection": collection_name,
                "total_records": 0,
                "status": "empty"
            }
        
        df = pd.DataFrame(data)
        
        # Determine grouping columns based on what's available
        group_cols = []
        key_cols = ['scenario_id', 'asset_id', 'date']
        
        for col in key_cols:
            if col in df.columns:
                group_cols.append(col)
        
        if not group_cols:
            return {
                "collection": collection_name,
                "total_records": len(df),
                "status": "no_grouping_columns",
                "available_columns": list(df.columns)
            }
        
        # Check for duplicates
        duplicate_check = df.groupby(group_cols).size().reset_index(name='count')
        duplicates = duplicate_check[duplicate_check['count'] > 1]
        
        total_records = len(df)
        unique_combinations = len(duplicate_check)
        duplicate_combinations = len(duplicates)
        
        result = {
            "collection": collection_name,
            "total_records": total_records,
            "unique_combinations": unique_combinations,
            "duplicate_combinations": duplicate_combinations,
            "duplication_factor": total_records / unique_combinations if unique_combinations > 0 else 0,
            "grouping_columns": group_cols,
            "status": "has_duplicates" if duplicate_combinations > 0 else "clean"
        }
        
        if duplicate_combinations > 0:
            result["avg_duplication"] = duplicates['count'].mean()
            result["max_duplication"] = duplicates['count'].max()
            result["sample_duplicates"] = []
            
            # Add sample duplicates
            for _, row in duplicates.head(3).iterrows():
                sample = {col: row[col] for col in group_cols}
                sample["count"] = row["count"]
                result["sample_duplicates"].append(sample)
        
        return result
        
    except Exception as e:
        return {
            "collection": collection_name,
            "error": str(e),
            "status": "error"
        }
    finally:
        if client:
            client.close()

def audit_all_collections(scenario_prefix: str = None):
    """
    Audit all MongoDB collections for duplicate records
    """
    print(f"=== AUDITING ALL COLLECTIONS FOR DUPLICATES ===")
    if scenario_prefix:
        print(f"Focusing on scenarios starting with: {scenario_prefix}")
    else:
        print("Checking all records in all collections")
    
    # All collections to check
    collections_to_check = [
        ("asset_cash_flows", MONGO_ASSET_OUTPUT_COLLECTION),
        ("asset_inputs_summary", MONGO_ASSET_INPUTS_SUMMARY_COLLECTION),
        ("revenue_data", MONGO_REVENUE_COLLECTION),
        ("price_series", MONGO_PRICE_SERIES_COLLECTION),
        ("pnl_statements", MONGO_PNL_COLLECTION),
        ("cash_flow_statements", MONGO_CASH_FLOW_STATEMENT_COLLECTION),
        ("balance_sheets", MONGO_BALANCE_SHEET_COLLECTION)
    ]
    
    results = []
    total_issues = 0
    
    print(f"\nChecking {len(collections_to_check)} collections...")
    
    for display_name, collection_name in collections_to_check:
        print(f"\n--- {display_name} ({collection_name}) ---")
        
        result = audit_collection_duplicates(collection_name, scenario_prefix)
        results.append(result)
        
        if result["status"] == "empty":
            print("  ✓ Collection is empty")
        elif result["status"] == "error":
            print(f"  ✗ Error: {result['error']}")
            total_issues += 1
        elif result["status"] == "no_grouping_columns":
            print(f"  ⚠ Cannot check duplicates - no key columns found")
            print(f"    Available columns: {result.get('available_columns', [])}")
        elif result["status"] == "clean":
            print(f"  ✓ No duplicates found ({result['total_records']:,} records)")
        elif result["status"] == "has_duplicates":
            print(f"  ✗ DUPLICATES FOUND!")
            print(f"    Total records: {result['total_records']:,}")
            print(f"    Unique combinations: {result['unique_combinations']:,}")
            print(f"    Duplicate combinations: {result['duplicate_combinations']:,}")
            print(f"    Duplication factor: {result['duplication_factor']:.2f}x")
            print(f"    Average duplication: {result.get('avg_duplication', 0):.2f}")
            print(f"    Maximum duplication: {result.get('max_duplication', 0)}")
            
            if result.get('sample_duplicates'):
                print(f"    Sample duplicates:")
                for sample in result['sample_duplicates']:
                    count = sample.pop('count')
                    key_str = ', '.join([f"{k}={v}" for k, v in sample.items()])
                    print(f"      {key_str}: {count} copies")
            
            total_issues += 1
    
    # Summary
    print(f"\n=== AUDIT SUMMARY ===")
    print(f"Collections checked: {len(collections_to_check)}")
    print(f"Collections with issues: {total_issues}")
    
    if total_issues == 0:
        print("✓ All collections are clean!")
    else:
        print(f"⚠ {total_issues} collections have duplicate issues")
        
        # Provide recommendations
        print(f"\n=== RECOMMENDATIONS ===")
        
        problematic_collections = [r for r in results if r["status"] == "has_duplicates"]
        
        if problematic_collections:
            print("Collections that need cleanup:")
            for result in problematic_collections:
                factor = result.get('duplication_factor', 0)
                if factor > 2:
                    urgency = "HIGH PRIORITY"
                elif factor > 1.5:
                    urgency = "Medium priority"
                else:
                    urgency = "Low priority"
                
                print(f"  - {result['collection']} ({factor:.1f}x duplication) - {urgency}")
            
            print(f"\nSuggested actions:")
            print(f"1. Clean up sensitivity results: python scripts/cleanup_duplicate_sensitivity_results.py")
            print(f"2. Use improved database functions with replace_scenario=True")
            print(f"3. Add cleanup steps to other model runners (three-way model, etc.)")
    
    return results

def cleanup_specific_collection(collection_name: str, scenario_prefix: str):
    """
    Clean up duplicates from a specific collection
    """
    print(f"=== CLEANING UP {collection_name} ===")
    
    client = None
    try:
        client = get_mongo_client()
        db = client.get_database()
        collection = db[collection_name]
        
        # Count records to be deleted
        query = {"scenario_id": {"$regex": f"^{scenario_prefix}"}}
        count = collection.count_documents(query)
        
        if count > 0:
            print(f"Found {count:,} records to delete")
            response = input(f"Delete all {count:,} records from {collection_name}? (yes/no): ")
            
            if response.lower() in ['yes', 'y']:
                result = collection.delete_many(query)
                print(f"✓ Deleted {result.deleted_count:,} records")
                return True
            else:
                print("Cleanup cancelled")
                return False
        else:
            print("No records found to delete")
            return True
            
    except Exception as e:
        print(f"Error during cleanup: {e}")
        return False
    finally:
        if client:
            client.close()

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description="Audit all MongoDB collections for duplicate records")
    parser.add_argument('--prefix', type=str, 
                       help='Only check scenarios starting with this prefix (e.g., sensitivity_results)')
    parser.add_argument('--cleanup', type=str,
                       help='Clean up specific collection (use with --prefix)')
    
    args = parser.parse_args()
    
    if args.cleanup and args.prefix:
        cleanup_specific_collection(args.cleanup, args.prefix)
    else:
        results = audit_all_collections(args.prefix)
        
        if args.cleanup:
            print("\n⚠ To cleanup, you must specify both --cleanup COLLECTION and --prefix PREFIX")
            print("Example: python scripts/audit_all_database_duplicates.py --cleanup asset_cash_flows --prefix sensitivity_results")