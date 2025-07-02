# scripts/run_three_way_model.py

import pandas as pd
from datetime import datetime
import os
import sys

# Add the project root and src directory to the Python path for module imports
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)  # Go up one level to project root
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, project_root)
sys.path.insert(0, src_dir)

from src.core.database import get_mongo_client
from src.calculations.three_way_financials import generate_pnl, generate_cash_flow_statement, generate_balance_sheet
from src.config import MONGO_ASSET_OUTPUT_COLLECTION

# Updated collection names as requested
MONGO_PNL_COLLECTION = "3WAY_P&L"
MONGO_CASH_FLOW_STATEMENT_COLLECTION = "3WAY_CASH"
MONGO_BALANCE_SHEET_COLLECTION = "3WAY_BS"

def insert_dataframe_with_replace(df: pd.DataFrame, collection_name: str, scenario_id: str = None):
    """
    Insert DataFrame with replacement of existing scenario data to prevent duplicates.
    """
    if df.empty:
        print(f"DataFrame for collection '{collection_name}' is empty. No data inserted.")
        return

    client = None
    try:
        client = get_mongo_client()
        db = client.get_database()
        collection = db[collection_name]

        # If scenario_id is provided, delete existing records for this scenario first
        if scenario_id:
            existing_count = collection.count_documents({"scenario_id": scenario_id})
            if existing_count > 0:
                print(f"Replacing {existing_count} existing records for scenario '{scenario_id}' in '{collection_name}'")
                delete_result = collection.delete_many({"scenario_id": scenario_id})
                print(f"Deleted {delete_result.deleted_count} existing records")

        # Convert DataFrame to a list of dictionaries (JSON-like objects)
        records = df.to_dict(orient='records')
        
        # Add scenario_id to each document if provided
        if scenario_id:
            for record in records:
                record['scenario_id'] = scenario_id
        
        # Insert records
        result = collection.insert_many(records)
        action = "Replaced and inserted" if scenario_id else "Inserted"
        print(f"Successfully {action.lower()} {len(result.inserted_ids)} documents into '{collection_name}' collection.")
        
    except Exception as e:
        print(f"Error inserting data into MongoDB collection '{collection_name}': {e}")
    finally:
        if client:
            client.close()

def delete_existing_tables():
    """
    Delete the existing three-way model tables completely.
    """
    print(f"=== DELETING EXISTING THREE-WAY TABLES ===")
    
    collections_to_delete = [
        ("P&L Statements", MONGO_PNL_COLLECTION),
        ("Cash Flow Statements", MONGO_CASH_FLOW_STATEMENT_COLLECTION), 
        ("Balance Sheets", MONGO_BALANCE_SHEET_COLLECTION)
    ]
    
    client = None
    try:
        client = get_mongo_client()
        db = client.get_database()
        
        for display_name, collection_name in collections_to_delete:
            collection = db[collection_name]
            
            # Count existing records
            existing_count = collection.count_documents({})
            
            if existing_count > 0:
                print(f"  {display_name} ({collection_name}): Found {existing_count:,} records")
                
                response = input(f"    Delete entire {display_name} table? (yes/no): ")
                if response.lower() in ['yes', 'y']:
                    # Drop the entire collection
                    db.drop_collection(collection_name)
                    print(f"    ✓ Deleted entire {display_name} table")
                else:
                    print(f"    Skipped {display_name}")
            else:
                print(f"  {display_name} ({collection_name}): No records found")
        
        print("Table deletion complete.")
        
    except Exception as e:
        print(f"Error during table deletion: {e}")
    finally:
        if client:
            client.close()

def cleanup_three_way_collections(scenario_id: str = None):
    """
    Clean up existing three-way model results to prevent duplicates.
    """
    if not scenario_id:
        print("No scenario_id provided - cannot clean up safely")
        return False
    
    print(f"=== CLEANING UP EXISTING THREE-WAY RESULTS ===")
    print(f"Scenario: {scenario_id}")
    
    collections_to_clean = [
        ("P&L Statements", MONGO_PNL_COLLECTION),
        ("Cash Flow Statements", MONGO_CASH_FLOW_STATEMENT_COLLECTION),
        ("Balance Sheets", MONGO_BALANCE_SHEET_COLLECTION)
    ]
    
    client = None
    total_deleted = 0
    
    try:
        client = get_mongo_client()
        db = client.get_database()
        
        for display_name, collection_name in collections_to_clean:
            collection = db[collection_name]
            
            # Count existing records for this scenario
            existing_count = collection.count_documents({"scenario_id": scenario_id})
            
            if existing_count > 0:
                print(f"  {display_name}: Found {existing_count} existing records")
                delete_result = collection.delete_many({"scenario_id": scenario_id})
                print(f"  {display_name}: Deleted {delete_result.deleted_count} records")
                total_deleted += delete_result.deleted_count
            else:
                print(f"  {display_name}: No existing records found")
        
        print(f"Total records deleted: {total_deleted}")
        return True
        
    except Exception as e:
        print(f"Error during cleanup: {e}")
        return False
    finally:
        if client:
            client.close()

def aggregate_by_frequency(df, frequency='Q', fiscal_year_start_month=7):
    """
    Aggregate DataFrame by specified frequency.
    
    Args:
        df (pd.DataFrame): DataFrame with 'date' column
        frequency (str): 'M' (Monthly), 'Q' (Quarterly), 'CY' (Calendar Year), 'FY' (Fiscal Year)
        fiscal_year_start_month (int): Month that fiscal year starts (1-12)
    
    Returns:
        pd.DataFrame: Aggregated DataFrame
    """
    if df.empty or 'date' not in df.columns:
        return df
    
    # Ensure date is datetime
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    
    # Get numeric columns to aggregate
    numeric_columns = df.select_dtypes(include=['number']).columns.tolist()
    
    # Keep essential columns
    groupby_columns = ['asset_id'] if 'asset_id' in df.columns else []
    
    if frequency == 'M':
        # Monthly - group by year-month
        df['period'] = df['date'].dt.to_period('M')
        df['period_date'] = df['period'].dt.start_time
        
    elif frequency == 'Q':
        # Quarterly - group by quarter
        df['period'] = df['date'].dt.to_period('Q')
        df['period_date'] = df['period'].dt.start_time
        
    elif frequency == 'CY':
        # Calendar Year - group by year
        df['period'] = df['date'].dt.to_period('Y')
        df['period_date'] = df['period'].dt.start_time
        
    elif frequency == 'FY':
        # Fiscal Year - custom logic based on fiscal_year_start_month
        df['fiscal_year'] = df['date'].apply(
            lambda x: x.year if x.month >= fiscal_year_start_month else x.year - 1
        )
        df['period'] = df['fiscal_year'].astype(str) + '_FY'
        # Set period_date to start of fiscal year
        df['period_date'] = df['fiscal_year'].apply(
            lambda fy: pd.Timestamp(year=fy, month=fiscal_year_start_month, day=1)
        )
    else:
        raise ValueError(f"Unsupported frequency: {frequency}. Use 'M', 'Q', 'CY', or 'FY'")
    
    # Group by asset_id (if exists) and period
    group_columns = groupby_columns + ['period', 'period_date']
    
    # Aggregate numeric columns
    agg_dict = {col: 'sum' for col in numeric_columns if col not in group_columns}
    
    if not agg_dict:
        return df  # No numeric columns to aggregate
    
    aggregated = df.groupby(group_columns).agg(agg_dict).reset_index()
    
    # Rename period_date back to date
    aggregated['date'] = aggregated['period_date']
    aggregated = aggregated.drop(['period_date'], axis=1)
    
    # Add frequency info
    aggregated['frequency'] = frequency
    if frequency == 'FY':
        aggregated['fiscal_year_start_month'] = fiscal_year_start_month
    
    return aggregated

def run_three_way_model_for_scenario(scenario_id, frequency='Q', fiscal_year_start_month=7):
    """
    Run three-way model for a specific scenario with specified frequency.
    
    Args:
        scenario_id (str): Scenario identifier
        frequency (str): 'M' (Monthly), 'Q' (Quarterly), 'CY' (Calendar Year), 'FY' (Fiscal Year)
        fiscal_year_start_month (int): Month that fiscal year starts (1-12), only used for FY
    """
    print(f"Processing scenario: {scenario_id} (Frequency: {frequency})")
    
    client = None
    try:
        client = get_mongo_client()
        db = client.get_database()
        
        # Step 1: Clean up existing three-way results for this scenario
        cleanup_success = cleanup_three_way_collections(scenario_id)
        if not cleanup_success:
            print("Failed to clean up existing results. Continuing anyway...")
        
        # Step 2: Retrieve final_cash_flow data from MongoDB
        final_cash_flow_collection = db[MONGO_ASSET_OUTPUT_COLLECTION]
        
        query = {'scenario_id': scenario_id}
        final_cash_flow_data = list(final_cash_flow_collection.find(query))
        
        if not final_cash_flow_data:
            print(f"No data found for scenario_id '{scenario_id}'. Skipping.")
            return False

        final_cash_flow_df = pd.DataFrame(final_cash_flow_data)
        print(f"  Retrieved {len(final_cash_flow_df)} records")
        
        # Convert date column back to datetime objects if necessary
        if 'date' in final_cash_flow_df.columns:
            final_cash_flow_df['date'] = pd.to_datetime(final_cash_flow_df['date'])

        # Fill missing columns with zeros
        required_columns = ['revenue', 'opex', 'depreciation', 'interest', 'tax_expense', 'capex', 'principal', 'equity_cash_flow']
        for col in required_columns:
            if col not in final_cash_flow_df.columns:
                print(f"  Warning: Column '{col}' not found, filling with zeros")
                final_cash_flow_df[col] = 0.0

        # Step 3: Aggregate data by frequency before generating statements
        print(f"  Aggregating data by {frequency} frequency...")
        aggregated_df = aggregate_by_frequency(final_cash_flow_df, frequency, fiscal_year_start_month)
        print(f"  Aggregated to {len(aggregated_df)} {frequency} periods")

        # Step 4: Generate P&L Statement
        print("  Generating P&L Statement...")
        pnl_df = generate_pnl(aggregated_df)
        insert_dataframe_with_replace(pnl_df, MONGO_PNL_COLLECTION, scenario_id=scenario_id)

        # Step 5: Generate Cash Flow Statement
        print("  Generating Cash Flow Statement...")
        cf_statement_df = generate_cash_flow_statement(pnl_df, aggregated_df)
        insert_dataframe_with_replace(cf_statement_df, MONGO_CASH_FLOW_STATEMENT_COLLECTION, scenario_id=scenario_id)

        # Step 6: Generate Balance Sheet
        print("  Generating Balance Sheet...")
        balance_sheet_df = generate_balance_sheet(pnl_df, cf_statement_df, aggregated_df, 
                                                  initial_cash=100000, initial_ppe=500000, 
                                                  initial_debt=300000, initial_equity=200000)
        insert_dataframe_with_replace(balance_sheet_df, MONGO_BALANCE_SHEET_COLLECTION, scenario_id=scenario_id)

        print(f"  ✓ Completed scenario: {scenario_id}")
        return True

    except Exception as e:
        print(f"  ✗ Error processing scenario {scenario_id}: {e}")
        return False
    finally:
        if client:
            client.close()

def run_three_way_model(scenario_id=None, process_all_sensitivities=False, sensitivity_prefix="sensitivity_results", 
                       frequency='Q', fiscal_year_start_month=7):
    print("=== RUNNING 3-WAY FINANCIAL MODEL ===")
    print(f"Frequency: {frequency}")
    if frequency == 'FY':
        print(f"Fiscal Year starts in month: {fiscal_year_start_month}")

    if process_all_sensitivities:
        print(f"Processing all scenarios with prefix: {sensitivity_prefix}")
        
        # Get all sensitivity scenario IDs
        client = None
        try:
            client = get_mongo_client()
            db = client.get_database()
            collection = db[MONGO_ASSET_OUTPUT_COLLECTION]
            
            scenario_ids = collection.distinct("scenario_id", {
                "scenario_id": {"$regex": f"^{sensitivity_prefix}"}
            })
            
            print(f"Found {len(scenario_ids)} scenarios to process")
            
            if not scenario_ids:
                print("No sensitivity scenarios found")
                return
            
            # Process each scenario
            successful = 0
            failed = 0
            
            for scenario_id in sorted(scenario_ids):
                success = run_three_way_model_for_scenario(scenario_id, frequency, fiscal_year_start_month)
                if success:
                    successful += 1
                else:
                    failed += 1
            
            print(f"\n=== BATCH PROCESSING COMPLETE ===")
            print(f"Successfully processed: {successful} scenarios")
            print(f"Failed: {failed} scenarios")
            
        except Exception as e:
            print(f"Error getting scenario list: {e}")
        finally:
            if client:
                client.close()
    
    else:
        # Single scenario processing (original behavior)
        if not scenario_id:
            # Default to base scenario
            scenario_id = f"{sensitivity_prefix}_base"
            print(f"No scenario specified, defaulting to: {scenario_id}")
        
        success = run_three_way_model_for_scenario(scenario_id, frequency, fiscal_year_start_month)
        
        if success:
            # Verify no duplicates were created
            verify_no_duplicates(scenario_id)
            print("3-Way Financial Model run complete. Results saved to MongoDB.")
        else:
            print("3-Way Financial Model run failed.")

def verify_no_duplicates(scenario_id: str):
    """
    Verify that no duplicate records exist after the three-way model run.
    """
    if not scenario_id:
        print("Cannot verify duplicates without scenario_id")
        return
    
    print(f"\n=== VERIFYING NO DUPLICATES ===")
    
    collections_to_check = [
        ("P&L", MONGO_PNL_COLLECTION),
        ("Cash Flow", MONGO_CASH_FLOW_STATEMENT_COLLECTION),
        ("Balance Sheet", MONGO_BALANCE_SHEET_COLLECTION)
    ]
    
    client = None
    try:
        client = get_mongo_client()
        db = client.get_database()
        
        for display_name, collection_name in collections_to_check:
            collection = db[collection_name]
            
            # Get all data for this scenario
            scenario_data = list(collection.find({"scenario_id": scenario_id}))
            
            if scenario_data:
                df = pd.DataFrame(scenario_data)
                
                # Check for duplicates by asset_id + date
                if 'asset_id' in df.columns and 'date' in df.columns:
                    duplicate_check = df.groupby(['asset_id', 'date']).size().reset_index(name='count')
                    duplicates = duplicate_check[duplicate_check['count'] > 1]
                    
                    if len(duplicates) > 0:
                        print(f"  ⚠ {display_name}: Found {len(duplicates)} duplicate combinations!")
                    else:
                        print(f"  ✓ {display_name}: No duplicates ({len(df)} records)")
                else:
                    print(f"  ? {display_name}: Cannot check duplicates - missing key columns")
            else:
                print(f"  - {display_name}: No data found")
        
    except Exception as e:
        print(f"Error verifying duplicates: {e}")
    finally:
        if client:
            client.close()

def cleanup_all_three_way_results(scenario_prefix: str = None):
    """
    Clean up ALL three-way model results (useful for bulk cleanup).
    """
    print(f"=== CLEANING UP ALL THREE-WAY RESULTS ===")
    
    collections_to_clean = [
        ("P&L Statements", MONGO_PNL_COLLECTION),
        ("Cash Flow Statements", MONGO_CASH_FLOW_STATEMENT_COLLECTION),
        ("Balance Sheets", MONGO_BALANCE_SHEET_COLLECTION)
    ]
    
    client = None
    total_deleted = 0
    
    try:
        client = get_mongo_client()
        db = client.get_database()
        
        for display_name, collection_name in collections_to_clean:
            collection = db[collection_name]
            
            if scenario_prefix:
                query = {"scenario_id": {"$regex": f"^{scenario_prefix}"}}
                description = f"scenarios starting with '{scenario_prefix}'"
            else:
                query = {}
                description = "all records"
            
            # Count existing records
            existing_count = collection.count_documents(query)
            
            if existing_count > 0:
                print(f"  {display_name}: Found {existing_count} records ({description})")
                
                response = input(f"    Delete {existing_count} records from {display_name}? (y/n): ")
                if response.lower() in ['y', 'yes']:
                    delete_result = collection.delete_many(query)
                    print(f"    Deleted {delete_result.deleted_count} records")
                    total_deleted += delete_result.deleted_count
                else:
                    print(f"    Skipped {display_name}")
            else:
                print(f"  {display_name}: No records found")
        
        print(f"Total records deleted: {total_deleted}")
        
    except Exception as e:
        print(f"Error during cleanup: {e}")
    finally:
        if client:
            client.close()

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description="Run the 3-way financial model")
    parser.add_argument('--scenario_id', type=str, help="Scenario ID to filter data (defaults to sensitivity_results_base)")
    parser.add_argument('--all-sensitivities', action='store_true', help="Process all sensitivity scenarios")
    parser.add_argument('--sensitivity-prefix', type=str, default='sensitivity_results', help="Prefix for sensitivity scenarios (default: sensitivity_results)")
    parser.add_argument('--frequency', type=str, choices=['M', 'Q', 'CY', 'FY'], default='Q', 
                       help="Aggregation frequency: M=Monthly, Q=Quarterly, CY=Calendar Year, FY=Fiscal Year (default: Q)")
    parser.add_argument('--fiscal-year-start', type=int, default=7, choices=range(1, 13), 
                       help="Fiscal year start month (1-12, default: 7 for July)")
    parser.add_argument('--delete-tables', action='store_true', help="Delete existing three-way tables completely")
    parser.add_argument('--cleanup-all', action='store_true', help="Clean up all three-way results")
    parser.add_argument('--cleanup-prefix', type=str, help="Clean up three-way results for scenarios starting with prefix")
    
    args = parser.parse_args()
    
    if args.delete_tables:
        delete_existing_tables()
    elif args.cleanup_all:
        cleanup_all_three_way_results()
    elif args.cleanup_prefix:
        cleanup_all_three_way_results(args.cleanup_prefix)
    else:
        run_three_way_model(
            scenario_id=args.scenario_id, 
            process_all_sensitivities=args.all_sensitivities,
            sensitivity_prefix=args.sensitivity_prefix,
            frequency=args.frequency,
            fiscal_year_start_month=args.fiscal_year_start
        )