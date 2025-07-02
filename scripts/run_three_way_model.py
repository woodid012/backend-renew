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
from src.config import MONGO_ASSET_OUTPUT_COLLECTION, MONGO_SENSITIVITY_COLLECTION

# Base case collection names (for non-sensitivity scenarios)
BASE_PNL_COLLECTION = "3WAY_P&L"
BASE_CASH_FLOW_STATEMENT_COLLECTION = "3WAY_CASH"
BASE_BALANCE_SHEET_COLLECTION = "3WAY_BS"

# Sensitivity collection names (for sensitivity scenarios)
SENS_PNL_COLLECTION = "SENS_3WAY_P&L"
SENS_CASH_FLOW_STATEMENT_COLLECTION = "SENS_3WAY_CASH"
SENS_BALANCE_SHEET_COLLECTION = "SENS_3WAY_BS"

def get_collection_names(scenario_id, sensitivity_prefix="sensitivity_results"):
    """
    Determine which collections to use based on scenario type
    
    Args:
        scenario_id (str): The scenario identifier
        sensitivity_prefix (str): Prefix that identifies sensitivity scenarios
    
    Returns:
        tuple: (pnl_collection, cash_collection, bs_collection)
    """
    if scenario_id and scenario_id.startswith(sensitivity_prefix):
        print(f"  Detected sensitivity scenario: using SENS_ collections")
        return SENS_PNL_COLLECTION, SENS_CASH_FLOW_STATEMENT_COLLECTION, SENS_BALANCE_SHEET_COLLECTION
    else:
        print(f"  Detected base case scenario: using 3WAY_ collections")
        return BASE_PNL_COLLECTION, BASE_CASH_FLOW_STATEMENT_COLLECTION, BASE_BALANCE_SHEET_COLLECTION

def get_input_collection(scenario_id, sensitivity_prefix="sensitivity_results"):
    """
    Determine which input collection to read from based on scenario type
    
    Args:
        scenario_id (str): The scenario identifier
        sensitivity_prefix (str): Prefix that identifies sensitivity scenarios
    
    Returns:
        str: Collection name to read input data from
    """
    if scenario_id and scenario_id.startswith(sensitivity_prefix):
        return MONGO_SENSITIVITY_COLLECTION
    else:
        return MONGO_ASSET_OUTPUT_COLLECTION

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

def cleanup_three_way_collections(scenario_id: str = None, sensitivity_prefix="sensitivity_results", include_sensitivities=False):
    """
    Clean up existing three-way model results to prevent duplicates.
    Only cleans SENS_3WAY collections if include_sensitivities=True.
    """
    if not scenario_id:
        print("No scenario_id provided - cannot clean up safely")
        return False
    
    # Check if this is a sensitivity scenario
    is_sensitivity = scenario_id.startswith(sensitivity_prefix)
    
    if is_sensitivity and not include_sensitivities:
        print(f"  Skipping cleanup for sensitivity scenario (include_sensitivities=False)")
        return True
    
    print(f"=== CLEANING UP EXISTING THREE-WAY RESULTS ===")
    print(f"Scenario: {scenario_id}")
    
    # Determine which collections to clean based on scenario type
    pnl_collection, cash_collection, bs_collection = get_collection_names(scenario_id, sensitivity_prefix)
    
    collections_to_clean = [
        ("P&L Statements", pnl_collection),
        ("Cash Flow Statements", cash_collection),
        ("Balance Sheets", bs_collection)
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

def run_three_way_model_for_scenario(scenario_id, frequency='Q', fiscal_year_start_month=7, sensitivity_prefix="sensitivity_results", include_sensitivities=False):
    """
    Run three-way model for a specific scenario with specified frequency.
    Only creates SENS_3WAY tables if include_sensitivities=True.
    
    Args:
        scenario_id (str): Scenario identifier
        frequency (str): 'M' (Monthly), 'Q' (Quarterly), 'CY' (Calendar Year), 'FY' (Fiscal Year)
        fiscal_year_start_month (int): Month that fiscal year starts (1-12), only used for FY
        sensitivity_prefix (str): Prefix that identifies sensitivity scenarios
        include_sensitivities (bool): Whether to process sensitivity scenarios into SENS_3WAY tables
    """
    print(f"Processing scenario: {scenario_id} (Frequency: {frequency})")
    
    # Check if this is a sensitivity scenario
    is_sensitivity = scenario_id.startswith(sensitivity_prefix)
    
    if is_sensitivity and not include_sensitivities:
        print(f"  ⚠ Skipping sensitivity scenario (use --include-sensitivities to process)")
        return False
    
    # Determine collections to use
    pnl_collection, cash_collection, bs_collection = get_collection_names(scenario_id, sensitivity_prefix)
    input_collection_name = get_input_collection(scenario_id, sensitivity_prefix)
    
    client = None
    try:
        client = get_mongo_client()
        db = client.get_database()
        
        # Step 1: Clean up existing three-way results for this scenario
        cleanup_success = cleanup_three_way_collections(scenario_id, sensitivity_prefix, include_sensitivities)
        if not cleanup_success:
            print("Failed to clean up existing results. Continuing anyway...")
        
        # Step 2: Retrieve cash flow data from the appropriate collection
        input_collection = db[input_collection_name]
        
        query = {'scenario_id': scenario_id}
        cash_flow_data = list(input_collection.find(query))
        
        if not cash_flow_data:
            print(f"No data found for scenario_id '{scenario_id}' in collection '{input_collection_name}'. Skipping.")
            return False

        cash_flow_df = pd.DataFrame(cash_flow_data)
        print(f"  Retrieved {len(cash_flow_df)} records from '{input_collection_name}'")
        
        # Convert date column back to datetime objects if necessary
        if 'date' in cash_flow_df.columns:
            cash_flow_df['date'] = pd.to_datetime(cash_flow_df['date'])

        # Fill missing columns with zeros
        required_columns = ['revenue', 'opex', 'depreciation', 'interest', 'tax_expense', 'capex', 'principal', 'equity_cash_flow']
        for col in required_columns:
            if col not in cash_flow_df.columns:
                print(f"  Warning: Column '{col}' not found, filling with zeros")
                cash_flow_df[col] = 0.0

        # Step 3: Aggregate data by frequency before generating statements
        print(f"  Aggregating data by {frequency} frequency...")
        aggregated_df = aggregate_by_frequency(cash_flow_df, frequency, fiscal_year_start_month)
        print(f"  Aggregated to {len(aggregated_df)} {frequency} periods")

        # Step 4: Generate P&L Statement
        print(f"  Generating P&L Statement -> {pnl_collection}")
        pnl_df = generate_pnl(aggregated_df)
        insert_dataframe_with_replace(pnl_df, pnl_collection, scenario_id=scenario_id)

        # Step 5: Generate Cash Flow Statement
        print(f"  Generating Cash Flow Statement -> {cash_collection}")
        cf_statement_df = generate_cash_flow_statement(pnl_df, aggregated_df)
        insert_dataframe_with_replace(cf_statement_df, cash_collection, scenario_id=scenario_id)

        # Step 6: Generate Balance Sheet
        print(f"  Generating Balance Sheet -> {bs_collection}")
        balance_sheet_df = generate_balance_sheet(pnl_df, cf_statement_df, aggregated_df, 
                                                  initial_cash=100000, initial_ppe=500000, 
                                                  initial_debt=300000, initial_equity=200000)
        insert_dataframe_with_replace(balance_sheet_df, bs_collection, scenario_id=scenario_id)

        print(f"  ✓ Completed scenario: {scenario_id}")
        return True

    except Exception as e:
        print(f"  ✗ Error processing scenario {scenario_id}: {e}")
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

def run_three_way_model(scenario_id=None, process_all_sensitivities=False, sensitivity_prefix="sensitivity_results", 
                       frequency='Q', fiscal_year_start_month=7, include_sensitivities=False):
    print("=== RUNNING 3-WAY FINANCIAL MODEL ===")
    print(f"Frequency: {frequency}")
    if frequency == 'FY':
        print(f"Fiscal Year starts in month: {fiscal_year_start_month}")

    if process_all_sensitivities:
        if not include_sensitivities:
            print(f"⚠ WARNING: --all-sensitivities specified but --include-sensitivities not set")
            print(f"No sensitivity scenarios will be processed. Use --include-sensitivities to enable.")
            return
            
        print(f"Processing all scenarios with prefix: {sensitivity_prefix}")
        
        # Get all sensitivity scenario IDs from the sensitivity collection
        client = None
        try:
            client = get_mongo_client()
            db = client.get_database()
            collection = db[MONGO_SENSITIVITY_COLLECTION]
            
            scenario_ids = collection.distinct("scenario_id", {
                "scenario_id": {"$regex": f"^{sensitivity_prefix}"}
            })
            
            print(f"Found {len(scenario_ids)} sensitivity scenarios to process")
            
            if not scenario_ids:
                print("No sensitivity scenarios found")
                return
            
            # Process each scenario
            successful = 0
            failed = 0
            
            for scenario_id in sorted(scenario_ids):
                success = run_three_way_model_for_scenario(scenario_id, frequency, fiscal_year_start_month, sensitivity_prefix, include_sensitivities)
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
        # Single scenario processing
        if not scenario_id:
            # Default to base scenario
            scenario_id = f"{sensitivity_prefix}_base"
            print(f"No scenario specified, defaulting to: {scenario_id}")
        
        # Check if this is a sensitivity scenario
        is_sensitivity = scenario_id.startswith(sensitivity_prefix)
        
        if is_sensitivity and not include_sensitivities:
            print(f"⚠ WARNING: Scenario '{scenario_id}' appears to be a sensitivity scenario")
            print(f"But --include-sensitivities not specified. Skipping to avoid creating SENS_3WAY tables.")
            print(f"Use --include-sensitivities flag to process sensitivity scenarios.")
            return
        
        success = run_three_way_model_for_scenario(scenario_id, frequency, fiscal_year_start_month, sensitivity_prefix, include_sensitivities)
        
        if success:
            # Verify no duplicates were created
            verify_no_duplicates(scenario_id, sensitivity_prefix, include_sensitivities)
            print("3-Way Financial Model run complete. Results saved to MongoDB.")
        else:
            print("3-Way Financial Model run failed.")

def verify_no_duplicates(scenario_id: str, sensitivity_prefix="sensitivity_results", include_sensitivities=False):
    """
    Verify that no duplicate records exist after the three-way model run.
    """
    if not scenario_id:
        print("Cannot verify duplicates without scenario_id")
        return
    
    # Check if this is a sensitivity scenario
    is_sensitivity = scenario_id.startswith(sensitivity_prefix)
    
    if is_sensitivity and not include_sensitivities:
        print(f"  Skipping verification for sensitivity scenario (include_sensitivities=False)")
        return
    
    print(f"\n=== VERIFYING NO DUPLICATES ===")
    
    # Get the appropriate collection names
    pnl_collection, cash_collection, bs_collection = get_collection_names(scenario_id, sensitivity_prefix)
    
    collections_to_check = [
        ("P&L", pnl_collection),
        ("Cash Flow", cash_collection),
        ("Balance Sheet", bs_collection)
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

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description="Run the 3-way financial model")
    parser.add_argument('--scenario_id', type=str, help="Scenario ID to filter data")
    parser.add_argument('--all-sensitivities', action='store_true', help="Process all sensitivity scenarios")
    parser.add_argument('--sensitivity-prefix', type=str, default='sensitivity_results', help="Prefix for sensitivity scenarios")
    parser.add_argument('--frequency', type=str, choices=['M', 'Q', 'CY', 'FY'], default='Q', 
                       help="Aggregation frequency: M=Monthly, Q=Quarterly, CY=Calendar Year, FY=Fiscal Year")
    parser.add_argument('--fiscal-year-start', type=int, default=7, choices=range(1, 13), 
                       help="Fiscal year start month (1-12, default: 7 for July)")
    parser.add_argument('--include-sensitivities', action='store_true', 
                       help="Allow processing of sensitivity scenarios into SENS_3WAY collections")
    
    args = parser.parse_args()
    
    run_three_way_model(
        scenario_id=args.scenario_id, 
        process_all_sensitivities=args.all_sensitivities,
        sensitivity_prefix=args.sensitivity_prefix,
        frequency=args.frequency,
        fiscal_year_start_month=args.fiscal_year_start,
        include_sensitivities=args.include_sensitivities
    )