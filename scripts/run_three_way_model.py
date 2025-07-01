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

from src.core.database import get_mongo_client, insert_dataframe_to_mongodb
from src.calculations.three_way_financials import generate_pnl, generate_cash_flow_statement, generate_balance_sheet
from src.config import (
    MONGO_ASSET_OUTPUT_COLLECTION, 
    MONGO_PNL_COLLECTION, 
    MONGO_CASH_FLOW_STATEMENT_COLLECTION, 
    MONGO_BALANCE_SHEET_COLLECTION
)

def run_three_way_model(scenario_id=None):
    print("=== RUNNING 3-WAY FINANCIAL MODEL ===")

    client = None
    try:
        client = get_mongo_client()
        db = client.get_database()
        
        # 1. Retrieve final_cash_flow data from MongoDB
        print(f"Retrieving data from {MONGO_ASSET_OUTPUT_COLLECTION}...")
        final_cash_flow_collection = db[MONGO_ASSET_OUTPUT_COLLECTION]
        
        # If scenario_id is provided, filter by it
        query = {}
        if scenario_id:
            query['scenario_id'] = scenario_id
            print(f"Filtering by scenario_id: {scenario_id}")
        
        final_cash_flow_data = list(final_cash_flow_collection.find(query))
        
        if not final_cash_flow_data:
            if scenario_id:
                print(f"No data found in {MONGO_ASSET_OUTPUT_COLLECTION} for scenario_id '{scenario_id}'. Please run main.py with this scenario first.")
            else:
                print(f"No data found in {MONGO_ASSET_OUTPUT_COLLECTION}. Please run main.py first.")
            return

        final_cash_flow_df = pd.DataFrame(final_cash_flow_data)
        print(f"Retrieved {len(final_cash_flow_df)} records")
        
        # Convert date column back to datetime objects if necessary
        if 'date' in final_cash_flow_df.columns:
            final_cash_flow_df['date'] = pd.to_datetime(final_cash_flow_df['date'])

        # Fill missing columns with zeros
        required_columns = ['revenue', 'opex', 'depreciation', 'interest', 'tax_expense', 'capex', 'principal', 'equity_cash_flow']
        for col in required_columns:
            if col not in final_cash_flow_df.columns:
                print(f"Warning: Column '{col}' not found, filling with zeros")
                final_cash_flow_df[col] = 0.0

        # 2. Generate P&L Statement
        print("Generating P&L Statement...")
        pnl_df = generate_pnl(final_cash_flow_df)
        insert_dataframe_to_mongodb(pnl_df, MONGO_PNL_COLLECTION, scenario_id=scenario_id)
        print(f"Saved {len(pnl_df)} P&L records to MongoDB")

        # 3. Generate Cash Flow Statement
        print("Generating Cash Flow Statement...")
        cf_statement_df = generate_cash_flow_statement(pnl_df, final_cash_flow_df)
        insert_dataframe_to_mongodb(cf_statement_df, MONGO_CASH_FLOW_STATEMENT_COLLECTION, scenario_id=scenario_id)
        print(f"Saved {len(cf_statement_df)} Cash Flow Statement records to MongoDB")

        # 4. Generate Balance Sheet
        print("Generating Balance Sheet...")
        # For simplicity, using dummy initial values. In a real scenario, these would come from initial balance sheet.
        balance_sheet_df = generate_balance_sheet(pnl_df, cf_statement_df, final_cash_flow_df, 
                                                  initial_cash=100000, initial_ppe=500000, 
                                                  initial_debt=300000, initial_equity=200000)
        insert_dataframe_to_mongodb(balance_sheet_df, MONGO_BALANCE_SHEET_COLLECTION, scenario_id=scenario_id)
        print(f"Saved {len(balance_sheet_df)} Balance Sheet records to MongoDB")

        print("3-Way Financial Model run complete. Results saved to MongoDB.")

    except Exception as e:
        print(f"Error running 3-way financial model: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if client:
            client.close()

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description="Run the 3-way financial model")
    parser.add_argument('--scenario_id', type=str, help="Scenario ID to filter data (optional)")
    
    args = parser.parse_args()
    
    run_three_way_model(scenario_id=args.scenario_id)