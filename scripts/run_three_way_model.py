import pandas as pd
from datetime import datetime
import os
import sys

# Add the backend directory to the Python path for module imports
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'src'))

from src.core.database import get_mongo_client, insert_dataframe_to_mongodb
from src.calculations.three_way_financials import generate_pnl, generate_cash_flow_statement, generate_balance_sheet
from config import MONGO_ASSET_OUTPUT_COLLECTION, MONGO_PNL_COLLECTION, MONGO_CASH_FLOW_STATEMENT_COLLECTION, MONGO_BALANCE_SHEET_COLLECTION

def run_three_way_model():
    print("=== RUNNING 3-WAY FINANCIAL MODEL ===")

    client = None
    try:
        client = get_mongo_client()
        db = client.get_database()
        
        # 1. Retrieve final_cash_flow data from MongoDB
        print(f"Retrieving data from {MONGO_ASSET_OUTPUT_COLLECTION}...")
        final_cash_flow_collection = db[MONGO_ASSET_OUTPUT_COLLECTION]
        final_cash_flow_data = list(final_cash_flow_collection.find({}))
        
        if not final_cash_flow_data:
            print(f"No data found in {MONGO_ASSET_OUTPUT_COLLECTION}. Please run main.py first.")
            return

        final_cash_flow_df = pd.DataFrame(final_cash_flow_data)
        
        # Convert date column back to datetime objects if necessary
        if 'date' in final_cash_flow_df.columns:
            final_cash_flow_df['date'] = pd.to_datetime(final_cash_flow_df['date'])

        # 2. Generate P&L Statement
        print("Generating P&L Statement...")
        pnl_df = generate_pnl(final_cash_flow_df)
        insert_dataframe_to_mongodb(pnl_df, MONGO_PNL_COLLECTION)

        # 3. Generate Cash Flow Statement
        print("Generating Cash Flow Statement...")
        cf_statement_df = generate_cash_flow_statement(pnl_df, final_cash_flow_df)
        insert_dataframe_to_mongodb(cf_statement_df, MONGO_CASH_FLOW_STATEMENT_COLLECTION)

        # 4. Generate Balance Sheet
        print("Generating Balance Sheet...")
        # For simplicity, using dummy initial values. In a real scenario, these would come from initial balance sheet.
        balance_sheet_df = generate_balance_sheet(pnl_df, cf_statement_df, final_cash_flow_df, 
                                                  initial_cash=100000, initial_ppe=500000, 
                                                  initial_debt=300000, initial_equity=200000)
        insert_dataframe_to_mongodb(balance_sheet_df, MONGO_BALANCE_SHEET_COLLECTION)

        print("3-Way Financial Model run complete. Results saved to MongoDB.")

    except Exception as e:
        print(f"Error running 3-way financial model: {e}")
    finally:
        if client:
            client.close()

if __name__ == '__main__':
    run_three_way_model()