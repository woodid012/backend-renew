
import sys
import os
import pandas as pd
import numpy as np

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.database import mongo_session
from src.core.equity_irr import calculate_equity_irr

def diagnose_asset_irr(asset_id, unique_id=None):
    print(f"\n=== DIAGNOSING IRR FOR ASSET {asset_id} ===")
    
    with mongo_session() as db_mgr:
        # 1. Check ASSET_Output_Summary (Dashboard Source)
        summary_col = db_mgr.get_collection("ASSET_Output_Summary")
        query = {"asset_id": asset_id}
        if unique_id:
            query["unique_id"] = unique_id
            
        summary_doc = summary_col.find_one(query)
        
        if summary_doc:
            print(f"ASSET_Output_Summary stored IRR: {summary_doc.get('equity_irr', 'N/A')}")
        else:
            print("ASSET_Output_Summary: No document found")
            
        # 2. Check ASSET_cash_flows (Calculation Source)
        cf_col = db_mgr.get_collection("ASSET_cash_flows")
        
        # Build query for Asset 1
        # Need to be careful with types (int vs str)
        cf_query = {
            "scenario_id": {"$in": [None, "base_case"]},
            "$or": [
                {"asset_id": asset_id},
                {"asset_id": str(asset_id)},
                {"asset_id": int(asset_id) if str(asset_id).isdigit() else -1}
            ]
        }
        if unique_id:
            cf_query["unique_id"] = unique_id

        cf_docs = list(cf_col.find(cf_query))
        print(f"ASSET_cash_flows: Found {len(cf_docs)} records")
        
        if not cf_docs:
            print("No cash flow records found!")
            return

        df = pd.DataFrame(cf_docs)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        
        # Check available columns
        print(f"Columns available: {list(df.columns)}")
        
        # 3. Calculate IRR using 'equity_cash_flow' (Net)
        if 'equity_cash_flow' in df.columns:
            # Filter logic matching enhanced_sensitivity_summary OLD logic (mostly)
            # Typically C+O periods
            if 'period_type' in df.columns:
                df_net = df[df['period_type'].isin(['C', 'O']) | (df['equity_cash_flow'] != 0)].copy()
            else:
                df_net = df.copy()
                
            irr_net = calculate_equity_irr(df_net[['date', 'equity_cash_flow']])
            print(f"Calculated IRR using 'equity_cash_flow' (Net): {irr_net:.4%}")
        else:
            print("'equity_cash_flow' column missing")

        # 4. Calculate IRR using 'equity_cash_flow_pre_distributions' (Gross)
        if 'equity_cash_flow_pre_distributions' in df.columns:
            col = 'equity_cash_flow_pre_distributions'
            # Filter logic matching MAIN.PY logic
            # C+O periods OR non-zero value OR terminal value > 0 OR equity_capex != 0
            
            mask = (df[col] != 0)
            if 'period_type' in df.columns:
                mask = mask | df['period_type'].isin(['C', 'O'])
            if 'terminal_value' in df.columns:
                mask = mask | (df['terminal_value'] > 0)
            if 'equity_capex' in df.columns:
                mask = mask | (df['equity_capex'] != 0)
                
            df_gross = df[mask].copy()
            
            # Fix: Select ONLY the column we need first to avoid collision when renaming
            df_gross_for_irr = df_gross[['date', col]].copy()
            df_gross_for_irr = df_gross_for_irr.rename(columns={col: 'equity_cash_flow'})
            
            irr_gross = calculate_equity_irr(df_gross_for_irr)
            print(f"Calculated IRR using 'equity_cash_flow_pre_distributions' (Gross): {irr_gross:.4%}")
            
            # Debug: Check totals
            print(f"Total Gross CF: {df_gross[col].sum():,.2f}")
            if 'equity_cash_flow' in df.columns:
                print(f"Total Net CF:   {df_net['equity_cash_flow'].sum():,.2f}")

        else:
            print("'equity_cash_flow_pre_distributions' column missing")

    with mongo_session() as db_mgr:
        # 5. Check SENS_Asset_Outputs for column existence
        sens_col = db_mgr.get_collection("SENS_Asset_Outputs")
        # Find one record for this portfolio
        sens_doc = sens_col.find_one({"scenario_id": {"$regex": "sensitivity_results"}, "$or": [{"unique_id": unique_id}, {"portfolio": "Renewable Project Finance"}]})
        
        if sens_doc:
            print(f"\nSENS_Asset_Outputs (Sample): Found record for scenario {sens_doc.get('scenario_id')}")
            print(f"SENS Columns available: {list(sens_doc.keys())}")
            if 'equity_cash_flow_pre_distributions' in sens_doc:
                print("✓ 'equity_cash_flow_pre_distributions' EXISTS in sensitivity data")
            else:
                print("❌ 'equity_cash_flow_pre_distributions' MISSING in sensitivity data")
        else:
            print("\nSENS_Asset_Outputs: No records found to check schema")


if __name__ == "__main__":
    # Asset 1, Portfolio PRIe3oRLfO4uck35xwYFJ
    diagnose_asset_irr(1, "PRIe3oRLfO4uck35xwYFJ")
