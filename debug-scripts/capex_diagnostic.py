# scripts/diagnose_capex_asset1.py

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
from src.config import MONGO_ASSET_OUTPUT_COLLECTION

def diagnose_asset1_capex(scenario_id="sensitivity_results_base"):
    """
    Diagnose CAPEX calculation for Asset 1 (Templers BESS)
    """
    print(f"=== DIAGNOSING ASSET 1 CAPEX ===")
    print(f"Expected CAPEX: $238.6M")
    print(f"Construction: 2024-03-01 to 2025-08-01 (17 months)")
    print(f"Scenario: {scenario_id}")
    
    client = None
    
    try:
        client = get_mongo_client()
        db = client.get_database()
        collection = db[MONGO_ASSET_OUTPUT_COLLECTION]
        
        # Get Asset 1 data for the base scenario
        query = {
            "scenario_id": scenario_id,
            "asset_id": 1
        }
        
        asset1_data = list(collection.find(query))
        
        if not asset1_data:
            print(f"No data found for Asset 1 in scenario {scenario_id}")
            return
        
        df = pd.DataFrame(asset1_data)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        
        print(f"\nFound {len(df)} records for Asset 1")
        print(f"Date range: {df['date'].min()} to {df['date'].max()}")
        
        # Check for CAPEX entries
        capex_entries = df[df['capex'] > 0].copy()
        
        print(f"\n=== CAPEX ENTRIES ===")
        print(f"Number of months with CAPEX > 0: {len(capex_entries)}")
        
        if len(capex_entries) > 0:
            print(f"CAPEX date range: {capex_entries['date'].min()} to {capex_entries['date'].max()}")
            print(f"Total CAPEX sum: ${capex_entries['capex'].sum():,.2f}M")
            print(f"Average monthly CAPEX: ${capex_entries['capex'].mean():,.3f}M")
            print(f"Min monthly CAPEX: ${capex_entries['capex'].min():,.3f}M")
            print(f"Max monthly CAPEX: ${capex_entries['capex'].max():,.3f}M")
            
            # Show first few CAPEX entries
            print(f"\nFirst 5 CAPEX entries:")
            for _, row in capex_entries.head().iterrows():
                print(f"  {row['date'].strftime('%Y-%m-%d')}: CAPEX=${row['capex']:,.3f}M, Debt=${row.get('debt_capex', 0):,.3f}M, Equity=${row.get('equity_capex', 0):,.3f}M")
            
            # Check for duplicates by date
            date_counts = capex_entries['date'].value_counts()
            duplicates = date_counts[date_counts > 1]
            
            if len(duplicates) > 0:
                print(f"\n⚠ DUPLICATE DATES FOUND:")
                for date, count in duplicates.items():
                    print(f"  {date.strftime('%Y-%m-%d')}: {count} entries")
                    
                    # Show the duplicate entries
                    dup_entries = capex_entries[capex_entries['date'] == date]
                    for _, row in dup_entries.iterrows():
                        print(f"    CAPEX=${row['capex']:,.3f}M, Record ID: {row.get('_id', 'N/A')}")
            else:
                print(f"\n✓ No duplicate dates found")
        
        # Check all columns that might contain CAPEX data
        print(f"\n=== ALL CAPEX-RELATED COLUMNS ===")
        capex_columns = [col for col in df.columns if 'capex' in col.lower()]
        for col in capex_columns:
            col_sum = df[col].sum() if col in df.columns else 0
            col_nonzero = (df[col] > 0).sum() if col in df.columns else 0
            print(f"  {col}: Sum=${col_sum:,.2f}M, Non-zero entries={col_nonzero}")
        
        # Check for any systematic patterns
        print(f"\n=== SYSTEMATIC ANALYSIS ===")
        
        # Group by date and sum to see if there are multiple entries per date
        daily_totals = df.groupby('date').agg({
            'capex': 'sum',
            'debt_capex': 'sum', 
            'equity_capex': 'sum'
        }).reset_index()
        
        construction_period = daily_totals[daily_totals['capex'] > 0]
        
        if len(construction_period) > 0:
            print(f"After grouping by date:")
            print(f"  Construction months: {len(construction_period)}")
            print(f"  Total CAPEX: ${construction_period['capex'].sum():,.2f}M")
            print(f"  Expected monthly CAPEX: ${238.6/17:.3f}M")
            print(f"  Actual average monthly: ${construction_period['capex'].mean():,.3f}M")
            print(f"  Multiplier vs expected: {construction_period['capex'].sum()/238.6:.2f}x")
        
        # Check record count per date during construction
        construction_dates = capex_entries['date'].unique()
        print(f"\n=== RECORD COUNT ANALYSIS ===")
        
        for date in sorted(construction_dates)[:5]:  # First 5 construction dates
            date_records = df[df['date'] == date]
            print(f"  {date.strftime('%Y-%m-%d')}: {len(date_records)} records")
            
            if len(date_records) > 1:
                print(f"    ⚠ Multiple records for same date!")
                for i, (_, record) in enumerate(date_records.iterrows()):
                    print(f"      Record {i+1}: CAPEX=${record.get('capex', 0):,.3f}M")
        
        return df
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None
    
    finally:
        if client:
            client.close()

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description="Diagnose CAPEX calculation for Asset 1")
    parser.add_argument('--scenario', type=str, default='sensitivity_results_base',
                       help='Scenario ID to analyze (default: sensitivity_results_base)')
    
    args = parser.parse_args()
    
    df = diagnose_asset1_capex(args.scenario)
    
    if df is not None:
        print(f"\n✓ Diagnosis complete. Check output above for issues.")
    else:
        print(f"\n✗ Diagnosis failed.")