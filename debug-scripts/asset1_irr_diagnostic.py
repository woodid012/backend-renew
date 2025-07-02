# debug-scripts/asset1_irr_diagnostic.py

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os

# Add project paths
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, project_root)
sys.path.insert(0, src_dir)

from src.core.database import get_mongo_client
from src.config import MONGO_ASSET_OUTPUT_COLLECTION
from src.core.equity_irr import calculate_equity_irr, xirr

def diagnose_asset1_irr(scenario_id=None):
    print("=== ASSET 1 IRR DIAGNOSTIC ===")
    print(f"Expected: Templers BESS, 111MW, $238.6M CAPEX")
    
    client = None
    try:
        client = get_mongo_client()
        db = client.get_database()
        collection = db[MONGO_ASSET_OUTPUT_COLLECTION]
        
        # If no scenario_id provided, find the best one
        if scenario_id is None:
            print("No scenario specified, searching for available data...")
            
            # Get all scenarios with Asset 1 data
            all_scenarios = collection.distinct("scenario_id")
            asset1_scenarios = []
            
            for sid in all_scenarios:
                count = collection.count_documents({"scenario_id": sid, "asset_id": 1})
                if count > 0:
                    asset1_scenarios.append((sid, count))
            
            if not asset1_scenarios:
                print("ERROR: No scenarios found with Asset 1 data")
                print("Available scenarios:", all_scenarios)
                print("You may need to run: python src/main.py")
                return
            
            # Use the scenario with the most Asset 1 records
            scenario_id = max(asset1_scenarios, key=lambda x: x[1])[0]
            print(f"Auto-selected scenario: {scenario_id}")
            
            # Show all available options
            print("Available scenarios with Asset 1:")
            for sid, count in sorted(asset1_scenarios, key=lambda x: x[1], reverse=True):
                marker = " (SELECTED)" if sid == scenario_id else ""
                print(f"  {sid}: {count} records{marker}")
        
        print(f"\nAnalyzing scenario: {scenario_id}")
        
        # Get Asset 1 data
        query = {"scenario_id": scenario_id, "asset_id": 1}
        asset1_data = list(collection.find(query))
        
        if not asset1_data:
            print(f"ERROR: No data found for Asset 1 in scenario: {scenario_id}")
            
            # Show what data we do have
            total_docs = collection.count_documents({"scenario_id": scenario_id})
            print(f"Total documents in scenario: {total_docs}")
            
            if total_docs > 0:
                available_assets = collection.distinct("asset_id", {"scenario_id": scenario_id})
                print(f"Available assets: {sorted(available_assets)}")
            
            return
        
        df = pd.DataFrame(asset1_data)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        
        print(f"\nFound {len(df)} records from {df['date'].min()} to {df['date'].max()}")
        
        # 1. CONSTRUCTION PERIOD ANALYSIS
        print("\n=== CONSTRUCTION PERIOD ANALYSIS ===")
        construction_start = pd.to_datetime('2024-03-01')
        operations_start = pd.to_datetime('2025-08-01')
        
        construction_df = df[(df['date'] >= construction_start) & (df['date'] < operations_start)]
        print(f"Construction periods: {len(construction_df)}")
        print(f"Expected: 17 months from 2024-03 to 2025-07")
        
        if not construction_df.empty:
            total_capex = construction_df['capex'].sum()
            total_equity_capex = construction_df['equity_capex'].sum()
            total_debt_capex = construction_df['debt_capex'].sum()
            
            print(f"Total CAPEX: ${total_capex:.2f}M (expected: $238.6M)")
            print(f"Equity CAPEX: ${total_equity_capex:.2f}M")
            print(f"Debt CAPEX: ${total_debt_capex:.2f}M")
            print(f"Gearing: {total_debt_capex/total_capex:.1%}" if total_capex > 0 else "N/A")
            
            # Check for CAPEX multiplier effect
            expected_monthly_capex = 238.6 / 17
            actual_monthly_capex = total_capex / len(construction_df) if len(construction_df) > 0 else 0
            multiplier_effect = actual_monthly_capex / expected_monthly_capex if expected_monthly_capex > 0 else 0
            
            print(f"Expected monthly CAPEX: ${expected_monthly_capex:.2f}M")
            print(f"Actual monthly CAPEX: ${actual_monthly_capex:.2f}M")
            print(f"Apparent multiplier: {multiplier_effect:.3f}x")
            
            if abs(multiplier_effect - 1.0) > 0.01:
                print(f"⚠️  WARNING: CAPEX appears to have a {multiplier_effect:.3f}x multiplier!")
                print(f"   This could be from scenario application or debt sizing issues")
            
            # Monthly CAPEX pattern
            print("\nMonthly CAPEX pattern:")
            for _, row in construction_df[['date', 'capex', 'equity_capex', 'debt_capex']].head(6).iterrows():
                print(f"  {row['date'].strftime('%Y-%m')}: CAPEX=${row['capex']:.2f}M, Equity=${row['equity_capex']:.2f}M, Debt=${row['debt_capex']:.2f}M")
        
        # 2. OPERATIONS PERIOD ANALYSIS
        print("\n=== OPERATIONS PERIOD ANALYSIS ===")
        operations_df = df[df['date'] >= operations_start]
        print(f"Operations periods: {len(operations_df)}")
        
        if not operations_df.empty:
            # First 6 months of operations
            first_ops = operations_df.head(6)
            print("\nFirst 6 months operations:")
            for _, row in first_ops.iterrows():
                print(f"  {row['date'].strftime('%Y-%m')}: Rev=${row.get('revenue', 0):.2f}M, OPEX=${row.get('opex', 0):.2f}M, Interest=${row.get('interest', 0):.2f}M, Principal=${row.get('principal', 0):.2f}M, Equity CF=${row.get('equity_cash_flow', 0):.2f}M")
            
            # Key metrics
            total_revenue = operations_df['revenue'].sum() if 'revenue' in operations_df.columns else 0
            total_opex = operations_df['opex'].sum() if 'opex' in operations_df.columns else 0
            total_cfads = operations_df['cfads'].sum() if 'cfads' in operations_df.columns else 0
            total_equity_cf = operations_df['equity_cash_flow'].sum() if 'equity_cash_flow' in operations_df.columns else 0
            total_interest = operations_df['interest'].sum() if 'interest' in operations_df.columns else 0
            total_principal = operations_df['principal'].sum() if 'principal' in operations_df.columns else 0
            
            print(f"\nOperations totals:")
            print(f"  Revenue: ${total_revenue:.2f}M")
            print(f"  OPEX: ${total_opex:.2f}M")
            print(f"  CFADS: ${total_cfads:.2f}M")
            print(f"  Interest: ${total_interest:.2f}M")
            print(f"  Principal: ${total_principal:.2f}M")
            print(f"  Equity Cash Flow: ${total_equity_cf:.2f}M")
            
            # Expected monthly revenue check
            expected_monthly_volume = 293  # MWh daily
            expected_monthly_revenue = (expected_monthly_volume * 30.4375 * 23) / 1000  # Approximate
            actual_monthly_revenue = total_revenue / len(operations_df) if len(operations_df) > 0 else 0
            
            print(f"\nRevenue validation:")
            print(f"  Expected monthly revenue: ~${expected_monthly_revenue:.2f}M")
            print(f"  Actual monthly revenue: ${actual_monthly_revenue:.2f}M")
        
        # 3. TERMINAL VALUE ANALYSIS
        print("\n=== TERMINAL VALUE ANALYSIS ===")
        terminal_periods = df[df['terminal_value'] > 0] if 'terminal_value' in df.columns else pd.DataFrame()
        if not terminal_periods.empty:
            for _, row in terminal_periods.iterrows():
                print(f"  {row['date'].strftime('%Y-%m')}: Terminal Value=${row['terminal_value']:.2f}M")
                print(f"    Expected: $51M at end of 25-year asset life")
        else:
            print("  No terminal value found")
            print("  Expected: $51M terminal value")
        
        # 4. IRR CALCULATION STEP-BY-STEP
        print("\n=== IRR CALCULATION ANALYSIS ===")
        
        # Check period type assignments
        if 'period_type' in df.columns:
            period_counts = df['period_type'].value_counts()
            print("Period type distribution:")
            for period_type, count in period_counts.items():
                period_name = {'C': 'Construction', 'O': 'Operations', '': 'Unassigned'}.get(period_type, f'Unknown ({period_type})')
                print(f"  {period_name}: {count} periods")
        else:
            print("WARNING: No period_type column found")
        
        # Filter for C+O periods (Construction + Operations)
        if 'period_type' in df.columns:
            co_df = df[df['period_type'].isin(['C', 'O'])].copy()
        else:
            # Fallback: use all periods
            co_df = df.copy()
        
        print(f"C+O periods: {len(co_df)}")
        
        # Non-zero equity cash flows
        if 'equity_cash_flow' in co_df.columns:
            nonzero_df = co_df[co_df['equity_cash_flow'] != 0].copy()
            print(f"Non-zero equity cash flows: {len(nonzero_df)}")
            
            if not nonzero_df.empty:
                # Group by date and sum
                irr_df = nonzero_df.groupby('date')['equity_cash_flow'].sum().reset_index()
                print(f"Grouped periods: {len(irr_df)}")
                
                # Show cash flow pattern
                print("\nEquity cash flow pattern:")
                print("  First 10 periods:")
                for _, row in irr_df.head(10).iterrows():
                    cf_type = "OUTFLOW" if row['equity_cash_flow'] < 0 else "INFLOW"
                    print(f"    {row['date'].strftime('%Y-%m')}: ${row['equity_cash_flow']:.2f}M ({cf_type})")
                
                print("  Last 5 periods:")
                for _, row in irr_df.tail().iterrows():
                    cf_type = "OUTFLOW" if row['equity_cash_flow'] < 0 else "INFLOW"
                    print(f"    {row['date'].strftime('%Y-%m')}: ${row['equity_cash_flow']:.2f}M ({cf_type})")
                
                # Cash flow summary
                total_cf = irr_df['equity_cash_flow'].sum()
                negative_cf = irr_df[irr_df['equity_cash_flow'] < 0]['equity_cash_flow'].sum()
                positive_cf = irr_df[irr_df['equity_cash_flow'] > 0]['equity_cash_flow'].sum()
                
                print(f"\nCash flow summary:")
                print(f"  Total: ${total_cf:.2f}M")
                print(f"  Outflows: ${negative_cf:.2f}M")
                print(f"  Inflows: ${positive_cf:.2f}M")
                print(f"  Net: ${positive_cf + negative_cf:.2f}M")
                
                # IRR calculation
                print("\nCalculating XIRR...")
                irr_result = calculate_equity_irr(irr_df)
                
                if not pd.isna(irr_result):
                    print(f"Asset 1 XIRR: {irr_result:.4f} ({irr_result:.2%})")
                    print(f"Previous IRR was presumably: {irr_result + 0.01:.2%} (1% higher)")
                else:
                    print("XIRR calculation failed")
                    
                    # Manual XIRR debugging
                    dates = irr_df['date'].tolist()
                    cash_flows = irr_df['equity_cash_flow'].tolist()
                    
                    print(f"\nManual XIRR debugging:")
                    print(f"  Cash flows: {len(cash_flows)}")
                    print(f"  Total: ${sum(cash_flows):.2f}M")
                    print(f"  Range: ${min(cash_flows):.2f}M to ${max(cash_flows):.2f}M")
                    
                    # Check for sign changes
                    signs = [1 if cf > 0 else -1 for cf in cash_flows if abs(cf) > 0.001]
                    sign_changes = sum(1 for i in range(1, len(signs)) if signs[i] != signs[i-1])
                    print(f"  Sign changes: {sign_changes}")
                    
                    # Try simple IRR if XIRR fails
                    try:
                        import numpy_financial as npf
                        simple_irr = npf.irr(cash_flows)
                        if not np.isnan(simple_irr):
                            print(f"  Simple IRR (ignoring dates): {simple_irr:.2%}")
                        else:
                            print("  Simple IRR also failed")
                    except Exception as e:
                        print(f"  Simple IRR error: {e}")
            else:
                print("No non-zero equity cash flows found")
        else:
            print("ERROR: No equity_cash_flow column found")
        
        # 5. COMPARATIVE ANALYSIS
        print("\n=== COMPARATIVE ANALYSIS ===")
        
        # Compare key metrics that could affect IRR
        print("Key IRR drivers:")
        
        if not construction_df.empty:
            total_equity_investment = construction_df['equity_capex'].sum()
            print(f"  Total equity investment: ${total_equity_investment:.2f}M")
            
            if total_equity_investment != 0:
                # Calculate simple payback if we have operations cash flows
                if not operations_df.empty and 'equity_cash_flow' in operations_df.columns:
                    avg_annual_equity_cf = operations_df['equity_cash_flow'].mean() * 12
                    simple_payback = total_equity_investment / avg_annual_equity_cf if avg_annual_equity_cf > 0 else float('inf')
                    print(f"  Average annual equity cash flow: ${avg_annual_equity_cf:.2f}M")
                    print(f"  Simple payback: {simple_payback:.1f} years")
        
        # 6. POTENTIAL ISSUES IDENTIFICATION
        print("\n=== POTENTIAL ISSUES IDENTIFICATION ===")
        
        issues_found = []
        
        # Check for CAPEX multiplier
        if not construction_df.empty:
            total_capex = construction_df['capex'].sum()
            expected_capex = 238.6
            if abs(total_capex - expected_capex) > 1.0:
                multiplier = total_capex / expected_capex
                issues_found.append(f"CAPEX multiplier detected: {multiplier:.3f}x (${total_capex:.1f}M vs ${expected_capex:.1f}M expected)")
        
        # Check for missing terminal value
        if terminal_periods.empty:
            issues_found.append("Terminal value missing (expected $51M)")
        
        # Check for revenue issues
        if not operations_df.empty and 'revenue' in operations_df.columns:
            total_revenue = operations_df['revenue'].sum()
            if total_revenue < 10:  # Suspiciously low
                issues_found.append(f"Revenue very low: ${total_revenue:.2f}M total")
        
        # Check for period type issues
        if 'period_type' in df.columns:
            unassigned = df[df['period_type'] == ''].shape[0]
            if unassigned > 0:
                issues_found.append(f"{unassigned} periods have unassigned period_type")
        
        if issues_found:
            print("Issues detected:")
            for issue in issues_found:
                print(f"  ⚠️  {issue}")
        else:
            print("No obvious issues detected")
        
        # 7. DEBUGGING RECOMMENDATIONS
        print("\n=== DEBUGGING RECOMMENDATIONS ===")
        print("1. Check for CAPEX scenario double-application:")
        print("   - Compare CAPEX in base case vs scenarios")
        print("   - Verify apply_post_debt_sizing_capex_scenarios() is working correctly")
        print("\n2. Verify debt sizing changes:")
        print("   - Compare debt amounts between runs")
        print("   - Check if gearing calculations changed")
        print("\n3. Check terminal value timing:")
        print("   - Verify terminal value appears at correct date")
        print("   - Confirm terminal value amount is correct")
        print("\n4. Compare revenue calculations:")
        print("   - Check if price scenarios are being applied correctly")
        print("   - Verify volume adjustments")
        print("\n5. Run side-by-side comparison:")
        print("   - Export both old and new cash flows")
        print("   - Compare month-by-month differences")
        
        return df
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        if client:
            client.close()

def compare_scenarios(scenario1="old_base", scenario2="sensitivity_results_base"):
    """Compare Asset 1 between two scenarios to identify differences"""
    print(f"\n=== COMPARING SCENARIOS: {scenario1} vs {scenario2} ===")
    
    client = None
    try:
        client = get_mongo_client()
        db = client.get_database()
        collection = db[MONGO_ASSET_OUTPUT_COLLECTION]
        
        # Get data for both scenarios
        data1 = list(collection.find({"scenario_id": scenario1, "asset_id": 1}))
        data2 = list(collection.find({"scenario_id": scenario2, "asset_id": 1}))
        
        if not data1:
            print(f"No data found for scenario: {scenario1}")
            return
        if not data2:
            print(f"No data found for scenario: {scenario2}")
            return
        
        df1 = pd.DataFrame(data1)
        df2 = pd.DataFrame(data2)
        
        # Merge on date for comparison
        df1['date'] = pd.to_datetime(df1['date'])
        df2['date'] = pd.to_datetime(df2['date'])
        
        merged = pd.merge(df1[['date', 'capex', 'equity_capex', 'revenue', 'equity_cash_flow']], 
                         df2[['date', 'capex', 'equity_capex', 'revenue', 'equity_cash_flow']], 
                         on='date', suffixes=('_old', '_new'))
        
        # Calculate differences
        merged['capex_diff'] = merged['capex_new'] - merged['capex_old']
        merged['equity_capex_diff'] = merged['equity_capex_new'] - merged['equity_capex_old']
        merged['revenue_diff'] = merged['revenue_new'] - merged['revenue_old']
        merged['equity_cf_diff'] = merged['equity_cash_flow_new'] - merged['equity_cash_flow_old']
        
        # Show significant differences
        print("Significant differences (>$0.1M):")
        significant = merged[
            (abs(merged['capex_diff']) > 0.1) | 
            (abs(merged['revenue_diff']) > 0.1) | 
            (abs(merged['equity_cf_diff']) > 0.1)
        ]
        
        for _, row in significant.head(10).iterrows():
            print(f"  {row['date'].strftime('%Y-%m')}:")
            if abs(row['capex_diff']) > 0.1:
                print(f"    CAPEX: ${row['capex_old']:.2f}M -> ${row['capex_new']:.2f}M (diff: ${row['capex_diff']:.2f}M)")
            if abs(row['revenue_diff']) > 0.1:
                print(f"    Revenue: ${row['revenue_old']:.2f}M -> ${row['revenue_new']:.2f}M (diff: ${row['revenue_diff']:.2f}M)")
            if abs(row['equity_cf_diff']) > 0.1:
                print(f"    Equity CF: ${row['equity_cash_flow_old']:.2f}M -> ${row['equity_cash_flow_new']:.2f}M (diff: ${row['equity_cf_diff']:.2f}M)")
        
        # Summary of total differences
        print(f"\nTotal differences:")
        print(f"  CAPEX: ${merged['capex_diff'].sum():.2f}M")
        print(f"  Revenue: ${merged['revenue_diff'].sum():.2f}M")
        print(f"  Equity Cash Flow: ${merged['equity_cf_diff'].sum():.2f}M")
        
    except Exception as e:
        print(f"Comparison error: {e}")
    finally:
        if client:
            client.close()

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description="Diagnose Asset 1 IRR calculation")
    parser.add_argument('--scenario', type=str, 
                       help='Scenario ID to analyze (if not provided, will auto-detect)')
    parser.add_argument('--compare', type=str, 
                       help='Compare with another scenario')
    
    args = parser.parse_args()
    
    # First, find available scenarios if no scenario specified
    if not args.scenario:
        print("=== FINDING AVAILABLE SCENARIOS ===")
        # Run the scenario finder logic inline
        client = None
        try:
            client = get_mongo_client()
            db = client.get_database()
            collection = db[MONGO_ASSET_OUTPUT_COLLECTION]
            
            all_scenarios = collection.distinct("scenario_id")
            if all_scenarios:
                print(f"Available scenarios: {all_scenarios}")
            else:
                print("No scenarios found. Run 'python src/main.py' first.")
                exit(1)
        except Exception as e:
            print(f"Error checking scenarios: {e}")
            exit(1)
        finally:
            if client:
                client.close()
    
    df = diagnose_asset1_irr(args.scenario)
    
    if args.compare:
        compare_scenarios(args.compare, args.scenario or "auto-detected")
    
    if df is not None:
        print("\n=== DIAGNOSTIC COMPLETE ===")
        print("Check output above for IRR calculation issues")
        print("\nNext steps:")
        print("1. Save this output and compare with previous run")
        print("2. Check if CAPEX multiplier is being applied correctly")
        print("3. Verify debt sizing hasn't changed unexpectedly")
        print("4. Confirm terminal value timing and amount")
    else:
        print("\n=== DIAGNOSTIC FAILED ===")
        print("Make sure you've run the main model first:")
        print("  python src/main.py")
