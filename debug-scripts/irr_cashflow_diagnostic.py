# debug-scripts/irr_cashflow_diagnostic.py

"""
Diagnostic script to analyze IRR calculation and identify issues causing high IRRs.
Extracts actual cash flows used in IRR calculation and validates the calculation.
"""

import pandas as pd
import numpy as np
from datetime import datetime
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
from src.core.equity_irr import calculate_equity_irr, xnpv, xirr


def analyze_irr_cashflows(scenario_id=None, asset_id=None):
    """
    Analyze IRR calculation for a specific asset or portfolio.
    
    Args:
        scenario_id: Scenario ID to analyze (if None, uses most recent)
        asset_id: Asset ID to analyze (if None, analyzes portfolio)
    """
    print("=" * 80)
    print("IRR CASHFLOW DIAGNOSTIC")
    print("=" * 80)
    
    client = None
    try:
        client = get_mongo_client()
        db = client.get_database()
        collection = db[MONGO_ASSET_OUTPUT_COLLECTION]
        
        # Find scenario if not provided
        if scenario_id is None:
            all_scenarios = collection.distinct("scenario_id")
            if not all_scenarios:
                print("ERROR: No scenarios found. Run the model first.")
                return
            scenario_id = all_scenarios[-1]  # Use most recent
            print(f"Using scenario: {scenario_id}")
        
        # Build query
        query = {"scenario_id": scenario_id}
        if asset_id is not None:
            query["asset_id"] = asset_id
            print(f"\nAnalyzing Asset {asset_id} in scenario {scenario_id}")
        else:
            print(f"\nAnalyzing Portfolio in scenario {scenario_id}")
        
        # Get data
        data = list(collection.find(query))
        if not data:
            print(f"ERROR: No data found for query: {query}")
            return
        
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values(['asset_id', 'date'] if 'asset_id' in df.columns else 'date')
        
        print(f"\nFound {len(df)} records")
        print(f"Date range: {df['date'].min()} to {df['date'].max()}")
        
        # Analyze cash flow components
        print("\n" + "=" * 80)
        print("CASH FLOW COMPONENTS ANALYSIS")
        print("=" * 80)
        
        # Check required columns
        required_cols = ['date', 'equity_cash_flow_pre_distributions', 'equity_capex', 
                        'cfads', 'interest', 'principal', 'tax_expense', 'terminal_value']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            print(f"WARNING: Missing columns: {missing_cols}")
            return
        
        # Filter for Construction + Operations periods
        if 'period_type' in df.columns:
            co_df = df[df['period_type'].isin(['C', 'O'])].copy()
            print(f"\nConstruction + Operations periods: {len(co_df)}")
        else:
            co_df = df.copy()
            print("\nWARNING: No period_type column found, using all periods")
        
        # Include terminal value periods
        terminal_periods = df[
            (df['terminal_value'] > 0) & 
            (~df.index.isin(co_df.index if 'period_type' in df.columns else []))
        ].copy()
        
        if not terminal_periods.empty:
            print(f"Terminal value periods: {len(terminal_periods)}")
            co_df = pd.concat([co_df, terminal_periods], ignore_index=True)
        
        # Filter for non-zero cash flows or terminal value
        equity_irr_df = co_df[
            (co_df['equity_cash_flow_pre_distributions'] != 0) | 
            (co_df['terminal_value'] > 0)
        ].copy()
        
        print(f"Non-zero equity cash flow periods: {len(equity_irr_df)}")
        
        if asset_id is not None:
            # For single asset, group by date
            equity_irr_summary = equity_irr_df.groupby('date')['equity_cash_flow_pre_distributions'].sum().reset_index()
        else:
            # For portfolio, group by date across all assets
            equity_irr_summary = equity_irr_df.groupby('date')['equity_cash_flow_pre_distributions'].sum().reset_index()
        
        equity_irr_summary = equity_irr_summary.rename(columns={'equity_cash_flow_pre_distributions': 'equity_cash_flow'})
        equity_irr_summary = equity_irr_summary.sort_values('date')
        
        print(f"\nGrouped cash flows: {len(equity_irr_summary)} periods")
        
        # Show cash flow breakdown
        print("\n" + "-" * 80)
        print("CASH FLOW BREAKDOWN")
        print("-" * 80)
        
        # Calculate totals
        total_equity_cf = equity_irr_summary['equity_cash_flow'].sum()
        negative_cf = equity_irr_summary[equity_irr_summary['equity_cash_flow'] < 0]['equity_cash_flow'].sum()
        positive_cf = equity_irr_summary[equity_irr_summary['equity_cash_flow'] > 0]['equity_cash_flow'].sum()
        
        print(f"\nTotal Equity Cash Flow: ${total_equity_cf:,.2f}M")
        print(f"  Outflows (negative): ${negative_cf:,.2f}M")
        print(f"  Inflows (positive): ${positive_cf:,.2f}M")
        print(f"  Net: ${positive_cf + negative_cf:,.2f}M")
        
        # Show first 10 and last 5 periods
        print("\nFirst 10 periods:")
        for idx, row in equity_irr_summary.head(10).iterrows():
            cf_type = "OUTFLOW" if row['equity_cash_flow'] < 0 else "INFLOW"
            print(f"  {row['date'].strftime('%Y-%m-%d')}: ${row['equity_cash_flow']:,.2f}M ({cf_type})")
        
        print("\nLast 5 periods:")
        for idx, row in equity_irr_summary.tail(5).iterrows():
            cf_type = "OUTFLOW" if row['equity_cash_flow'] < 0 else "INFLOW"
            print(f"  {row['date'].strftime('%Y-%m-%d')}: ${row['equity_cash_flow']:,.2f}M ({cf_type})")
        
        # Analyze components
        print("\n" + "-" * 80)
        print("COMPONENT ANALYSIS")
        print("-" * 80)
        
        if asset_id is not None:
            component_df = equity_irr_df.groupby('date').agg({
                'equity_capex': 'sum',
                'cfads': 'sum',
                'interest': 'sum',
                'principal': 'sum',
                'tax_expense': 'sum',
                'terminal_value': 'sum',
                'equity_cash_flow_pre_distributions': 'sum'
            }).reset_index()
        else:
            component_df = equity_irr_df.groupby('date').agg({
                'equity_capex': 'sum',
                'cfads': 'sum',
                'interest': 'sum',
                'principal': 'sum',
                'tax_expense': 'sum',
                'terminal_value': 'sum',
                'equity_cash_flow_pre_distributions': 'sum'
            }).reset_index()
        
        total_equity_capex = component_df['equity_capex'].sum()
        total_cfads = component_df['cfads'].sum()
        total_interest = component_df['interest'].sum()
        total_principal = component_df['principal'].sum()
        total_tax = component_df['tax_expense'].sum()
        total_terminal = component_df['terminal_value'].sum()
        
        print(f"\nTotal Equity CAPEX: ${total_equity_capex:,.2f}M")
        print(f"Total CFADS: ${total_cfads:,.2f}M")
        print(f"Total Interest: ${total_interest:,.2f}M")
        print(f"Total Principal: ${total_principal:,.2f}M")
        print(f"Total Tax: ${total_tax:,.2f}M")
        print(f"Total Terminal Value: ${total_terminal:,.2f}M")
        
        # Verify formula: equity_cash_flow_pre_distributions = CFADS - interest - principal - equity_capex - tax + terminal_value
        calculated_ecf = (
            total_cfads - 
            total_interest - 
            total_principal - 
            total_equity_capex - 
            total_tax + 
            total_terminal
        )
        actual_ecf = component_df['equity_cash_flow_pre_distributions'].sum()
        
        print(f"\nFormula verification:")
        print(f"  Calculated: ${calculated_ecf:,.2f}M")
        print(f"  Actual: ${actual_ecf:,.2f}M")
        print(f"  Difference: ${abs(calculated_ecf - actual_ecf):,.2f}M")
        
        if abs(calculated_ecf - actual_ecf) > 0.01:
            print("  ⚠️  WARNING: Formula mismatch detected!")
        
        # Calculate IRR
        print("\n" + "-" * 80)
        print("IRR CALCULATION")
        print("-" * 80)
        
        irr_result = calculate_equity_irr(equity_irr_summary)
        
        if not np.isnan(irr_result):
            print(f"\nCalculated IRR: {irr_result:.4f} ({irr_result:.2%})")
            
            # Validate IRR by calculating NPV at the IRR rate
            dates = equity_irr_summary['date'].tolist()
            cash_flows = equity_irr_summary['equity_cash_flow'].tolist()
            npv_at_irr = xnpv(irr_result, cash_flows, dates)
            print(f"NPV at IRR: ${npv_at_irr:,.2f}M (should be ~0)")
            
            if abs(npv_at_irr) > 0.01:
                print(f"  ⚠️  WARNING: NPV at IRR is not zero! This suggests calculation error.")
            
            # Calculate NPV at various discount rates
            print("\nNPV at various discount rates:")
            for rate in [0.05, 0.10, 0.15, 0.20, 0.25]:
                npv = xnpv(rate, cash_flows, dates)
                print(f"  {rate:.0%}: ${npv:,.2f}M")
        else:
            print("\n⚠️  Could not calculate IRR")
        
        # Identify potential issues
        print("\n" + "=" * 80)
        print("POTENTIAL ISSUES IDENTIFICATION")
        print("=" * 80)
        
        issues = []
        
        # Check if equity CAPEX is properly included
        if total_equity_capex == 0 and asset_id is not None:
            issues.append("No equity CAPEX found - equity contributions may be missing")
        
        # Check for sign changes
        signs = [1 if cf > 0 else -1 for cf in cash_flows if abs(cf) > 1e-10]
        sign_changes = sum(1 for i in range(1, len(signs)) if signs[i] != signs[i-1])
        if sign_changes < 1:
            issues.append("No sign changes in cash flows - IRR may not be meaningful")
        
        # Check if terminal value is included
        if total_terminal == 0:
            issues.append("No terminal value found - may be missing from calculation")
        
        # Check for timing issues
        first_negative = equity_irr_summary[equity_irr_summary['equity_cash_flow'] < 0]
        if first_negative.empty:
            issues.append("No negative cash flows found - equity contributions may be missing")
        else:
            first_negative_date = first_negative['date'].min()
            print(f"\nFirst negative cash flow: {first_negative_date.strftime('%Y-%m-%d')}")
        
        # Check if equity CAPEX timing matches negative cash flows
        if asset_id is not None:
            equity_capex_periods = equity_irr_df[equity_irr_df['equity_capex'] > 0]
            if not equity_capex_periods.empty:
                capex_dates = equity_capex_periods['date'].tolist()
                negative_dates = equity_irr_summary[equity_irr_summary['equity_cash_flow'] < 0]['date'].tolist()
                
                # Check if equity CAPEX dates align with negative cash flows
                capex_in_negative = sum(1 for d in capex_dates if d in negative_dates)
                if capex_in_negative < len(capex_dates) * 0.8:  # 80% threshold
                    issues.append(f"Equity CAPEX dates may not align with negative cash flows ({capex_in_negative}/{len(capex_dates)} match)")
        
        if issues:
            print("\nIssues found:")
            for issue in issues:
                print(f"  ⚠️  {issue}")
        else:
            print("\n✓ No obvious issues detected")
        
        return equity_irr_summary, irr_result
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return None, None
    finally:
        if client:
            client.close()


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description="Diagnose IRR calculation")
    parser.add_argument('--scenario', type=str, help='Scenario ID to analyze')
    parser.add_argument('--asset', type=int, help='Asset ID to analyze (if not provided, analyzes portfolio)')
    
    args = parser.parse_args()
    
    analyze_irr_cashflows(scenario_id=args.scenario, asset_id=args.asset)


