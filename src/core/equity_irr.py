# backend/core/equity_irr.py

import numpy as np
import pandas as pd
from datetime import datetime
from scipy.optimize import fsolve, brentq
import warnings

def xnpv(rate, cash_flows, dates):
    """
    Calculate Net Present Value with irregular dates (XNPV equivalent).
    
    Args:
        rate (float): Discount rate
        cash_flows (list): List of cash flows
        dates (list): List of dates corresponding to cash flows
    
    Returns:
        float: Net Present Value
    """
    if len(cash_flows) != len(dates):
        raise ValueError("Cash flows and dates must have the same length")
    
    if len(cash_flows) == 0:
        return 0.0
    
    # Convert dates to datetime if they aren't already
    dates = [pd.to_datetime(d) if not isinstance(d, datetime) else d for d in dates]
    
    # Use first date as reference point
    reference_date = dates[0]
    
    npv = 0.0
    for cf, date in zip(cash_flows, dates):
        # Calculate days difference from reference date
        days_diff = (date - reference_date).days
        years_diff = days_diff / 365.25  # Account for leap years
        
        # Calculate present value
        if rate <= -1:  # Avoid division by zero and negative bases
            if years_diff > 0:
                return float('inf') if cf > 0 else float('-inf')
            else:
                pv = cf
        else:
            pv = cf / ((1 + rate) ** years_diff)
        
        npv += pv
    
    return npv

def xirr(cash_flows, dates, guess=0.1, max_iterations=1000, tolerance=1e-6):
    """
    Calculate Internal Rate of Return with irregular dates (XIRR equivalent).
    Improved version with better error handling and multiple solving methods.
    
    Args:
        cash_flows (list): List of cash flows
        dates (list): List of dates corresponding to cash flows
        guess (float): Initial guess for IRR
        max_iterations (int): Maximum number of iterations
        tolerance (float): Convergence tolerance
    
    Returns:
        float: Internal Rate of Return as a decimal (e.g., 0.10 for 10%)
               Returns NaN if IRR cannot be calculated
    """
    
    # Input validation
    if not cash_flows or not dates:
        print("DEBUG: Empty cash flows or dates")
        return float('nan')
    
    if len(cash_flows) != len(dates):
        print("DEBUG: Cash flows and dates length mismatch")
        return float('nan')
    
    if len(cash_flows) < 2:
        print("DEBUG: Need at least 2 cash flows")
        return float('nan')
    
    # Convert to lists if needed
    cash_flows = list(cash_flows)
    dates = list(dates)
    
   
    # DON'T remove zero cash flows - they preserve timing
    # Instead, check for meaningful cash flows
    non_zero_count = sum(1 for cf in cash_flows if abs(cf) > 1e-10)
    if non_zero_count < 2:
        print("DEBUG: Need at least 2 non-zero cash flows")
        return float('nan')
    
    # Check for sign changes (required for IRR to exist)
    signs = []
    for cf in cash_flows:
        if abs(cf) > 1e-10:  # Only consider non-trivial cash flows
            signs.append(1 if cf > 0 else -1)
    
    if len(set(signs)) <= 1:
        print("DEBUG: No sign changes in cash flows")
        return float('nan')
    
    
    # Define the function to find root of (XNPV = 0)
    def npv_function(rate):
        try:
            npv = xnpv(rate, cash_flows, dates)
            return npv
        except:
            return float('inf')
    
    # Test the function at a few points
    test_rates = [-0.99, 0.0, 0.1, 0.5, 1.0, 5.0]
    npv_values = []
    for test_rate in test_rates:
        try:
            npv = npv_function(test_rate)
            npv_values.append((test_rate, npv))
            #print(f"DEBUG: NPV at {test_rate:.1%}: {npv:.2f}")
        except:
            npv_values.append((test_rate, float('inf')))
    
    # Try multiple solving methods
    methods_to_try = []
    
    # Method 1: Brentq (most robust if we can find bounds)
    try:
        # Find bounds where NPV changes sign
        valid_npvs = [(r, n) for r, n in npv_values if np.isfinite(n)]
        if len(valid_npvs) >= 2:
            for i in range(len(valid_npvs) - 1):
                r1, n1 = valid_npvs[i]
                r2, n2 = valid_npvs[i + 1]
                if n1 * n2 < 0:  # Sign change
                    methods_to_try.append(('brentq', (r1, r2)))
                    break
    except:
        pass
    
    # Method 2: Multiple fsolve attempts with different starting points
    starting_points = [0.05, 0.1, 0.15, 0.2, 0.3, -0.5, -0.1]
    for start_point in starting_points:
        methods_to_try.append(('fsolve', start_point))
    
    # Try each method
    for method_name, method_params in methods_to_try:
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                
                if method_name == 'brentq':
                    a, b = method_params
                    result = brentq(npv_function, a, b, xtol=tolerance, maxiter=max_iterations)
                    irr_result = result
                else:  # fsolve
                    result = fsolve(npv_function, method_params, maxfev=max_iterations, xtol=tolerance)
                    irr_result = result[0]
                
                # Validate the result
                npv_check = npv_function(irr_result)
                if abs(npv_check) < tolerance and -0.99 < irr_result < 50:  # Expanded reasonable bounds
                  #  print(f"DEBUG: Found IRR using {method_name}: {irr_result:.4f} ({irr_result:.2%})")
                  #  print(f"DEBUG: NPV check: {npv_check:.6f}")
                    return irr_result
                else:
                    print(f"DEBUG: {method_name} result {irr_result:.4f} failed validation (NPV: {npv_check:.6f})")
                    
        except Exception as e:
            print(f"DEBUG: {method_name} failed: {e}")
            continue
    
    print("DEBUG: All IRR calculation methods failed")
    return float('nan')

def calculate_equity_irr(cash_flow_df):
    """
    Calculates the Equity Internal Rate of Return (IRR) using XIRR methodology.
    Improved version with better debugging and error handling.
    
    Args:
        cash_flow_df (pd.DataFrame): DataFrame with columns 'date' and 'equity_cash_flow'
                                   or list of equity cash flows (legacy support)
    
    Returns:
        float: The Equity IRR as a decimal (e.g., 0.10 for 10%).
               Returns NaN if IRR cannot be calculated.
    """
    
    print("\n=== EQUITY IRR CALCULATION DEBUG ===")
    
    # Handle legacy input (list of cash flows without dates)
    if isinstance(cash_flow_df, (list, np.ndarray)):
        print("Warning: Using legacy IRR calculation. Consider providing dates for XIRR.")
        if not cash_flow_df or len(cash_flow_df) < 2:
            return float('nan')
        
        if all(abs(cf) < 1e-10 for cf in cash_flow_df):
            return float('nan')
        
        try:
            import numpy_financial as npf
            irr = npf.irr(cash_flow_df)
            return irr if not np.isnan(irr) else float('nan')
        except Exception as e:
            print(f"Error calculating legacy IRR: {e}")
            return float('nan')
    
    # Handle DataFrame input (preferred method with dates)
    if not isinstance(cash_flow_df, pd.DataFrame):
        print("ERROR: Input must be DataFrame or list")
        return float('nan')
    
    if 'date' not in cash_flow_df.columns or 'equity_cash_flow' not in cash_flow_df.columns:
        print("ERROR: DataFrame must contain 'date' and 'equity_cash_flow' columns")
        return float('nan')
    
    if cash_flow_df.empty:
        print("ERROR: DataFrame is empty")
        return float('nan')
    
    # Prepare data for XIRR calculation
    df = cash_flow_df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    
    # Group by date and sum cash flows (in case of multiple entries per date)
    df_grouped = df.groupby('date')['equity_cash_flow'].sum().reset_index()
    
    # Filter out rows where equity_cash_flow is NaN
    df_grouped = df_grouped.dropna(subset=['equity_cash_flow'])
    
    if df_grouped.empty:
        print("ERROR: No valid equity cash flows after cleaning")
        return float('nan')
    
    dates = df_grouped['date'].tolist()
    cash_flows = df_grouped['equity_cash_flow'].tolist()
        
    # Calculate XIRR
    irr_result = xirr(cash_flows, dates)
    
    if not np.isnan(irr_result):
        print(f"SUCCESS: Calculated Equity XIRR: {irr_result:.4f} ({irr_result:.2%})")
        return irr_result
    else:
        print("FAILURE: Could not calculate Equity XIRR")
        
        # Additional debugging: show summary statistics
        print(f"DEBUG: Cash flow statistics:")
        print(f"  Count: {len(cash_flows)}")
        print(f"  Sum: {sum(cash_flows):.2f}")
        print(f"  Min: {min(cash_flows):.2f}")
        print(f"  Max: {max(cash_flows):.2f}")
        print(f"  Non-zero count: {sum(1 for cf in cash_flows if abs(cf) > 1e-10)}")
        
        return float('nan')

def calculate_project_irr(cash_flow_df):
    """
    Calculates the Project Internal Rate of Return using total project cash flows.
    Project IRR considers total project cash flows before debt service.
    
    Args:
        cash_flow_df (pd.DataFrame): DataFrame with columns 'date' and 'cfads'
    
    Returns:
        float: The Project IRR as a decimal (e.g., 0.10 for 10%).
    """
    if not isinstance(cash_flow_df, pd.DataFrame):
        return float('nan')
    
    if 'date' not in cash_flow_df.columns or 'cfads' not in cash_flow_df.columns:
        print("Error: DataFrame must contain 'date' and 'cfads' columns for Project IRR")
        return float('nan')
    
    if cash_flow_df.empty:
        return float('nan')
    
    # Prepare data
    df = cash_flow_df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    
    # Group by date and sum cash flows
    df_grouped = df.groupby('date')['cfads'].sum().reset_index()
    
    dates = df_grouped['date'].tolist()
    cash_flows = df_grouped['cfads'].tolist()
    
    # For project IRR, we need to add the initial CAPEX as negative cash flows
    # This should be handled by including equity_capex in the calculation
    
    irr_result = xirr(cash_flows, dates)
    
    if not np.isnan(irr_result):
        print(f"Calculated Project XIRR: {irr_result:.2%}")
        return irr_result
    else:
        print("Could not calculate Project XIRR")
        return float('nan')

def calculate_asset_equity_irrs(final_cash_flow_df):
    """
    Calculates the Equity IRR for each unique asset in the final_cash_flow_df.

    Args:
        final_cash_flow_df (pd.DataFrame): The consolidated cash flow DataFrame
                                           with 'date', 'asset_id', and 'equity_cash_flow' columns.

    Returns:
        dict: A dictionary where keys are asset_ids and values are their Equity IRRs.
              Returns NaN for assets where IRR cannot be calculated.
    """
    asset_irrs = {}
    if 'asset_id' not in final_cash_flow_df.columns:
        print("Warning: 'asset_id' column not found in cash flow DataFrame. Cannot calculate asset-level IRRs.")
        return asset_irrs

    unique_assets = final_cash_flow_df['asset_id'].unique()
    print(f"Calculating asset-level IRRs for {len(unique_assets)} assets...")

    for asset_id in unique_assets:
        # Filter cash flows for the current asset
        asset_df = final_cash_flow_df[final_cash_flow_df['asset_id'] == asset_id].copy()

        # Filter for Construction ('C') and Operations ('O') periods, and non-zero equity cash flows
        # This ensures consistency with the portfolio IRR calculation in main.py
        if 'period_type' in asset_df.columns:
            co_periods_df = asset_df[asset_df['period_type'].isin(['C', 'O'])].copy()
        else:
            # If period_type is not available, use all data for the asset
            co_periods_df = asset_df.copy()

        equity_irr_df = co_periods_df[co_periods_df['equity_cash_flow'] != 0].copy()

        if not equity_irr_df.empty:
            # Group by date to get total equity cash flows for this asset for each date
            equity_irr_summary = equity_irr_df.groupby('date')['equity_cash_flow'].sum().reset_index()
            irr = calculate_equity_irr(equity_irr_summary)
            asset_irrs[asset_id] = irr
            print(f"  Asset {asset_id} IRR: {irr:.2%}" if not pd.isna(irr) else f"  Asset {asset_id} IRR: Could not calculate")
        else:
            asset_irrs[asset_id] = float('nan')
            print(f"  Asset {asset_id} IRR: No equity cash flows found")

    return asset_irrs