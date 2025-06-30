# backend/core/equity_irr.py

import numpy as np
import pandas as pd
from datetime import datetime
from scipy.optimize import fsolve
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
        if rate == -1:  # Avoid division by zero
            pv = cf * 0 if years_diff > 0 else cf
        else:
            pv = cf / ((1 + rate) ** years_diff)
        
        npv += pv
    
    return npv

def xirr(cash_flows, dates, guess=0.1, max_iterations=1000, tolerance=1e-6):
    """
    Calculate Internal Rate of Return with irregular dates (XIRR equivalent).
    
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
        return float('nan')
    
    if len(cash_flows) != len(dates):
        return float('nan')
    
    if len(cash_flows) < 2:
        return float('nan')
    
    # Remove zero cash flows and corresponding dates
    non_zero_pairs = [(cf, date) for cf, date in zip(cash_flows, dates) if cf != 0]
    
    if len(non_zero_pairs) < 2:
        return float('nan')
    
    cash_flows_clean, dates_clean = zip(*non_zero_pairs)
    cash_flows_clean = list(cash_flows_clean)
    dates_clean = list(dates_clean)
    
    # Check for sign changes (required for IRR to exist)
    signs = [1 if cf > 0 else -1 if cf < 0 else 0 for cf in cash_flows_clean]
    if len(set(signs)) <= 1:
        return float('nan')
    
    # Define the function to find root of (XNPV = 0)
    def npv_function(rate):
        return xnpv(rate, cash_flows_clean, dates_clean)
    
    try:
        # Use scipy's fsolve to find the rate where NPV = 0
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = fsolve(npv_function, guess, maxfev=max_iterations, xtol=tolerance)
            irr_result = result[0]
        
        # Validate the result
        if abs(npv_function(irr_result)) < tolerance and -1 < irr_result < 100:  # Reasonable bounds
            return irr_result
        else:
            return float('nan')
            
    except Exception as e:
        print(f"Error calculating XIRR: {e}")
        return float('nan')

def calculate_equity_irr(cash_flow_df):
    """
    Calculates the Equity Internal Rate of Return (IRR) using XIRR methodology.
    
    Args:
        cash_flow_df (pd.DataFrame): DataFrame with columns 'date' and 'equity_cash_flow'
                                   or list of equity cash flows (legacy support)
    
    Returns:
        float: The Equity IRR as a decimal (e.g., 0.10 for 10%).
               Returns NaN if IRR cannot be calculated.
    """
    
    # Handle legacy input (list of cash flows without dates)
    if isinstance(cash_flow_df, (list, np.ndarray)):
        print("Warning: Using legacy IRR calculation. Consider providing dates for XIRR.")
        if not cash_flow_df or len(cash_flow_df) < 2:
            return float('nan')
        
        if all(cf == 0 for cf in cash_flow_df):
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
        return float('nan')
    
    if 'date' not in cash_flow_df.columns or 'equity_cash_flow' not in cash_flow_df.columns:
        print("Error: DataFrame must contain 'date' and 'equity_cash_flow' columns")
        return float('nan')
    
    if cash_flow_df.empty:
        return float('nan')
    
    # Prepare data for XIRR calculation
    df = cash_flow_df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    
    # Group by date and sum cash flows (in case of multiple entries per date)
    df_grouped = df.groupby('date')['equity_cash_flow'].sum().reset_index()
    
    dates = df_grouped['date'].tolist()
    cash_flows = df_grouped['equity_cash_flow'].tolist()
    
    # Calculate XIRR
    irr_result = xirr(cash_flows, dates)
    
    if not np.isnan(irr_result):
        print(f"Calculated Equity XIRR: {irr_result:.2%}")
        return irr_result
    else:
        print("Could not calculate Equity XIRR - insufficient or invalid cash flow data")
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