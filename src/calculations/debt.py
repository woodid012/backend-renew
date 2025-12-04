# src/calculations/debt.py

import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
from ..config import DEFAULT_DEBT_REPAYMENT_FREQUENCY, DEFAULT_DEBT_GRACE_PERIOD, DEFAULT_DEBT_SIZING_METHOD, DSCR_CALCULATION_FREQUENCY

def calculate_blended_dscr(contracted_revenue, merchant_revenue, target_dscr_contract, target_dscr_merchant):
    """
    Calculate blended DSCR target based on revenue mix.
    
    Args:
        contracted_revenue (float): Annual contracted revenue
        merchant_revenue (float): Annual merchant revenue
        target_dscr_contract (float): Target DSCR for contracted revenue
        target_dscr_merchant (float): Target DSCR for merchant revenue
    
    Returns:
        float: Blended DSCR target
    """
    total_revenue = contracted_revenue + merchant_revenue
    
    if total_revenue == 0:
        return target_dscr_merchant
    
    contracted_share = contracted_revenue / total_revenue
    merchant_share = merchant_revenue / total_revenue
    
    return contracted_share * target_dscr_contract + merchant_share * target_dscr_merchant

def calculate_annual_debt_schedule(debt_amount, cash_flows, interest_rate, tenor_years, target_dscrs, period_frequency='annual'):
    """
    Calculate debt schedule using sculpting approach for annual or quarterly periods.
    
    Args:
        debt_amount (float): Initial debt amount in millions
        cash_flows (list): Operating cash flows (CFADS) in millions - annual or quarterly
        interest_rate (float): Annual interest rate
        tenor_years (int): Debt term in years
        target_dscrs (list): Target DSCR for each period
        period_frequency (str): 'annual' or 'quarterly' - determines period calculation
    
    Returns:
        dict: Complete debt schedule with metrics
    """
    # Determine number of periods based on frequency
    if period_frequency.lower() == 'quarterly':
        num_periods = tenor_years * 4
        period_rate = interest_rate / 4  # Quarterly interest rate
    else:
        num_periods = tenor_years
        period_rate = interest_rate  # Annual interest rate
    
    # Initialize arrays
    debt_balance = [0.0] * (num_periods + 1)
    interest_payments = [0.0] * num_periods
    principal_payments = [0.0] * num_periods
    debt_service = [0.0] * num_periods
    dscr_values = [0.0] * num_periods
    
    # Set initial debt balance
    debt_balance[0] = debt_amount
    
    # Calculate debt service for each period
    for period in range(num_periods):
        if period >= len(cash_flows):
            break
            
        # Interest payment on opening balance
        interest_payments[period] = debt_balance[period] * period_rate
        
        # Get available cash flow and target DSCR
        operating_cash_flow = cash_flows[period]
        target_dscr = target_dscrs[period] if period < len(target_dscrs) else target_dscrs[-1]
        
        # Maximum debt service allowed by DSCR constraint
        max_debt_service = operating_cash_flow / target_dscr if target_dscr > 0 else 0
        
        # Principal repayment (limited by max debt service and remaining balance)
        principal_payments[period] = min(
            max(0, max_debt_service - interest_payments[period]),
            debt_balance[period]
        )
        
        # Total debt service
        debt_service[period] = interest_payments[period] + principal_payments[period]
        
        # Calculate actual DSCR
        dscr_values[period] = operating_cash_flow / debt_service[period] if debt_service[period] > 0 else float('inf')
        
        # Update debt balance
        debt_balance[period + 1] = debt_balance[period] - principal_payments[period]
    
    # Calculate metrics
    fully_repaid = debt_balance[num_periods] < 0.001  # $1M tolerance
    avg_debt_service = sum(debt_service) / num_periods if num_periods > 0 else 0
    valid_dscrs = [d for d in dscr_values if d != float('inf') and d > 0]
    min_dscr = min(valid_dscrs) if valid_dscrs else 0
    
    return {
        'debt_balance': debt_balance,
        'interest_payments': interest_payments,
        'principal_payments': principal_payments,
        'debt_service': debt_service,
        'dscr_values': dscr_values,
        'metrics': {
            'fully_repaid': fully_repaid,
            'avg_debt_service': avg_debt_service,
            'min_dscr': min_dscr,
            'final_balance': debt_balance[num_periods]
        },
        'period_frequency': period_frequency
    }

def solve_maximum_debt(capex, cash_flows, target_dscrs, max_gearing, interest_rate, tenor_years, period_frequency='annual', debug=True):
    """
    Find maximum sustainable debt using binary search.
    
    Args:
        capex (float): Total CAPEX in millions
        cash_flows (list): Operating cash flows in millions (annual or quarterly)
        target_dscrs (list): Target DSCR for each period
        max_gearing (float): Maximum gearing ratio (0-1)
        interest_rate (float): Annual interest rate
        tenor_years (int): Debt term in years
        period_frequency (str): 'annual' or 'quarterly' - determines period calculation
        debug (bool): Print debug information
    
    Returns:
        dict: Optimal debt solution
    """
    if capex == 0 or not cash_flows:
        return {
            'debt': 0,
            'gearing': 0,
            'schedule': calculate_annual_debt_schedule(0, cash_flows or [0], interest_rate, tenor_years, target_dscrs or [1.4], period_frequency)
        }
    
    # Binary search bounds
    lower_bound = 0
    upper_bound = capex * max_gearing
    tolerance = 0.001  # $1M precision
    max_iterations = 50
    
    best_debt = 0
    best_schedule = None
    
   
    iteration = 0
    while iteration < max_iterations and (upper_bound - lower_bound) > tolerance:
        test_debt = (lower_bound + upper_bound) / 2
        
        # Test this debt amount
        schedule = calculate_annual_debt_schedule(
            test_debt, cash_flows, interest_rate, tenor_years, target_dscrs, period_frequency
        )
        
        if debug and iteration < 5:
            print(f"\nIteration {iteration + 1}: Testing ${test_debt:,.2f}M")
            print(f"  Fully repaid: {schedule['metrics']['fully_repaid']}")
            print(f"  Final balance: ${schedule['metrics']['final_balance']:,.3f}M")
            print(f"  Min DSCR: {schedule['metrics']['min_dscr']:.2f}")
        
        if schedule['metrics']['fully_repaid']:
            # Debt can be repaid - try higher amount
            lower_bound = test_debt
            best_debt = test_debt
            best_schedule = schedule
        else:
            # Debt cannot be repaid - try lower amount
            upper_bound = test_debt
        
        iteration += 1
    
    # Final result
    if best_debt == 0:
        best_schedule = calculate_annual_debt_schedule(0, cash_flows, interest_rate, tenor_years, target_dscrs, period_frequency)
    
    actual_gearing = best_debt / capex if capex > 0 else 0
    
    # Verify gearing constraint is respected
    if actual_gearing > max_gearing + 0.001:  # Small tolerance for floating point
        print(f"WARNING: Calculated gearing {actual_gearing:.1%} exceeds max gearing {max_gearing:.1%}")
        # Cap at max gearing
        best_debt = capex * max_gearing
        actual_gearing = max_gearing
        best_schedule = calculate_annual_debt_schedule(
            best_debt, cash_flows, interest_rate, tenor_years, target_dscrs, period_frequency
        )
    
    # Check if optimal debt hit the gearing limit
    hit_gearing_limit = abs(actual_gearing - max_gearing) < 0.001
    
    if debug:
        if best_debt > 0:
            print(f"SOLUTION: ${best_debt:,.2f}M ({actual_gearing:.1%} gearing)")
            if hit_gearing_limit:
                print(f"  ⚠️  WARNING: Optimal debt hit max gearing limit ({max_gearing:.1%})")
            print(f"  Average debt service: ${best_schedule['metrics']['avg_debt_service']:,.2f}M")
            print(f"  Minimum DSCR: {best_schedule['metrics']['min_dscr']:.2f}")
            # Show DSCR by period for first few years
            if len(best_schedule['dscr_values']) > 0:
                print(f"  DSCR by year (first 5): {[f'{d:.2f}' for d in best_schedule['dscr_values'][:5]]}")
        else:
            print(f"SOLUTION: No debt viable (100% equity)")
        print("=" * 50)
    
    return {
        'debt': best_debt,
        'gearing': actual_gearing,
        'schedule': best_schedule,
        'hit_gearing_limit': hit_gearing_limit
    }

def prepare_annual_cash_flows_from_operations_start(asset, revenue_df, opex_df, dscr_calculation_frequency='quarterly'):
    """
    Convert monthly cash flows to annual or quarterly periods for debt sizing, starting from operations start date.
    
    Args:
        asset (dict): Asset data
        revenue_df (pd.DataFrame): Monthly revenue data
        opex_df (pd.DataFrame): Monthly OPEX data
        dscr_calculation_frequency (str): 'annual' or 'quarterly' - determines aggregation period
    
    Returns:
        pd.DataFrame: Aggregated cash flows and revenue breakdown (annual or quarterly)
    """
    # Filter data for this asset
    asset_revenue = revenue_df[revenue_df['asset_id'] == asset['id']].copy()
    asset_opex = opex_df[opex_df['asset_id'] == asset['id']].copy()
    
    if asset_revenue.empty or asset_opex.empty:
        return pd.DataFrame()
    
    # Merge revenue and opex
    cash_flow_data = pd.merge(asset_revenue, asset_opex, on=['asset_id', 'date'], how='inner')
    
    # Calculate monthly CFADS
    cash_flow_data['cfads'] = cash_flow_data['revenue'] - cash_flow_data['opex']
    
    # CRITICAL: Filter to only include periods from operations start date onwards
    operations_start = pd.to_datetime(asset['OperatingStartDate'])
    cash_flow_data = cash_flow_data[cash_flow_data['date'] >= operations_start].copy()
    
    if cash_flow_data.empty:
        return pd.DataFrame()
    
    # Determine aggregation period
    if dscr_calculation_frequency.lower() == 'quarterly':
        # Group by quarter (Q1=Jan-Mar, Q2=Apr-Jun, Q3=Jul-Sep, Q4=Oct-Dec)
        # Calculate quarter offset from operations start
        operations_start_month = operations_start.month
        operations_start_year = operations_start.year
        operations_start_quarter = (operations_start_month - 1) // 3 + 1
        
        # Calculate quarter offset for each row
        cash_flow_data['quarter'] = (cash_flow_data['date'].dt.month - 1) // 3 + 1
        cash_flow_data['year'] = cash_flow_data['date'].dt.year
        
        # Calculate quarter offset from operations start
        def calculate_quarter_offset(row):
            year_diff = row['year'] - operations_start_year
            quarter_diff = row['quarter'] - operations_start_quarter
            return year_diff * 4 + quarter_diff
        
        cash_flow_data['period_offset'] = cash_flow_data.apply(calculate_quarter_offset, axis=1)
        
        # Group by quarter offset and sum
        aggregated_data = cash_flow_data.groupby('period_offset').agg({
            'cfads': 'sum',
            'contractedGreenRevenue': 'sum',
            'contractedEnergyRevenue': 'sum',
            'merchantGreenRevenue': 'sum',
            'merchantEnergyRevenue': 'sum'
        }).reset_index()
        
        # Rename period_offset to period for consistency
        aggregated_data['period'] = aggregated_data['period_offset']
        aggregated_data = aggregated_data.drop('period_offset', axis=1)
        
        period_type = 'quarterly'
    else:
        # Annual aggregation (default/legacy behavior)
        operations_start_month = operations_start.month
        cash_flow_data['year_offset'] = ((cash_flow_data['date'].dt.year - operations_start.year) * 12 + 
                                        (cash_flow_data['date'].dt.month - operations_start_month)) // 12
        
        # Group by year and sum
        aggregated_data = cash_flow_data.groupby('year_offset').agg({
            'cfads': 'sum',
            'contractedGreenRevenue': 'sum',
            'contractedEnergyRevenue': 'sum',
            'merchantGreenRevenue': 'sum',
            'merchantEnergyRevenue': 'sum'
        }).reset_index()
        
        # Rename year_offset to period for consistency
        aggregated_data['period'] = aggregated_data['year_offset']
        aggregated_data = aggregated_data.drop('year_offset', axis=1)
        
        period_type = 'annual'
    
    print(f"  Operations start: {operations_start.strftime('%Y-%m-%d')}")
    print(f"  {period_type.capitalize()} periods extracted: {len(aggregated_data)}")
    print(f"  First 3 periods CFADS: {[f'${cf:.1f}M' for cf in aggregated_data['cfads'].head(3)]}")
    
    return aggregated_data

def prepare_annual_cash_flows(asset, revenue_df, opex_df, dscr_calculation_frequency='quarterly'):
    """
    Legacy function - redirects to the corrected version.
    """
    return prepare_annual_cash_flows_from_operations_start(asset, revenue_df, opex_df, dscr_calculation_frequency)

def size_debt_for_asset(asset, asset_assumptions, revenue_df, opex_df, dscr_calculation_frequency='quarterly'):
    """
    Size debt for a single asset based on operational cash flows starting from operations.
    
    Args:
        asset (dict): Asset data
        asset_assumptions (dict): Asset cost assumptions
        revenue_df (pd.DataFrame): Revenue data
        opex_df (pd.DataFrame): OPEX data
        dscr_calculation_frequency (str): 'annual' or 'quarterly' - determines DSCR calculation period
    
    Returns:
        dict: Debt sizing results
    """
    # Get asset parameters
    capex = asset_assumptions.get('capex', 0)
    max_gearing = asset_assumptions.get('maxGearing', 0.7)
    interest_rate = asset_assumptions.get('interestRate', 0.055)
    tenor_years = asset_assumptions.get('tenorYears', 18)
    target_dscr_contract = asset_assumptions.get('targetDSCRContract', 1.4)
    target_dscr_merchant = asset_assumptions.get('targetDSCRMerchant', 1.8)
    
    if capex == 0:
        return {
            'optimal_debt': 0,
            'gearing': 0,
            'debt_service_start_date': None,
            'period_rate': None,
            'total_payments': None,
            'annual_schedule': None
        }
    
    # Prepare cash flows for debt sizing FROM OPERATIONS START (annual or quarterly)
    period_data = prepare_annual_cash_flows_from_operations_start(asset, revenue_df, opex_df, dscr_calculation_frequency)
    
    if period_data.empty:
        print(f"WARNING: No operational cash flows found for {asset.get('name', asset['id'])}")
        return {
            'optimal_debt': 0,
            'gearing': 0,
            'debt_service_start_date': None,
            'period_rate': None,
            'total_payments': None,
            'annual_schedule': None
        }
    
    # Calculate cash flows and blended DSCRs for each period
    period_cash_flows = period_data['cfads'].tolist()
    period_target_dscrs = []
    
    for _, row in period_data.iterrows():
        contracted_revenue = row['contractedGreenRevenue'] + row['contractedEnergyRevenue']
        merchant_revenue = row['merchantGreenRevenue'] + row['merchantEnergyRevenue']
        
        blended_dscr_value = calculate_blended_dscr(
            contracted_revenue, merchant_revenue, 
            target_dscr_contract, target_dscr_merchant
        )
        period_target_dscrs.append(blended_dscr_value)
    
    period_type = dscr_calculation_frequency.lower()
    print(f"\nAsset {asset.get('name', asset['id'])}: {period_type.capitalize()} debt sizing from operations start")
    print(f"CAPEX: ${capex:,.0f}M, {period_type.capitalize()} periods: {len(period_cash_flows)}")
    
    # Solve for optimal debt
    solution = solve_maximum_debt(
        capex, period_cash_flows, period_target_dscrs, 
        max_gearing, interest_rate, tenor_years, period_frequency=period_type, debug=False
    )
    
    # Calculate debt service start date (from operations start)
    operations_start = pd.to_datetime(asset['OperatingStartDate'])
    
    return {
        'optimal_debt': solution['debt'],
        'gearing': solution['gearing'],
        'debt_service_start_date': operations_start,  # Debt service starts with operations
        'period_rate': interest_rate / 12,
        'total_payments': tenor_years * 12,
        'annual_schedule': solution['schedule'],
        'interest_rate': interest_rate,
        'tenor_years': tenor_years
    }

def generate_monthly_debt_schedule(debt_amount, asset, capex_df, debt_sizing_result, 
                                 start_date, end_date, repayment_frequency, monthly_cfads=None, target_dscrs=None):
    """
    Generate monthly debt schedule from debt sizing results.
    Key corrections:
    1. Interest accrues on drawn debt during construction (even if not paid)
    2. Debt service starts from operations start date
    3. Monthly DSCR validation to ensure payments don't violate constraints
    
    Args:
        debt_amount (float): Total debt amount in millions
        asset (dict): Asset data
        capex_df (pd.DataFrame): CAPEX schedule for the asset
        debt_sizing_result (dict): Results from debt sizing
        start_date (datetime): Model start date
        end_date (datetime): Model end date
        repayment_frequency (str): 'monthly' or 'quarterly'
        monthly_cfads (pd.DataFrame): Optional monthly CFADS for DSCR validation (columns: date, cfads)
        target_dscrs (list): Optional target DSCRs for monthly validation
    
    Returns:
        pd.DataFrame: Monthly debt schedule
    """
    date_range = pd.date_range(start=start_date, end=end_date, freq='MS')
    
    # Initialize schedule
    schedule = pd.DataFrame({
        'asset_id': asset['id'],
        'date': date_range,
        'beginning_balance': 0.0,
        'drawdowns': 0.0,
        'interest': 0.0,
        'principal': 0.0,
        'ending_balance': 0.0
    })
    
    if debt_amount == 0:
        return schedule
    
    # Calculate actual gearing from debt amount and total CAPEX
    total_capex = capex_df['capex'].sum()
    if total_capex > 0:
        actual_gearing = debt_amount / total_capex
        
        # Populate debt drawdowns during construction
        current_drawn = 0
        for _, row in capex_df.iterrows():
            if row['date'] in schedule['date'].values and current_drawn < debt_amount:
                # Calculate debt portion of this month's CAPEX
                monthly_debt_capex = row['capex'] * actual_gearing
                drawdown_amount = min(monthly_debt_capex, debt_amount - current_drawn)
                
                schedule.loc[schedule['date'] == row['date'], 'drawdowns'] = drawdown_amount
                current_drawn += drawdown_amount
    
    # Get debt service parameters
    debt_service_start_date = debt_sizing_result.get('debt_service_start_date')
    interest_rate = debt_sizing_result.get('interest_rate', 0.055)
    tenor_years = debt_sizing_result.get('tenor_years', 18)
    annual_schedule = debt_sizing_result.get('annual_schedule')
    monthly_rate = interest_rate / 12
    
    if not debt_service_start_date:
        return schedule
    
    print(f"  Debt service starts: {debt_service_start_date.strftime('%Y-%m-%d')}")
    
    # Prepare monthly CFADS lookup if provided
    monthly_cfads_dict = {}
    if monthly_cfads is not None and not monthly_cfads.empty:
        for _, row in monthly_cfads.iterrows():
            monthly_cfads_dict[pd.to_datetime(row['date'])] = row.get('cfads', 0)
    
    # Track balance, accrued interest, and populate payments
    balance = 0.0
    accrued_interest = 0.0  # Track interest accrued during construction
    
    for i, current_date in enumerate(schedule['date']):
        schedule.loc[i, 'beginning_balance'] = balance
        balance += schedule.loc[i, 'drawdowns']
        
        # Interest accrues on outstanding balance (including during construction)
        if balance > 0:
            monthly_interest_accrued = balance * monthly_rate
            accrued_interest += monthly_interest_accrued
            
            # If we're past operations start, interest is paid (not just accrued)
            if current_date >= debt_service_start_date:
                # Apply debt service after operations start date
                if annual_schedule:
                    # Determine period index based on schedule frequency
                    schedule_frequency = annual_schedule.get('period_frequency', 'annual')
                    period_index = None  # Initialize for DSCR validation
                    
                    if schedule_frequency == 'quarterly':
                        # Calculate which quarter we're in
                        months_since_start = (current_date.year - debt_service_start_date.year) * 12 + \
                                           (current_date.month - debt_service_start_date.month)
                        quarter_index = months_since_start // 3
                        period_index = quarter_index  # For DSCR validation
                        
                        if quarter_index < len(annual_schedule['interest_payments']):
                            # Get quarterly amounts directly
                            quarterly_interest = annual_schedule['interest_payments'][quarter_index]
                            quarterly_principal = annual_schedule['principal_payments'][quarter_index]
                            
                            # Only make payments in quarter-end months
                            if current_date.month in [3, 6, 9, 12]:
                                monthly_interest = quarterly_interest
                                monthly_principal = quarterly_principal
                            else:
                                monthly_interest = 0
                                monthly_principal = 0
                        else:
                            monthly_interest = 0
                            monthly_principal = 0
                    else:
                        # Annual schedule - calculate which year we're in
                        years_since_start = (current_date.year - debt_service_start_date.year) + \
                                           (current_date.month - debt_service_start_date.month) / 12
                        year_index = int(years_since_start)
                        period_index = year_index  # For DSCR validation
                        
                        if year_index < len(annual_schedule['interest_payments']):
                            # Get annual amounts
                            annual_interest = annual_schedule['interest_payments'][year_index]
                            annual_principal = annual_schedule['principal_payments'][year_index]
                            
                            # Calculate monthly/quarterly payments
                            if repayment_frequency == 'monthly':
                                monthly_interest = annual_interest / 12
                                monthly_principal = annual_principal / 12
                            elif repayment_frequency == 'quarterly':
                                # Only make payments in quarter-end months
                                if current_date.month in [3, 6, 9, 12]:
                                    monthly_interest = annual_interest / 4
                                    monthly_principal = annual_principal / 4
                                else:
                                    monthly_interest = 0
                                    monthly_principal = 0
                            else:
                                monthly_interest = 0
                                monthly_principal = 0
                        else:
                            monthly_interest = 0
                            monthly_principal = 0
                    
                    # Apply monthly DSCR validation if monthly CFADS provided
                    if monthly_cfads_dict and current_date in monthly_cfads_dict and period_index is not None:
                        monthly_cfads = monthly_cfads_dict[current_date]
                        
                        # Get target DSCR for this period (use blended or default)
                        if target_dscrs and period_index < len(target_dscrs):
                            target_dscr = target_dscrs[period_index]
                        else:
                            # Default to conservative DSCR
                            target_dscr = 1.4
                        
                        # Maximum debt service allowed by DSCR
                        max_debt_service = monthly_cfads / target_dscr if target_dscr > 0 and monthly_cfads > 0 else float('inf')
                        
                        # Adjust payments if they would violate DSCR
                        proposed_debt_service = monthly_interest + monthly_principal
                        original_principal = monthly_principal
                        if proposed_debt_service > max_debt_service:
                            # Reduce principal payment to meet DSCR constraint
                            monthly_principal = max(0, max_debt_service - monthly_interest)
                            # Only warn if significant reduction (more than 10%)
                            if original_principal > 0 and monthly_principal < original_principal * 0.9:
                                print(f"    WARNING: Reduced principal payment in {current_date.strftime('%Y-%m')} from ${original_principal:.2f}M to ${monthly_principal:.2f}M to meet DSCR constraint")
                        
                        # Record payments
                        schedule.loc[i, 'interest'] = monthly_interest
                        schedule.loc[i, 'principal'] = min(monthly_principal, balance)
                        balance -= schedule.loc[i, 'principal']
                        accrued_interest = max(0, accrued_interest - monthly_interest)  # Reduce accrued interest by amount paid
                else:
                    # No annual schedule (e.g., annuity method) - calculate interest directly
                    if current_date >= debt_service_start_date:
                        schedule.loc[i, 'interest'] = balance * monthly_rate
                        # For annuity, principal would be calculated separately
                        accrued_interest = 0  # Reset since we're paying it
        
        schedule.loc[i, 'ending_balance'] = balance
    
    return schedule

def calculate_debt_schedule(assets, debt_assumptions, capex_schedule, cash_flow_df, start_date, end_date, 
                          repayment_frequency=DEFAULT_DEBT_REPAYMENT_FREQUENCY, 
                          grace_period=DEFAULT_DEBT_GRACE_PERIOD, 
                          debt_sizing_method=DEFAULT_DEBT_SIZING_METHOD, 
                          dscr_calculation_frequency=DSCR_CALCULATION_FREQUENCY):
    """
    Calculate debt schedule for all assets with corrected logic.
    
    Key corrections:
    1. Debt sizing starts from operations start date (assetStartDate)
    2. Proper unit handling (all values in millions)
    3. DSCR sculpting based on operational cash flows only
    4. Binary search to find optimal debt within gearing constraints
    
    Returns:
        tuple: (debt_schedule_df, updated_capex_df)
    """
    print("DEBT SIZING STARTING (CORRECTED)")
    print(f"Assets to process: {len(assets)}")
    print(f"Method: {debt_sizing_method}, DSCR Calculation Frequency: {dscr_calculation_frequency}, Repayment Frequency: {dscr_calculation_frequency}")
    
    all_debt_schedules = []
    updated_capex_schedules = []
    
    # Prepare revenue and opex data
    revenue_df = cash_flow_df[['asset_id', 'date', 'revenue', 'contractedGreenRevenue', 
                               'contractedEnergyRevenue', 'merchantGreenRevenue', 'merchantEnergyRevenue']].copy()
    opex_df = cash_flow_df[['asset_id', 'date', 'opex']].copy()
    
    for asset in assets:
        asset_name = asset.get('name', f"Asset_{asset['id']}")
        asset_assumptions = debt_assumptions.get(asset_name, {})
        
        print(f"\n--- Processing {asset_name} ---")
        print(f"Operations Start: {asset.get('OperatingStartDate', 'Not specified')}")
        
        if debt_sizing_method == 'dscr':
            # Size debt based on operational cash flows FROM OPERATIONS START
            # Use dscr_calculation_frequency for DSCR calculation
            debt_sizing_result = size_debt_for_asset(asset, asset_assumptions, revenue_df, opex_df, dscr_calculation_frequency)
            optimal_debt = debt_sizing_result['optimal_debt']
            
            # Generate monthly debt schedule
            asset_capex = capex_schedule[capex_schedule['asset_id'] == asset['id']].copy()
            
            # Prepare monthly CFADS for DSCR validation
            asset_cash_flow = cash_flow_df[cash_flow_df['asset_id'] == asset['id']].copy()
            monthly_cfads = None
            target_dscrs_list = None
            if not asset_cash_flow.empty:
                # Calculate monthly CFADS
                monthly_cfads = asset_cash_flow[['date', 'revenue', 'opex']].copy()
                monthly_cfads['cfads'] = monthly_cfads['revenue'] - monthly_cfads['opex']
                monthly_cfads = monthly_cfads[['date', 'cfads']].copy()
                
                # Get target DSCRs from schedule if available
                annual_schedule = debt_sizing_result.get('annual_schedule')
                if annual_schedule and 'dscr_values' in annual_schedule:
                    # Use the target DSCRs that were used in sizing
                    # We'll need to reconstruct them - for now use blended DSCR
                    target_dscr_contract = asset_assumptions.get('targetDSCRContract', 1.4)
                    target_dscr_merchant = asset_assumptions.get('targetDSCRMerchant', 1.8)
                    # Create a simple list - in practice this should match the schedule
                    target_dscrs_list = [target_dscr_contract] * 20  # Default to contracted DSCR
            
            # Use dscr_calculation_frequency for repayment frequency to match DSCR calculation period
            # This ensures debt payments align with the DSCR calculation frequency
            effective_repayment_frequency = dscr_calculation_frequency
            debt_schedule = generate_monthly_debt_schedule(
                optimal_debt, asset, asset_capex, debt_sizing_result,
                start_date, end_date, effective_repayment_frequency, monthly_cfads, target_dscrs_list
            )
            
        elif debt_sizing_method == 'annuity':
            # Annuity method - use configured gearing (legacy approach)
            capex = asset_assumptions.get('capex', 0)
            max_gearing = asset_assumptions.get('maxGearing', 0.7)
            optimal_debt = capex * max_gearing
            
            # Create simple debt sizing result for annuity
            operations_start = pd.to_datetime(asset['assetStartDate']) if asset.get('assetStartDate') else start_date
            debt_sizing_result = {
                'optimal_debt': optimal_debt,
                'debt_service_start_date': operations_start,
                'interest_rate': asset_assumptions.get('interestRate', 0.055),
                'tenor_years': asset_assumptions.get('tenorYears', 18),
                'annual_schedule': None  # Not used for annuity
            }
            
            asset_capex = capex_schedule[capex_schedule['asset_id'] == asset['id']].copy()
            
            # For annuity method, monthly CFADS not required for DSCR validation
            debt_schedule = generate_monthly_debt_schedule(
                optimal_debt, asset, asset_capex, debt_sizing_result,
                start_date, end_date, repayment_frequency, None, None
            )
        
        else:
            # Unknown method
            print(f"  WARNING: Unknown debt sizing method '{debt_sizing_method}' - using 100% equity")
            optimal_debt = 0
            debt_schedule = pd.DataFrame(columns=['asset_id', 'date', 'beginning_balance', 
                                                'drawdowns', 'interest', 'principal', 'ending_balance'])
        
        # Update CAPEX schedule with actual debt/equity split
        asset_capex = capex_schedule[capex_schedule['asset_id'] == asset['id']].copy()
        total_capex = asset_capex['capex'].sum()
        
        if total_capex > 0:
            if optimal_debt > 0:
                actual_gearing = optimal_debt / total_capex
                asset_capex['debt_capex'] = asset_capex['capex'] * actual_gearing
                asset_capex['equity_capex'] = asset_capex['capex'] * (1 - actual_gearing)
                
                # Validate debt repayment
                if not debt_schedule.empty:
                    final_debt_balance = debt_schedule['ending_balance'].iloc[-1]
                    if final_debt_balance > 0.001:  # $1M tolerance
                        print(f"  ⚠️  WARNING: Debt not fully repaid. Final balance: ${final_debt_balance:,.2f}M")
                    else:
                        print(f"  ✓ Debt fully repaid by end of tenor")
                
                # Show debt sizing metrics if available
                if debt_sizing_method == 'dscr':
                    if debt_sizing_result.get('annual_schedule'):
                        annual_schedule = debt_sizing_result['annual_schedule']
                        metrics = annual_schedule.get('metrics', {})
                        if metrics.get('min_dscr'):
                            print(f"  Min DSCR: {metrics['min_dscr']:.2f}x")
                    if debt_sizing_result.get('hit_gearing_limit'):
                        print(f"  ⚠️  Hit max gearing limit")
                
                print(f"SUCCESS: {asset_name}: ${optimal_debt:,.0f}M debt ({actual_gearing:.1%} gearing)")
            else:
                asset_capex['debt_capex'] = 0
                asset_capex['equity_capex'] = asset_capex['capex']
                print(f"SUCCESS: {asset_name}: 100% equity funding")
            
            updated_capex_schedules.append(asset_capex)
            
            if not debt_schedule.empty:
                all_debt_schedules.append(debt_schedule)
    
    # Combine results
    if all_debt_schedules:
        debt_df = pd.concat(all_debt_schedules, ignore_index=True)
    else:
        debt_df = pd.DataFrame(columns=['asset_id', 'date', 'beginning_balance', 
                                      'drawdowns', 'interest', 'principal', 'ending_balance'])
    
    if updated_capex_schedules:
        updated_capex_df = pd.concat(updated_capex_schedules, ignore_index=True)
    else:
        # If no updates, ensure 100% equity
        updated_capex_df = capex_schedule.copy()
        updated_capex_df['debt_capex'] = 0
        updated_capex_df['equity_capex'] = updated_capex_df['capex']
    
    print(f"\nDEBT SIZING COMPLETE (CORRECTED)")
    print(f"Debt schedules generated: {len(all_debt_schedules)}")
    
    return debt_df, updated_capex_df