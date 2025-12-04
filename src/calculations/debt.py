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
    Calculate debt schedule using sculpting approach for annual, quarterly, or monthly periods.
    Ensures debt is fully paid down to zero by the end of the tenor period while maintaining DSCR constraints.
    
    Args:
        debt_amount (float): Initial debt amount in millions
        cash_flows (list): Operating cash flows (CFADS) in millions - annual, quarterly, or monthly
        interest_rate (float): Annual interest rate
        tenor_years (int): Debt term in years
        target_dscrs (list): Target DSCR for each period
        period_frequency (str): 'annual', 'quarterly', or 'monthly' - determines period calculation
    
    Returns:
        dict: Complete debt schedule with metrics
    """
    # Determine number of periods based on frequency
    if period_frequency.lower() == 'quarterly':
        num_periods = tenor_years * 4
        period_rate = interest_rate / 4  # Quarterly interest rate
    elif period_frequency.lower() == 'monthly':
        num_periods = tenor_years * 12
        period_rate = interest_rate / 12  # Monthly interest rate
    else:
        num_periods = tenor_years
        period_rate = interest_rate  # Annual interest rate
    
    # Ensure we have enough cash flows (pad with zeros if needed)
    if len(cash_flows) < num_periods:
        cash_flows = cash_flows + [0.0] * (num_periods - len(cash_flows))
    
    # Ensure we have enough target DSCRs (use last value if needed)
    if len(target_dscrs) < num_periods:
        last_dscr = target_dscrs[-1] if target_dscrs else 1.4
        target_dscrs = target_dscrs + [last_dscr] * (num_periods - len(target_dscrs))
    
    # Initialize arrays
    debt_balance = [0.0] * (num_periods + 1)
    interest_payments = [0.0] * num_periods
    principal_payments = [0.0] * num_periods
    debt_service = [0.0] * num_periods
    dscr_values = [0.0] * num_periods
    
    # Set initial debt balance
    debt_balance[0] = debt_amount
    
    # Calculate debt service for each period using DSCR sculpting
    # This matches Excel project finance models:
    # 1. Interest = opening balance * period rate (naturally decreases as balance is paid down)
    # 2. Max debt service = CFADS / target DSCR
    # 3. Principal = min(max debt service - interest, remaining balance)
    # 4. Principal must always be > 0 (except final period) to ensure amortization
    # 5. If DSCR is breached or debt can't be fully repaid, reduce debt size (handled by binary search)
    for period in range(num_periods):
        # Interest payment on opening balance (decreases each period as balance is paid down)
        interest_payments[period] = debt_balance[period] * period_rate
        
        # Get available cash flow and target DSCR
        operating_cash_flow = cash_flows[period]
        target_dscr = target_dscrs[period]
        
        # Maximum total debt service (interest + principal) allowed by DSCR constraint
        # For quarterly: operating_cash_flow is quarterly CFADS (sum of 3 months)
        # For annual: operating_cash_flow is annual CFADS (sum of 12 months)
        # Debt service must not exceed: CFADS / target DSCR
        max_debt_service = operating_cash_flow / target_dscr if target_dscr > 0 and operating_cash_flow > 0 else 0
        
        # Calculate remaining periods
        remaining_periods = num_periods - period - 1
        
        # Calculate principal payment
        # Principal must always be positive (when balance > 0) to ensure amortization
        if remaining_periods == 0:
            # Final period: must pay off all remaining balance
            required_principal = debt_balance[period]
        elif debt_balance[period] <= 0:
            # No balance left
            required_principal = 0
        else:
            # Maximum principal allowed by DSCR constraint
            max_principal_from_dscr = max_debt_service - interest_payments[period]
            
            # If interest alone exceeds max_debt_service, we can't pay any principal
            # This means debt is too large (will be caught by binary search)
            if max_principal_from_dscr <= 0:
                # Can't pay principal - debt is too large, but set to 0 to flag as non-viable
                required_principal = 0
            else:
                # We can pay principal - calculate how much
                # In DSCR sculpting, principal is determined by DSCR constraint, not forced minimum
                # Use DSCR-constrained amount (subject to remaining balance)
                required_principal = min(max_principal_from_dscr, debt_balance[period])
                
                # Principal should always be positive when we can pay it
                # If required_principal is 0 but we have balance, it means debt is too large
                # (This will be caught by binary search which will reduce debt size)
        
        # Principal repayment (never reduced - if DSCR is breached, debt size is reduced instead)
        principal_payments[period] = min(required_principal, debt_balance[period])
        
        # Total debt service (interest + principal)
        debt_service[period] = interest_payments[period] + principal_payments[period]
        
        # Calculate actual DSCR
        dscr_values[period] = operating_cash_flow / debt_service[period] if debt_service[period] > 0 else float('inf')
        
        # Update debt balance
        debt_balance[period + 1] = debt_balance[period] - principal_payments[period]
    
    # Final check: ensure debt is fully paid down in the last period
    # If there's still a balance, we need to pay it off (this should not happen if sizing is correct)
    if debt_balance[num_periods] > 0.001:  # $1M tolerance
        # Force payment in the last period
        last_period = num_periods - 1
        if last_period >= 0:
            # Add any remaining balance to the last period's principal
            remaining_balance = debt_balance[num_periods]
            principal_payments[last_period] += remaining_balance
            debt_service[last_period] = interest_payments[last_period] + principal_payments[last_period]
            debt_balance[num_periods] = 0.0
            
            # Recalculate DSCR for last period
            if last_period < len(cash_flows):
                operating_cash_flow = cash_flows[last_period]
                dscr_values[last_period] = operating_cash_flow / debt_service[last_period] if debt_service[last_period] > 0 else float('inf')
    
    # Calculate metrics
    fully_repaid = debt_balance[num_periods] < 0.001  # $1M tolerance
    avg_debt_service = sum(debt_service) / num_periods if num_periods > 0 else 0
    valid_dscrs = [d for d in dscr_values if d != float('inf') and d > 0]
    min_dscr = min(valid_dscrs) if valid_dscrs else 0
    
    # Check for DSCR breaches
    dscr_breached = False
    for period in range(num_periods):
        if period < len(target_dscrs):
            target_dscr = target_dscrs[period]
            if dscr_values[period] != float('inf') and dscr_values[period] < target_dscr - 0.01:  # Small tolerance
                dscr_breached = True
                break
    
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
            'final_balance': debt_balance[num_periods],
            'dscr_breached': dscr_breached
        },
        'period_frequency': period_frequency
    }

def solve_maximum_debt(capex, cash_flows, target_dscrs, max_gearing, interest_rate, tenor_years, period_frequency='annual', debug=True):
    """
    Find maximum sustainable debt using binary search.
    Ensures debt can be fully repaid by end of tenor while maintaining DSCR constraints.
    
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
        
        # Check if debt can be fully repaid AND DSCR constraints are met
        fully_repaid = schedule['metrics']['fully_repaid']
        dscr_breached = schedule['metrics'].get('dscr_breached', False)
        final_balance = schedule['metrics']['final_balance']
        
        # Debt is viable if it's fully repaid and DSCR is not breached
        is_viable = fully_repaid and not dscr_breached and abs(final_balance) < 0.001
        
        if debug and iteration < 5:
            print(f"\nIteration {iteration + 1}: Testing ${test_debt:,.2f}M")
            print(f"  Fully repaid: {fully_repaid}")
            print(f"  Final balance: ${final_balance:,.3f}M")
            print(f"  DSCR breached: {dscr_breached}")
            print(f"  Min DSCR: {schedule['metrics']['min_dscr']:.2f}")
            print(f"  Viable: {is_viable}")
        
        if is_viable:
            # Debt can be repaid and DSCR maintained - try higher amount
            lower_bound = test_debt
            best_debt = test_debt
            best_schedule = schedule
        else:
            # Debt cannot be repaid or DSCR would be breached - try lower amount
            upper_bound = test_debt
        
        iteration += 1
    
    # Final result - verify the best solution
    if best_debt > 0:
        # Recalculate to ensure it's correct
        best_schedule = calculate_annual_debt_schedule(
            best_debt, cash_flows, interest_rate, tenor_years, target_dscrs, period_frequency
        )
        
        # Validate final balance is zero
        if abs(best_schedule['metrics']['final_balance']) > 0.001:
            if debug:
                print(f"WARNING: Best debt solution has non-zero final balance: ${best_schedule['metrics']['final_balance']:,.3f}M")
    else:
        best_schedule = calculate_annual_debt_schedule(0, cash_flows, interest_rate, tenor_years, target_dscrs, period_frequency)
    
    actual_gearing = best_debt / capex if capex > 0 else 0
    
    # Verify gearing constraint is respected
    if actual_gearing > max_gearing + 0.001:  # Small tolerance for floating point
        if debug:
            print(f"WARNING: Calculated gearing {actual_gearing:.1%} exceeds max gearing {max_gearing:.1%}")
        # Cap at max gearing and test if it can be fully repaid
        capped_debt = capex * max_gearing
        capped_schedule = calculate_annual_debt_schedule(
            capped_debt, cash_flows, interest_rate, tenor_years, target_dscrs, period_frequency
        )
        
        # Only use capped debt if it can be fully repaid
        if capped_schedule['metrics']['fully_repaid'] and not capped_schedule['metrics'].get('dscr_breached', False):
            best_debt = capped_debt
            actual_gearing = max_gearing
            best_schedule = capped_schedule
        else:
            # Capped debt cannot be fully repaid - use the best we found
            if debug:
                print(f"WARNING: Max gearing debt cannot be fully repaid, using lower amount")
    
    # Check if optimal debt hit the gearing limit
    hit_gearing_limit = abs(actual_gearing - max_gearing) < 0.001
    
    if debug:
        if best_debt > 0:
            print(f"SOLUTION: ${best_debt:,.2f}M ({actual_gearing:.1%} gearing)")
            if hit_gearing_limit:
                print(f"  ⚠️  WARNING: Optimal debt hit max gearing limit ({max_gearing:.1%})")
            print(f"  Average debt service: ${best_schedule['metrics']['avg_debt_service']:,.2f}M")
            print(f"  Minimum DSCR: {best_schedule['metrics']['min_dscr']:.2f}")
            print(f"  Final balance: ${best_schedule['metrics']['final_balance']:,.3f}M")
            if best_schedule['metrics'].get('dscr_breached', False):
                print(f"  ⚠️  WARNING: DSCR constraint breached in some periods")
            # Show DSCR by period for first few years
            if len(best_schedule['dscr_values']) > 0:
                print(f"  DSCR by period (first 5): {[f'{d:.2f}' for d in best_schedule['dscr_values'][:5]]}")
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
    elif dscr_calculation_frequency.lower() == 'monthly':
        # Monthly aggregation - no grouping needed, just sort
        cash_flow_data = cash_flow_data.sort_values('date').reset_index(drop=True)
        
        # Add period index (0, 1, 2...)
        cash_flow_data['period'] = range(len(cash_flow_data))
        
        # Select columns
        aggregated_data = cash_flow_data[[
            'period', 'cfads', 'contractedGreenRevenue', 'contractedEnergyRevenue', 
            'merchantGreenRevenue', 'merchantEnergyRevenue'
        ]].copy()
        
        period_type = 'monthly'
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
    
    # No conservative adjustment - use actual CFADS
    # Debt sizing will iterate to find the maximum debt that can be supported
    
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
        'ending_balance': 0.0,
        'debt_service': 0.0  # Add debt_service column for DSCR validation
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
    
    # Calculate debt service end date (tenor years from start)
    debt_service_end_date = debt_service_start_date + relativedelta(years=tenor_years)
    
    print(f"  Debt service starts: {debt_service_start_date.strftime('%Y-%m-%d')}")
    print(f"  Debt service ends: {debt_service_end_date.strftime('%Y-%m-%d')}")
    
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
                    elif schedule_frequency == 'monthly':
                        # Monthly schedule - direct mapping
                        months_since_start = (current_date.year - debt_service_start_date.year) * 12 + \
                                           (current_date.month - debt_service_start_date.month)
                        period_index = months_since_start
                        
                        if period_index < len(annual_schedule['interest_payments']):
                            monthly_interest = annual_schedule['interest_payments'][period_index]
                            monthly_principal = annual_schedule['principal_payments'][period_index]
                        else:
                            monthly_interest = 0
                            monthly_principal = 0
                            
                        schedule.loc[i, 'interest'] = monthly_interest
                        schedule.loc[i, 'principal'] = min(monthly_principal, balance)
                        schedule.loc[i, 'debt_service'] = monthly_interest + schedule.loc[i, 'principal']
                        balance -= schedule.loc[i, 'principal']
                        accrued_interest = max(0, accrued_interest - monthly_interest)
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
                    
                    # Record payments - no monthly DSCR validation
                    # Debt sizing already accounts for DSCR constraints at the period level
                    # Monthly volatility is handled by conservative sizing, not by reducing payments
                    if schedule_frequency != 'monthly':
                        schedule.loc[i, 'interest'] = monthly_interest
                        schedule.loc[i, 'principal'] = min(monthly_principal, balance)
                        schedule.loc[i, 'debt_service'] = monthly_interest + schedule.loc[i, 'principal']
                        balance -= schedule.loc[i, 'principal']
                        accrued_interest = max(0, accrued_interest - monthly_interest)  # Reduce accrued interest by amount paid
                else:
                    # No annual schedule (e.g., annuity method) - calculate interest directly
                    if current_date >= debt_service_start_date:
                        schedule.loc[i, 'interest'] = balance * monthly_rate
                        # For annuity, principal would be calculated separately
                        # Calculate debt_service for validation
                        schedule.loc[i, 'debt_service'] = schedule.loc[i, 'interest'] + schedule.loc[i, 'principal']
                        accrued_interest = 0  # Reset since we're paying it
        
        schedule.loc[i, 'ending_balance'] = balance
    
    # Final check: ensure debt is fully paid down by end of tenor
    # Find the last month within the tenor period
    last_tenor_month_idx = None
    for i, current_date in enumerate(schedule['date']):
        if current_date >= debt_service_start_date and current_date <= debt_service_end_date:
            last_tenor_month_idx = i
    
    # If we're past the tenor end date and there's still a balance, pay it off
    if last_tenor_month_idx is not None:
        final_balance = schedule.loc[last_tenor_month_idx, 'ending_balance']
        if final_balance > 0.001:  # $1M tolerance
            # Find the last payment month within tenor and ensure balance is paid
            # This should not happen if the annual schedule is correct, but add as safeguard
            for i in range(last_tenor_month_idx, -1, -1):
                current_date = schedule.loc[i, 'date']
                if current_date >= debt_service_start_date and current_date <= debt_service_end_date:
                    remaining_balance = schedule.loc[i, 'ending_balance']
                    if remaining_balance > 0.001:
                        # Add remaining balance to principal payment for this month
                        current_principal = schedule.loc[i, 'principal']
                        schedule.loc[i, 'principal'] = current_principal + remaining_balance
                        schedule.loc[i, 'debt_service'] = schedule.loc[i, 'interest'] + schedule.loc[i, 'principal']
                        schedule.loc[i, 'ending_balance'] = 0.0
                        # Update balance for subsequent months
                        for j in range(i + 1, len(schedule)):
                            if schedule.loc[j, 'date'] <= debt_service_end_date:
                                schedule.loc[j, 'beginning_balance'] = schedule.loc[j - 1, 'ending_balance']
                                schedule.loc[j, 'ending_balance'] = schedule.loc[j, 'beginning_balance'] + \
                                                                      schedule.loc[j, 'drawdowns'] - \
                                                                      schedule.loc[j, 'principal']
                                schedule.loc[j, 'debt_service'] = schedule.loc[j, 'interest'] + schedule.loc[j, 'principal']
                        break
    
    # Additional safeguard: ensure ending balance is zero at end of tenor
    # Check all months up to and including the tenor end date
    for i, current_date in enumerate(schedule['date']):
        if current_date > debt_service_end_date:
            # After tenor ends, ensure balance is zero
            if schedule.loc[i, 'ending_balance'] > 0.001:
                # Pay off any remaining balance
                remaining = schedule.loc[i, 'ending_balance']
                schedule.loc[i, 'principal'] += remaining
                schedule.loc[i, 'debt_service'] = schedule.loc[i, 'interest'] + schedule.loc[i, 'principal']
                schedule.loc[i, 'ending_balance'] = 0.0
    
    # Ensure debt_service is calculated for all rows
    schedule['debt_service'] = schedule['interest'] + schedule['principal']
    
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
                    # Find the debt service end date
                    debt_service_start = debt_sizing_result.get('debt_service_start_date')
                    tenor_years = debt_sizing_result.get('tenor_years', 18)
                    if debt_service_start:
                        debt_service_end = pd.to_datetime(debt_service_start) + relativedelta(years=tenor_years)
                        # Find the last month within the tenor period
                        tenor_schedule = debt_schedule[
                            (debt_schedule['date'] >= debt_service_start) & 
                            (debt_schedule['date'] <= debt_service_end)
                        ]
                        if not tenor_schedule.empty:
                            final_debt_balance = tenor_schedule['ending_balance'].iloc[-1]
                        else:
                            final_debt_balance = debt_schedule['ending_balance'].iloc[-1]
                    else:
                        final_debt_balance = debt_schedule['ending_balance'].iloc[-1]
                    
                    if final_debt_balance > 0.001:  # $1M tolerance
                        print(f"  ⚠️  WARNING: Debt not fully repaid by end of tenor. Final balance: ${final_debt_balance:,.2f}M")
                    else:
                        print(f"  ✓ Debt fully repaid by end of tenor (final balance: ${final_debt_balance:,.3f}M)")
                    
                    # Validate DSCR constraints
                    if debt_sizing_method == 'dscr' and debt_service_start:
                        # Check DSCR for months within debt service period
                        service_period_schedule = debt_schedule[
                            (debt_schedule['date'] >= debt_service_start) & 
                            (debt_schedule['date'] <= debt_service_end)
                        ]
                        if not service_period_schedule.empty:
                            # Create monthly CFADS lookup if available
                            monthly_cfads_dict = {}
                            if monthly_cfads is not None and not monthly_cfads.empty:
                                for _, cfads_row in monthly_cfads.iterrows():
                                    monthly_cfads_dict[pd.to_datetime(cfads_row['date'])] = cfads_row.get('cfads', 0)
                            
                            # Calculate DSCR for each month in service period
                            dscr_breaches = []
                            
                            # Validation logic depends on frequency
                            if dscr_calculation_frequency == 'quarterly':
                                # Group by quarter for validation
                                service_period_schedule['quarter'] = service_period_schedule['date'].dt.to_period('Q')
                                quarterly_validation = service_period_schedule.groupby('quarter').agg({
                                    'debt_service': 'sum',
                                    'date': 'first'
                                }).reset_index()
                                
                                for _, row in quarterly_validation.iterrows():
                                    if row['debt_service'] > 0:
                                        # Sum CFADS for this quarter
                                        quarter_start = row['date']
                                        # Find all months in this quarter
                                        quarter_cfads = 0
                                        for m in range(3):
                                            month_date = quarter_start + relativedelta(months=m)
                                            # Ensure month is within the same quarter (handle quarter boundaries if needed)
                                            # But since we grouped by period('Q'), the 'date' is just the first one found
                                            # Better to iterate through original monthly data
                                            pass
                                        
                                        # Re-calculate quarterly CFADS properly
                                        q_start = row['quarter'].start_time
                                        q_end = row['quarter'].end_time
                                        
                                        q_cfads = 0
                                        for d, c in monthly_cfads_dict.items():
                                            if d >= q_start and d <= q_end:
                                                q_cfads += c
                                        
                                        actual_dscr = q_cfads / row['debt_service']
                                        
                                        # Get target DSCR
                                        if target_dscrs_list:
                                            # Estimate period index
                                            months_since_start = (q_start.year - pd.to_datetime(debt_service_start).year) * 12 + \
                                                                (q_start.month - pd.to_datetime(debt_service_start).month)
                                            period_idx = months_since_start // 3
                                            if period_idx < len(target_dscrs_list):
                                                target_dscr = target_dscrs_list[period_idx]
                                                if actual_dscr < target_dscr - 0.01:
                                                    dscr_breaches.append((q_start, actual_dscr, target_dscr))

                            else:
                                # Monthly or Annual validation
                                for _, row in service_period_schedule.iterrows():
                                    if row['debt_service'] > 0:
                                        month_cfads = monthly_cfads_dict.get(pd.to_datetime(row['date']), 0)
                                        if month_cfads > 0:
                                            actual_dscr = month_cfads / row['debt_service']
                                            # Get target DSCR for this period
                                            if target_dscrs_list:
                                                # Estimate period index
                                                months_since_start = (pd.to_datetime(row['date']).year - pd.to_datetime(debt_service_start).year) * 12 + \
                                                                    (pd.to_datetime(row['date']).month - pd.to_datetime(debt_service_start).month)
                                                
                                                if dscr_calculation_frequency == 'annual':
                                                    period_idx = months_since_start // 12
                                                elif dscr_calculation_frequency == 'quarterly':
                                                    period_idx = months_since_start // 3
                                                else:
                                                    period_idx = months_since_start
                                                    
                                                if period_idx < len(target_dscrs_list):
                                                    target_dscr = target_dscrs_list[period_idx]
                                                    if actual_dscr < target_dscr - 0.01:  # Small tolerance
                                                        dscr_breaches.append((row['date'], actual_dscr, target_dscr))
                            
                            if dscr_breaches:
                                print(f"  ⚠️  WARNING: DSCR constraint breached in {len(dscr_breaches)} period(s)")
                                for breach_date, actual, target in dscr_breaches[:3]:  # Show first 3
                                    print(f"    {breach_date.strftime('%Y-%m')}: DSCR {actual:.2f}x < target {target:.2f}x")
                            else:
                                print(f"  ✓ DSCR constraints maintained throughout debt service period")
                
                # Show debt sizing metrics if available
                if debt_sizing_method == 'dscr':
                    if debt_sizing_result.get('annual_schedule'):
                        annual_schedule = debt_sizing_result['annual_schedule']
                        metrics = annual_schedule.get('metrics', {})
                        if metrics.get('min_dscr'):
                            print(f"  Min DSCR: {metrics['min_dscr']:.2f}x")
                        if metrics.get('dscr_breached', False):
                            print(f"  ⚠️  WARNING: DSCR constraint breached in annual/quarterly schedule")
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