# backend/calculations/debt.py

import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
from config import DEFAULT_DEBT_REPAYMENT_FREQUENCY, DEFAULT_DEBT_GRACE_PERIOD, DEFAULT_DEBT_SIZING_METHOD, DSCR_CALCULATION_FREQUENCY

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

def calculate_annual_debt_schedule(debt_amount, cash_flows, interest_rate, tenor_years, target_dscrs):
    """
    Calculate annual debt schedule using sculpting approach.
    
    Args:
        debt_amount (float): Initial debt amount in millions
        cash_flows (list): Annual operating cash flows (CFADS) in millions
        interest_rate (float): Annual interest rate
        tenor_years (int): Debt term in years
        target_dscrs (list): Target DSCR for each year
    
    Returns:
        dict: Complete debt schedule with metrics
    """
    # Initialize arrays
    debt_balance = [0.0] * (tenor_years + 1)
    interest_payments = [0.0] * tenor_years
    principal_payments = [0.0] * tenor_years
    debt_service = [0.0] * tenor_years
    dscr_values = [0.0] * tenor_years
    
    # Set initial debt balance
    debt_balance[0] = debt_amount
    
    # Calculate debt service for each year
    for year in range(tenor_years):
        if year >= len(cash_flows):
            break
            
        # Interest payment on opening balance
        interest_payments[year] = debt_balance[year] * interest_rate
        
        # Get available cash flow and target DSCR
        operating_cash_flow = cash_flows[year]
        target_dscr = target_dscrs[year] if year < len(target_dscrs) else target_dscrs[-1]
        
        # Maximum debt service allowed by DSCR constraint
        max_debt_service = operating_cash_flow / target_dscr if target_dscr > 0 else 0
        
        # Principal repayment (limited by max debt service and remaining balance)
        principal_payments[year] = min(
            max(0, max_debt_service - interest_payments[year]),
            debt_balance[year]
        )
        
        # Total debt service
        debt_service[year] = interest_payments[year] + principal_payments[year]
        
        # Calculate actual DSCR
        dscr_values[year] = operating_cash_flow / debt_service[year] if debt_service[year] > 0 else float('inf')
        
        # Update debt balance
        debt_balance[year + 1] = debt_balance[year] - principal_payments[year]
    
    # Calculate metrics
    fully_repaid = debt_balance[tenor_years] < 0.001  # $1M tolerance
    avg_debt_service = sum(debt_service) / tenor_years if tenor_years > 0 else 0
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
            'final_balance': debt_balance[tenor_years]
        }
    }

def solve_maximum_debt(capex, cash_flows, target_dscrs, max_gearing, interest_rate, tenor_years, debug=True):
    """
    Find maximum sustainable debt using binary search.
    
    Args:
        capex (float): Total CAPEX in millions
        cash_flows (list): Annual operating cash flows in millions
        target_dscrs (list): Target DSCR for each year
        max_gearing (float): Maximum gearing ratio (0-1)
        interest_rate (float): Annual interest rate
        tenor_years (int): Debt term in years
        debug (bool): Print debug information
    
    Returns:
        dict: Optimal debt solution
    """
    if capex == 0 or not cash_flows:
        return {
            'debt': 0,
            'gearing': 0,
            'schedule': calculate_annual_debt_schedule(0, cash_flows or [0], interest_rate, tenor_years, target_dscrs or [1.4])
        }
    
    # Binary search bounds
    lower_bound = 0
    upper_bound = capex * max_gearing
    tolerance = 0.001  # $1M precision
    max_iterations = 50
    
    best_debt = 0
    best_schedule = None
    
    if debug:
        print(f"\n=== DEBT SIZING ===")
        print(f"CAPEX: ${capex:,.0f}M")
        print(f"Max gearing: {max_gearing:.1%}")
        print(f"Upper bound: ${upper_bound:,.0f}M")
        print(f"Cash flows (first 5 years): {[f'${cf:,.1f}M' for cf in cash_flows[:5]]}")
        print(f"Target DSCRs (first 5): {[f'{d:.2f}' for d in target_dscrs[:5]]}")
        print(f"Interest rate: {interest_rate:.2%}, Tenor: {tenor_years} years")
    
    iteration = 0
    while iteration < max_iterations and (upper_bound - lower_bound) > tolerance:
        test_debt = (lower_bound + upper_bound) / 2
        
        # Test this debt amount
        schedule = calculate_annual_debt_schedule(
            test_debt, cash_flows, interest_rate, tenor_years, target_dscrs
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
        best_schedule = calculate_annual_debt_schedule(0, cash_flows, interest_rate, tenor_years, target_dscrs)
    
    actual_gearing = best_debt / capex if capex > 0 else 0
    
    if debug:
        if best_debt > 0:
            print(f"SOLUTION: ${best_debt:,.2f}M ({actual_gearing:.1%} gearing)")
            print(f"  Average debt service: ${best_schedule['metrics']['avg_debt_service']:,.2f}M")
            print(f"  Minimum DSCR: {best_schedule['metrics']['min_dscr']:.2f}")
        else:
            print(f"[FAILURE] SOLUTION: No debt viable (100% equity)")
        print("=" * 50)
    
    return {
        'debt': best_debt,
        'gearing': actual_gearing,
        'schedule': best_schedule
    }

def prepare_annual_cash_flows_from_operations_start(asset, revenue_df, opex_df):
    """
    Convert monthly cash flows to annual for debt sizing, starting from operations start date.
    
    Args:
        asset (dict): Asset data
        revenue_df (pd.DataFrame): Monthly revenue data
        opex_df (pd.DataFrame): Monthly OPEX data
    
    Returns:
        pd.DataFrame: Annual cash flows and revenue breakdown
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
    
    # Add year for grouping (fiscal year starting from operations start month)
    operations_start_month = operations_start.month
    cash_flow_data['year_offset'] = ((cash_flow_data['date'].dt.year - operations_start.year) * 12 + 
                                    (cash_flow_data['date'].dt.month - operations_start_month)) // 12
    
    # Group by year and sum
    annual_data = cash_flow_data.groupby('year_offset').agg({
        'cfads': 'sum',
        'contractedGreenRevenue': 'sum',
        'contractedEnergyRevenue': 'sum',
        'merchantGreenRevenue': 'sum',
        'merchantEnergyRevenue': 'sum'
    }).reset_index()
    
    # Rename year_offset to year for consistency
    annual_data['year'] = annual_data['year_offset']
    annual_data = annual_data.drop('year_offset', axis=1)
    
    print(f"  Operations start: {operations_start.strftime('%Y-%m-%d')}")
    print(f"  Annual periods extracted: {len(annual_data)}")
    print(f"  First 3 years CFADS: {[f'${cf:.1f}M' for cf in annual_data['cfads'].head(3)]}")
    
    return annual_data

def prepare_annual_cash_flows(asset, revenue_df, opex_df):
    """
    Legacy function - redirects to the corrected version.
    """
    return prepare_annual_cash_flows_from_operations_start(asset, revenue_df, opex_df)

def size_debt_for_asset(asset, asset_assumptions, revenue_df, opex_df):
    """
    Size debt for a single asset based on operational cash flows starting from operations.
    
    Args:
        asset (dict): Asset data
        asset_assumptions (dict): Asset cost assumptions
        revenue_df (pd.DataFrame): Revenue data
        opex_df (pd.DataFrame): OPEX data
    
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
    
    # Prepare annual cash flows for debt sizing FROM OPERATIONS START
    annual_data = prepare_annual_cash_flows_from_operations_start(asset, revenue_df, opex_df)
    
    if annual_data.empty:
        print(f"WARNING: No operational cash flows found for {asset.get('name', asset['id'])}")
        return {
            'optimal_debt': 0,
            'gearing': 0,
            'debt_service_start_date': None,
            'period_rate': None,
            'total_payments': None,
            'annual_schedule': None
        }
    
    # Calculate annual cash flows and blended DSCRs
    annual_cash_flows = annual_data['cfads'].tolist()
    annual_target_dscrs = []
    
    for _, row in annual_data.iterrows():
        contracted_revenue = row['contractedGreenRevenue'] + row['contractedEnergyRevenue']
        merchant_revenue = row['merchantGreenRevenue'] + row['merchantEnergyRevenue']
        
        blended_dscr_value = calculate_blended_dscr(
            contracted_revenue, merchant_revenue, 
            target_dscr_contract, target_dscr_merchant
        )
        annual_target_dscrs.append(blended_dscr_value)
    
    print(f"\nAsset {asset.get('name', asset['id'])}: Annual debt sizing from operations start")
    print(f"CAPEX: ${capex:,.0f}M, Annual periods: {len(annual_cash_flows)}")
    
    # Solve for optimal debt
    solution = solve_maximum_debt(
        capex, annual_cash_flows, annual_target_dscrs, 
        max_gearing, interest_rate, tenor_years, debug=True
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
                                 start_date, end_date, repayment_frequency):
    """
    Generate monthly debt schedule from debt sizing results.
    Key correction: Debt service starts from operations start date.
    
    Args:
        debt_amount (float): Total debt amount in millions
        asset (dict): Asset data
        capex_df (pd.DataFrame): CAPEX schedule for the asset
        debt_sizing_result (dict): Results from debt sizing
        start_date (datetime): Model start date
        end_date (datetime): Model end date
        repayment_frequency (str): 'monthly' or 'quarterly'
    
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
    
    if not debt_service_start_date or not annual_schedule:
        return schedule
    
    print(f"  Debt service starts: {debt_service_start_date.strftime('%Y-%m-%d')}")
    
    # Track balance and populate payments
    balance = 0.0
    
    for i, current_date in enumerate(schedule['date']):
        schedule.loc[i, 'beginning_balance'] = balance
        balance += schedule.loc[i, 'drawdowns']
        
        # Apply debt service ONLY after operations start date
        if current_date >= debt_service_start_date and balance > 0:
            # Calculate which year we're in for the annual schedule
            years_since_start = (current_date.year - debt_service_start_date.year) + \
                               (current_date.month - debt_service_start_date.month) / 12
            year_index = int(years_since_start)
            
            if year_index < len(annual_schedule['interest_payments']):
                # Get annual amounts
                annual_interest = annual_schedule['interest_payments'][year_index]
                annual_principal = annual_schedule['principal_payments'][year_index]
                
                if repayment_frequency == 'monthly':
                    monthly_interest = annual_interest / 12
                    monthly_principal = annual_principal / 12
                    
                    schedule.loc[i, 'interest'] = monthly_interest
                    schedule.loc[i, 'principal'] = min(monthly_principal, balance)
                    balance -= schedule.loc[i, 'principal']
                    
                elif repayment_frequency == 'quarterly':
                    # Only make payments in quarter-end months
                    if current_date.month in [3, 6, 9, 12]:
                        quarterly_interest = annual_interest / 4
                        quarterly_principal = annual_principal / 4
                        
                        schedule.loc[i, 'interest'] = quarterly_interest
                        schedule.loc[i, 'principal'] = min(quarterly_principal, balance)
                        balance -= schedule.loc[i, 'principal']
        
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
    print(f"Method: {debt_sizing_method}, Frequency: {repayment_frequency}")
    
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
            debt_sizing_result = size_debt_for_asset(asset, asset_assumptions, revenue_df, opex_df)
            optimal_debt = debt_sizing_result['optimal_debt']
            
            # Generate monthly debt schedule
            asset_capex = capex_schedule[capex_schedule['asset_id'] == asset['id']].copy()
            
            debt_schedule = generate_monthly_debt_schedule(
                optimal_debt, asset, asset_capex, debt_sizing_result,
                start_date, end_date, repayment_frequency
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
            
            debt_schedule = generate_monthly_debt_schedule(
                optimal_debt, asset, asset_capex, debt_sizing_result,
                start_date, end_date, repayment_frequency
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
                print(f"✓ {asset_name}: ${optimal_debt:,.0f}M debt ({actual_gearing:.1%} gearing)")
            else:
                asset_capex['debt_capex'] = 0
                asset_capex['equity_capex'] = asset_capex['capex']
                print(f"[SUCCESS] {asset_name}: 100% equity funding")
            
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