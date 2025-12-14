import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from ..config import DEFAULT_CAPEX_FUNDING_TYPE

def calculate_construction_capex_timeseries(
    asset_id: str,
    total_capex: float,
    construction_start: datetime,
    construction_end: datetime,
    start_date: datetime,
    end_date: datetime,
    max_gearing: float,
    capex_funding_type: str,
    distribution_method: str = 'linear', # New parameter
    percentage_distribution: list[float] = None # New parameter
) -> pd.DataFrame:
    """
    Generates a construction CAPEX schedule for a single asset with linear or percentage-based drawdown.

    Args:
        asset_id (str): The ID of the asset.
        total_capex (float): The total CAPEX amount for the asset.
        construction_start (datetime): The start date of the construction period.
        construction_end (datetime): The end date of the construction period (typically COD).
        start_date (datetime): The overall model start date.
        end_date (datetime): The overall model end date.
        max_gearing (float): The maximum gearing ratio for debt funding.
        capex_funding_type (str): How CAPEX is funded ('equity_first' or 'pari_passu').
        distribution_method (str): Method for CAPEX distribution ('linear' or 'percentage').
        percentage_distribution (list[float]): List of monthly percentages for 'percentage' method.

    Returns:
        pd.DataFrame: A DataFrame with columns for asset_id, date, capex, equity_capex, and debt_capex.
    """
    date_range = pd.date_range(start=start_date, end=end_date, freq='MS')

    capex_values = []
    equity_capex_values = []
    debt_capex_values = []

    # Calculate total equity and debt required for the asset
    total_debt_funding = total_capex * max_gearing
    total_equity_funding = total_capex * (1 - max_gearing)

    current_equity_funded = 0

    # Normalize dates to first of month for consistent comparison (date_range uses 'MS' frequency)
    construction_start_normalized = construction_start.replace(day=1)
    construction_end_normalized = construction_end.replace(day=1)
    start_date_normalized = start_date.replace(day=1) if hasattr(start_date, 'replace') else pd.to_datetime(start_date).replace(day=1)
    
    # Calculate construction months
    construction_months = (construction_end_normalized.year - construction_start_normalized.year) * 12 + \
                          (construction_end_normalized.month - construction_start_normalized.month)
    
    # Calculate how many construction months occurred before the model start_date
    # This ensures we capture the full CAPEX even if construction starts before model start
    months_before_model_start = 0
    if construction_start_normalized < start_date_normalized:
        # Calculate months from construction_start to start_date
        months_before_model_start = (start_date_normalized.year - construction_start_normalized.year) * 12 + \
                                     (start_date_normalized.month - construction_start_normalized.month)
        # Ensure we don't count more months than the total construction period
        months_before_model_start = min(months_before_model_start, construction_months)

    # Handle operating assets (construction_start == construction_end, construction_months == 0)
    # For operating assets, allocate full capex as single equity outflow at construction_start
    if construction_months == 0:
        # If operating asset start is before model start, allocate at first model date
        if construction_start_normalized < start_date_normalized:
            # Allocate all CAPEX at first model date
            for date in date_range:
                monthly_capex = 0
                monthly_equity_capex = 0
                monthly_debt_capex = 0
                
                # Allocate at first date in model period
                if date == date_range[0]:
                    monthly_capex = total_capex
                    # For operating assets, treat as acquisition - full equity outflow
                    monthly_equity_capex = total_capex
                    monthly_debt_capex = 0
                
                capex_values.append(monthly_capex)
                equity_capex_values.append(monthly_equity_capex)
                debt_capex_values.append(monthly_debt_capex)
        else:
            # Operating asset starts in model period - allocate at its start date
            for date in date_range:
                monthly_capex = 0
                monthly_equity_capex = 0
                monthly_debt_capex = 0

                # Allocate full capex at construction_start date (which equals ops start for operating assets)
                if date.year == construction_start_normalized.year and date.month == construction_start_normalized.month:
                    monthly_capex = total_capex
                    # For operating assets, treat as acquisition - full equity outflow
                    monthly_equity_capex = total_capex
                    monthly_debt_capex = 0

                capex_values.append(monthly_capex)
                equity_capex_values.append(monthly_equity_capex)
                debt_capex_values.append(monthly_debt_capex)
    elif distribution_method == 'linear':
        monthly_capex_linear = 0
        if construction_months > 0:
            monthly_capex_linear = total_capex / construction_months
        
        # Calculate CAPEX that occurred before model start_date
        pre_model_capex = 0
        if months_before_model_start > 0:
            pre_model_capex = monthly_capex_linear * months_before_model_start
            # Track equity already funded before model start
            if capex_funding_type == 'equity_first':
                # Calculate how much equity was funded in pre-model period
                pre_model_equity = min(pre_model_capex, total_equity_funding)
                current_equity_funded = pre_model_equity
                pre_model_debt = pre_model_capex - pre_model_equity
            elif capex_funding_type == 'pari_passu':
                pre_model_equity = pre_model_capex * (1 - max_gearing)
                pre_model_debt = pre_model_capex * max_gearing
            else:
                pre_model_equity = 0
                pre_model_debt = 0

        # Find the first date in date_range that falls within or after the construction period
        first_model_date_in_construction = None
        construction_completed_before_model = construction_end <= start_date.replace(day=1)
        
        if construction_completed_before_model:
            # Construction already completed before model start - add all CAPEX at first model date
            first_model_date_in_construction = date_range[0] if len(date_range) > 0 else start_date
            # If we haven't calculated pre_model_capex yet, it should be the full CAPEX
            if pre_model_capex == 0:
                pre_model_capex = total_capex
                if capex_funding_type == 'equity_first':
                    pre_model_equity = total_equity_funding
                    pre_model_debt = total_debt_funding
                    current_equity_funded = total_equity_funding
                elif capex_funding_type == 'pari_passu':
                    pre_model_equity = total_equity_funding
                    pre_model_debt = total_debt_funding
        else:
            # Construction is ongoing or hasn't started yet
            for date in date_range:
                if construction_start <= date < construction_end:
                    first_model_date_in_construction = date
                    break
            
            # If construction hasn't started yet in model period, use first date
            if first_model_date_in_construction is None and construction_end > start_date:
                first_model_date_in_construction = date_range[0] if len(date_range) > 0 else start_date

        for date in date_range:
            monthly_capex = 0
            monthly_equity_capex = 0
            monthly_debt_capex = 0

            # If this is the first date in the model period that falls within construction,
            # add the pre-model CAPEX as a lump sum
            if pre_model_capex > 0 and first_model_date_in_construction is not None and \
               date.year == first_model_date_in_construction.year and \
               date.month == first_model_date_in_construction.month:
                monthly_capex = pre_model_capex
                if capex_funding_type == 'equity_first':
                    monthly_equity_capex = pre_model_equity
                    monthly_debt_capex = pre_model_debt
                elif capex_funding_type == 'pari_passu':
                    monthly_equity_capex = pre_model_equity
                    monthly_debt_capex = pre_model_debt
                # Reset pre_model_capex so we don't add it again
                pre_model_capex = 0
            else:
                # Normalize date to first of month for comparison
                date_normalized = date.replace(day=1) if hasattr(date, 'replace') else pd.to_datetime(date).replace(day=1)
                if construction_start_normalized <= date_normalized < construction_end_normalized:
                    # Regular monthly CAPEX allocation
                    monthly_capex = monthly_capex_linear
                
                if capex_funding_type == 'equity_first':
                    if current_equity_funded < total_equity_funding:
                        equity_needed = total_equity_funding - current_equity_funded
                        monthly_equity_capex = min(monthly_capex, equity_needed)
                        monthly_debt_capex = monthly_capex - monthly_equity_capex
                        current_equity_funded += monthly_equity_capex
                    else:
                        monthly_debt_capex = monthly_capex
                elif capex_funding_type == 'pari_passu':
                    monthly_equity_capex = monthly_capex * (1 - max_gearing)
                    monthly_debt_capex = monthly_capex * max_gearing

            capex_values.append(monthly_capex)
            equity_capex_values.append(monthly_equity_capex)
            debt_capex_values.append(monthly_debt_capex)

    elif distribution_method == 'percentage':
        if percentage_distribution is None or sum(percentage_distribution) == 0:
            raise ValueError("percentage_distribution must be provided and sum to a non-zero value for 'percentage' method.")
        
        # Create a list of dates within the construction period
        construction_date_range = pd.date_range(start=construction_start, end=construction_end - relativedelta(months=1), freq='MS')
        
        if len(percentage_distribution) != len(construction_date_range):
            raise ValueError("Length of percentage_distribution must match the number of construction months.")

        # Calculate CAPEX that occurred before model start_date
        pre_model_capex = 0
        pre_model_equity = 0
        pre_model_debt = 0
        percentage_index_start = 0
        
        if months_before_model_start > 0:
            # Sum up the percentages for months before model start
            for i in range(min(months_before_model_start, len(percentage_distribution))):
                pre_model_capex += total_capex * percentage_distribution[i]
            percentage_index_start = months_before_model_start
            
            # Calculate equity/debt split for pre-model CAPEX
            if capex_funding_type == 'equity_first':
                pre_model_equity = min(pre_model_capex, total_equity_funding)
                current_equity_funded = pre_model_equity
                pre_model_debt = pre_model_capex - pre_model_equity
            elif capex_funding_type == 'pari_passu':
                pre_model_equity = pre_model_capex * (1 - max_gearing)
                pre_model_debt = pre_model_capex * max_gearing

        # Find the first date in date_range that falls within or after the construction period
        first_model_date_in_construction = None
        construction_completed_before_model = construction_end_normalized <= start_date_normalized
        
        if construction_completed_before_model:
            # Construction already completed before model start - add all CAPEX at first model date
            first_model_date_in_construction = date_range[0] if len(date_range) > 0 else start_date_normalized
            # If we haven't calculated pre_model_capex yet, it should be the full CAPEX
            if pre_model_capex == 0:
                pre_model_capex = total_capex
                if capex_funding_type == 'equity_first':
                    pre_model_equity = total_equity_funding
                    pre_model_debt = total_debt_funding
                    current_equity_funded = total_equity_funding
                elif capex_funding_type == 'pari_passu':
                    pre_model_equity = total_equity_funding
                    pre_model_debt = total_debt_funding
        else:
            # Construction is ongoing or hasn't started yet
            for date in date_range:
                # Normalize date to first of month for comparison
                date_normalized = date.replace(day=1) if hasattr(date, 'replace') else pd.to_datetime(date).replace(day=1)
                if construction_start_normalized <= date_normalized < construction_end_normalized:
                    first_model_date_in_construction = date
                    break
            
            # If construction hasn't started yet in model period, use first date
            if first_model_date_in_construction is None and construction_end_normalized > start_date_normalized:
                first_model_date_in_construction = date_range[0] if len(date_range) > 0 else start_date_normalized

        percentage_index = percentage_index_start
        for date in date_range:
            monthly_capex = 0
            monthly_equity_capex = 0
            monthly_debt_capex = 0

            # If this is the first date in the model period that falls within construction,
            # add the pre-model CAPEX as a lump sum
            if pre_model_capex > 0 and first_model_date_in_construction is not None and \
               date.year == first_model_date_in_construction.year and \
               date.month == first_model_date_in_construction.month:
                monthly_capex = pre_model_capex
                if capex_funding_type == 'equity_first':
                    monthly_equity_capex = pre_model_equity
                    monthly_debt_capex = pre_model_debt
                elif capex_funding_type == 'pari_passu':
                    monthly_equity_capex = pre_model_equity
                    monthly_debt_capex = pre_model_debt
                # Reset pre_model_capex so we don't add it again
                pre_model_capex = 0
            else:
                # Normalize date to first of month for comparison
                date_normalized = date.replace(day=1) if hasattr(date, 'replace') else pd.to_datetime(date).replace(day=1)
                if construction_start_normalized <= date_normalized < construction_end_normalized:
                    # Regular monthly CAPEX allocation based on percentage
                    if percentage_index < len(percentage_distribution):
                        monthly_capex = total_capex * percentage_distribution[percentage_index]
                        percentage_index += 1
                
                if capex_funding_type == 'equity_first':
                    if current_equity_funded < total_equity_funding:
                        equity_needed = total_equity_funding - current_equity_funded
                        monthly_equity_capex = min(monthly_capex, equity_needed)
                        monthly_debt_capex = monthly_capex - monthly_equity_capex
                        current_equity_funded += monthly_equity_capex
                    else:
                        monthly_debt_capex = monthly_capex
                elif capex_funding_type == 'pari_passu':
                    monthly_equity_capex = monthly_capex * (1 - max_gearing)
                    monthly_debt_capex = monthly_capex * max_gearing

            capex_values.append(monthly_capex)
            equity_capex_values.append(monthly_equity_capex)
            debt_capex_values.append(monthly_debt_capex)

    else:
        raise ValueError("Invalid distribution_method. Must be 'linear' or 'percentage'.")
    
    asset_capex_df = pd.DataFrame({
        'asset_id': asset_id,
        'date': date_range,
        'capex': capex_values,
        'equity_capex': equity_capex_values,
        'debt_capex': debt_capex_values
    })
    return asset_capex_df

def calculate_capex_timeseries(assets, capex_assumptions, start_date, end_date, capex_funding_type=DEFAULT_CAPEX_FUNDING_TYPE):
    """
    Aggregates CAPEX schedules for all assets.

    Args:
        assets (list): A list of asset dictionaries.
        capex_assumptions (dict): A dictionary with CAPEX assumptions.
        start_date (datetime): The start date of the analysis period.
        end_date (datetime): The end date of the analysis period.
        capex_funding_type (str): How CAPEX is funded ('equity_first' or 'pari_passu').

    Returns:
        pd.DataFrame: A DataFrame with columns for asset_id, date, capex, equity_capex, and debt_capex.
    """
    all_capex_data = []

    for asset in assets:
        asset_assumptions = capex_assumptions.get(asset['name'], {})
        total_capex = asset_assumptions.get('capex', 0)
        max_gearing = asset_assumptions.get('maxGearing', 0.7) # Default to 70% gearing
        
        construction_start = pd.to_datetime(asset['constructionStartDate'])
        construction_end = pd.to_datetime(asset['OperatingStartDate']) # Assuming OperatingStartDate is COD

        if total_capex > 0:
            asset_capex_df = calculate_construction_capex_timeseries(
                asset_id=asset['id'],
                total_capex=total_capex,
                construction_start=construction_start,
                construction_end=construction_end,
                start_date=start_date,
                end_date=end_date,
                max_gearing=max_gearing,
                capex_funding_type=capex_funding_type
            )
            all_capex_data.append(asset_capex_df)

    if not all_capex_data:
        return pd.DataFrame(columns=['asset_id', 'date', 'capex', 'equity_capex', 'debt_capex'])

    return pd.concat(all_capex_data, ignore_index=True)

if __name__ == "__main__":
    print("Running standalone example for construction_capex.py")

    # Example 1: Linear Distribution - Equity First Funding
    asset_id_1 = "asset_A_linear_equity_first"
    total_capex_1 = 12000000  # 12 million
    construction_start_1 = datetime(2024, 1, 1)
    construction_end_1 = datetime(2025, 1, 1) # 12 months construction
    start_date_1 = datetime(2023, 1, 1)
    end_date_1 = datetime(2026, 12, 31)
    max_gearing_1 = 0.7
    capex_funding_type_1 = 'equity_first'

    df1 = calculate_construction_capex_timeseries(
        asset_id_1, total_capex_1, construction_start_1, construction_end_1,
        start_date_1, end_date_1, max_gearing_1, capex_funding_type_1,
        distribution_method='linear'
    )
    print("\n--- Example 1: Linear Distribution - Equity First Funding ---")
    print(df1[(df1['capex'] > 0)])

    # Example 2: Linear Distribution - Pari Passu Funding
    asset_id_2 = "asset_B_linear_pari_passu"
    total_capex_2 = 6000000  # 6 million
    construction_start_2 = datetime(2024, 7, 1)
    construction_end_2 = datetime(2025, 7, 1) # 12 months construction
    start_date_2 = datetime(2024, 1, 1)
    end_date_2 = datetime(2025, 12, 31)
    max_gearing_2 = 0.6
    capex_funding_type_2 = 'pari_passu'

    df2 = calculate_construction_capex_timeseries(
        asset_id_2, total_capex_2, construction_start_2, construction_end_2,
        start_date_2, end_date_2, max_gearing_2, capex_funding_type_2,
        distribution_method='linear'
    )
    print("\n--- Example 2: Linear Distribution - Pari Passu Funding ---")
    print(df2[(df2['capex'] > 0)])

    # Example 3: Percentage Distribution - Equity First Funding
    asset_id_3 = "asset_C_percentage_equity_first"
    total_capex_3 = 10000000 # 10 million
    construction_start_3 = datetime(2024, 1, 1)
    construction_end_3 = datetime(2024, 7, 1) # 6 months construction
    start_date_3 = datetime(2023, 1, 1)
    end_date_3 = datetime(2025, 12, 31)
    max_gearing_3 = 0.75
    capex_funding_type_3 = 'equity_first'
    # Example: 10%, 15%, 20%, 20%, 20%, 15% over 6 months
    percentage_dist_3 = [0.10, 0.15, 0.20, 0.20, 0.20, 0.15]

    df3 = calculate_construction_capex_timeseries(
        asset_id_3, total_capex_3, construction_start_3, construction_end_3,
        start_date_3, end_date_3, max_gearing_3, capex_funding_type_3,
        distribution_method='percentage', percentage_distribution=percentage_dist_3
    )
    print("\n--- Example 3: Percentage Distribution - Equity First Funding ---")
    print(df3[(df3['capex'] > 0)])

    # Example 4: Percentage Distribution - Pari Passu Funding
    asset_id_4 = "asset_D_percentage_pari_passu"
    total_capex_4 = 5000000 # 5 million
    construction_start_4 = datetime(2024, 9, 1)
    construction_end_4 = datetime(2025, 1, 1) # 4 months construction
    start_date_4 = datetime(2024, 1, 1)
    end_date_4 = datetime(2025, 12, 31)
    max_gearing_4 = 0.5
    capex_funding_type_4 = 'pari_passu'
    # Example: 20%, 30%, 30%, 20% over 4 months
    percentage_dist_4 = [0.20, 0.30, 0.30, 0.20]

    df4 = calculate_construction_capex_timeseries(
        asset_id_4, total_capex_4, construction_start_4, construction_end_4,
        start_date_4, end_date_4, max_gearing_4, capex_funding_type_4,
        distribution_method='percentage', percentage_distribution=percentage_dist_4
    )
    print("\n--- Example 4: Percentage Distribution - Pari Passu Funding ---")
    print(df4[(df4['capex'] > 0)])
