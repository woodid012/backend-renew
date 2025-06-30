# backend/calculations/price_curves.py
import pandas as pd
import numpy as np
from datetime import datetime
from config import MERCHANT_PRICE_ESCALATION_RATE, MERCHANT_PRICE_ESCALATION_REFERENCE_DATE

def get_merchant_price(profile, price_type, region, date, monthly_prices, yearly_spreads, constants):
    # Ensure date is a datetime object
    if isinstance(date, str):
        date = datetime.strptime(date, '%Y-%m-%d')

    # Calculate escalation factor
    reference_date = datetime.strptime(MERCHANT_PRICE_ESCALATION_REFERENCE_DATE, '%Y-%m-%d')
    years_from_reference = (date.year - reference_date.year) + (date.month - reference_date.month) / 12
    escalation_factor = (1 + MERCHANT_PRICE_ESCALATION_RATE) ** max(0, years_from_reference)

    # For yearly spreads (storage duration based)
    if isinstance(price_type, (float, int)):
        # Find the closest duration in yearly_spreads
        duration = float(price_type)
        relevant_spreads = yearly_spreads[
            (yearly_spreads['REGION'] == region) & 
            (yearly_spreads['YEAR'] == date.year)
        ]

        if relevant_spreads.empty:
            print(f"Warning: No yearly spread data found for {region} in {date.year}")
            return 50 * escalation_factor # Default fallback with escalation

        # Sort by duration to find interpolation points
        relevant_spreads = relevant_spreads.sort_values(by='DURATION')

        lower_duration_row = relevant_spreads[relevant_spreads['DURATION'] <= duration].iloc[-1:]
        upper_duration_row = relevant_spreads[relevant_spreads['DURATION'] >= duration].iloc[:1]

        if not lower_duration_row.empty and not upper_duration_row.empty:
            lower_duration = lower_duration_row['DURATION'].iloc[0]
            upper_duration = upper_duration_row['DURATION'].iloc[0]
            lower_price = lower_duration_row['SPREAD'].iloc[0]
            upper_price = upper_duration_row['SPREAD'].iloc[0]

            if lower_duration == upper_duration:
                base_price = lower_price
            else:
                # Linear interpolation
                base_price = lower_price + (upper_price - lower_price) * \
                           (duration - lower_duration) / (upper_duration - lower_duration)
        elif not lower_duration_row.empty:
            base_price = lower_duration_row['SPREAD'].iloc[0]
        elif not upper_duration_row.empty:
            base_price = upper_duration_row['SPREAD'].iloc[0]
        else:
            print(f"Warning: No matching duration data for {duration}h storage in {region}")
            base_price = 50 # Default fallback

        return base_price * escalation_factor

    # For monthly prices (Energy/green)
    else:
        # Ensure monthly_prices has proper datetime column
        if '_time_dt' not in monthly_prices.columns:
            monthly_prices['_time_dt'] = pd.to_datetime(monthly_prices['time'], format='%d/%m/%Y', errors='coerce')
        
        # Filter for the exact month and year
        filtered_prices = monthly_prices[
            (monthly_prices['profile'] == profile) &
            (monthly_prices['type'] == price_type) &
            (monthly_prices['REGION'] == region) &
            (monthly_prices['_time_dt'].dt.year == date.year) &
            (monthly_prices['_time_dt'].dt.month == date.month)
        ]

        if not filtered_prices.empty:
            base_price = filtered_prices['price'].iloc[0]
            print(f"Found price for {profile}-{price_type} in {region} {date.strftime('%Y-%m')}: ${base_price:.2f} -> ${base_price * escalation_factor:.2f}")
            return base_price * escalation_factor
        else:
            # Fallback logic: search backwards for valid prices
            print(f"No direct price match for {profile}-{price_type} in {region} {date.strftime('%Y-%m')}, searching backwards...")
            
            year_to_try = date.year
            month_to_try = date.month
            
            for attempt in range(12 * 5): # Try up to 5 years back
                month_to_try -= 1
                if month_to_try == 0:
                    month_to_try = 12
                    year_to_try -= 1
                
                fallback_prices = monthly_prices[
                    (monthly_prices['profile'] == profile) &
                    (monthly_prices['type'] == price_type) &
                    (monthly_prices['REGION'] == region) &
                    (monthly_prices['_time_dt'].dt.year == year_to_try) &
                    (monthly_prices['_time_dt'].dt.month == month_to_try)
                ]
                
                if not fallback_prices.empty:
                    base_price = fallback_prices['price'].iloc[0]
                    print(f"Found fallback price from {year_to_try}-{month_to_try:02d}: ${base_price:.2f} -> ${base_price * escalation_factor:.2f}")
                    return base_price * escalation_factor
            
            # Final fallback - check what data we actually have
            available_profiles = monthly_prices['profile'].unique()
            available_types = monthly_prices['type'].unique()
            available_regions = monthly_prices['REGION'].unique()
            
            print(f"ERROR: No price data found for {profile}-{price_type} in {region}")
            print(f"Available profiles: {available_profiles}")
            print(f"Available types: {available_types}")
            print(f"Available regions: {available_regions}")
            
            return 50 * escalation_factor # Default fallback with escalation