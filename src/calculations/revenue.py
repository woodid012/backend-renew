# backend/calculations/revenue.py

import pandas as pd
import numpy as np
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from .price_curves import get_merchant_price
from .contracts import calculate_contract_revenue, calculate_storage_contract_revenue

HOURS_IN_YEAR = 8760
DAYS_IN_MONTH = 30.4375 # Average days in a month
HOURS_IN_MONTH = DAYS_IN_MONTH * 24

def calculate_renewables_revenue(asset, current_date, monthly_prices, yearly_spreads):
    asset_start_date = datetime.strptime(asset['OperatingStartDate'], '%Y-%m-%d')
    # Determine capacity factor for the current month/quarter
    capacity_factor = 0.25 # Default fallback
    
    # Try to get quarterly capacity factor if available
    quarter = (current_date.month - 1) // 3 + 1
    quarter_key = f'qtrCapacityFactor_q{quarter}'
    if quarter_key in asset and asset[quarter_key] not in ['', None]:
        capacity_factor = float(asset[quarter_key]) / 100
    elif 'capacityFactor' in asset and asset['capacityFactor'] not in ['', None]:
        capacity_factor = float(asset['capacityFactor']) / 100
    else:
        # Default capacity factors by technology and region if not specified in asset
        default_factors = {
            'solar': {'NSW': 0.28, 'VIC': 0.25, 'QLD': 0.29, 'SA': 0.27, 'WA': 0.26, 'TAS': 0.23},
            'wind': {'NSW': 0.35, 'VIC': 0.38, 'QLD': 0.32, 'SA': 0.40, 'WA': 0.37, 'TAS': 0.42}
        }
        capacity_factor = default_factors.get(asset['type'], {}).get(asset['region'], 0.25)

    capacity = float(asset.get('capacity', 0))
    volume_loss_adjustment = float(asset.get('volumeLossAdjustment', 95)) / 100

    # Calculate degradation factor
    asset_start_date = datetime.strptime(asset['OperatingStartDate'], '%Y-%m-%d')
    years_since_start = (current_date.year - asset_start_date.year) + (current_date.month - asset_start_date.month) / 12
    degradation = float(asset.get('annualDegradation', 0.5)) / 100
    degradation_factor = (1 - degradation) ** max(0, years_since_start)

    # Monthly generation
    monthly_generation = capacity * volume_loss_adjustment * (HOURS_IN_YEAR / 12) * \
                         capacity_factor * degradation_factor

    contracted_green = 0
    contracted_energy = 0
    total_green_percentage = 0
    total_energy_percentage = 0

    active_contracts = [c for c in asset.get('contracts', []) if
                        datetime.strptime(c['startDate'], '%Y-%m-%d') <= current_date <= datetime.strptime(c['endDate'], '%Y-%m-%d')]

    for contract in active_contracts:
        buyers_percentage = float(contract.get('buyersPercentage', 0)) / 100
        
        # Use the new modular contract calculator
        revenue_components = calculate_contract_revenue(
            contract, 
            current_date, 
            monthly_generation, 
            buyers_percentage, 
            degradation_factor
        )
        
        contracted_green += revenue_components['contracted_green']
        contracted_energy += revenue_components['contracted_energy']
        
        # Update percentages based on contract type
        contract_type = contract.get('type')
        if contract_type == 'fixed':
            total_energy_percentage += buyers_percentage * 100
        elif contract_type == 'bundled':
            total_green_percentage += buyers_percentage * 100
            total_energy_percentage += buyers_percentage * 100
        elif contract_type == 'green':
            total_green_percentage += buyers_percentage * 100
        elif contract_type == 'Energy':
            total_energy_percentage += buyers_percentage * 100

    # Calculate merchant revenue (moved outside the contract loop)
    green_merchant_percentage = max(0, 100 - total_green_percentage) / 100
    energy_merchant_percentage = max(0, 100 - total_energy_percentage) / 100

    profile_map = {
        'solar': 'solar',
        'wind': 'wind',
        'storage': 'storage'
    }
    profile = profile_map.get(asset['type'], asset['type'])

    merchant_green_price = get_merchant_price(profile, 'green', asset['region'], current_date, monthly_prices, yearly_spreads)
    merchant_energy_price = get_merchant_price(profile, 'Energy', asset['region'], current_date, monthly_prices, yearly_spreads)

    merchant_green = (monthly_generation * green_merchant_percentage * merchant_green_price) / 1_000_000
    merchant_energy = (monthly_generation * energy_merchant_percentage * merchant_energy_price) / 1_000_000

    # Calculate average prices (Revenue / Volume)
    green_volume = monthly_generation * (total_green_percentage + green_merchant_percentage * 100) / 100
    energy_volume = monthly_generation * (total_energy_percentage + energy_merchant_percentage * 100) / 100
    
    avg_green_price = ((contracted_green + merchant_green) * 1_000_000 / green_volume) if green_volume > 0 else 0
    avg_energy_price = ((contracted_energy + merchant_energy) * 1_000_000 / energy_volume) if energy_volume > 0 else 0

    return {
        'total': contracted_green + contracted_energy + merchant_green + merchant_energy,
        'contractedGreen': contracted_green,
        'contractedEnergy': contracted_energy,
        'merchantGreen': merchant_green,
        'merchantEnergy': merchant_energy,
        'greenPercentage': total_green_percentage,
        'EnergyPercentage': total_energy_percentage,
        'monthlyGeneration': monthly_generation,
        'avgGreenPrice': avg_green_price,
        'avgEnergyPrice': avg_energy_price
    }

def calculate_storage_revenue(asset, current_date, monthly_prices, yearly_spreads):
    volume = float(asset.get('volume', 0))
    capacity = float(asset.get('capacity', 0))
    volume_loss_adjustment = float(asset.get('volumeLossAdjustment', 95)) / 100
    
    asset_start_date = datetime.strptime(asset['OperatingStartDate'], '%Y-%m-%d')
    years_since_start = (current_date.year - asset_start_date.year) + (current_date.month - asset_start_date.month) / 12
    degradation = float(asset.get('annualDegradation', 0.5)) / 100
    degradation_factor = (1 - degradation) ** max(0, years_since_start)

    # Monthly Volume = Volume × (1 - Degradation) × (Days in Month)
    monthly_volume = volume * degradation_factor * volume_loss_adjustment * DAYS_IN_MONTH

    contracted_revenue = 0
    total_contracted_percentage = 0

    active_contracts = [c for c in asset.get('contracts', []) if
                        datetime.strptime(c['startDate'], '%Y-%m-%d') <= current_date <= datetime.strptime(c['endDate'], '%Y-%m-%d')]

    for contract in active_contracts:
        buyers_percentage = float(contract.get('buyersPercentage', 0)) / 100
        
        # Use the new modular contract calculator
        revenue = calculate_storage_contract_revenue(
            contract,
            current_date,
            monthly_volume,
            capacity,
            buyers_percentage,
            degradation_factor,
            volume_loss_adjustment,
            HOURS_IN_MONTH
        )
        
        contracted_revenue += revenue
        total_contracted_percentage += buyers_percentage * 100

    merchant_percentage = max(0, 100 - total_contracted_percentage) / 100
    merchant_revenue = 0

    if merchant_percentage > 0:
        # Use durationHours from asset if set, otherwise calculate from volume/capacity
        if 'durationHours' in asset and asset.get('durationHours') not in ['', None]:
            duration = float(asset['durationHours'])
        else:
            # Fallback: calculate duration from volume and capacity
            duration = volume / capacity if capacity > 0 else 0
            if duration == 0:
                # If still 0, try to get default from asset defaults
                from ..core.asset_defaults import get_asset_default_config
                storage_defaults = get_asset_default_config('storage')
                duration = storage_defaults.get('durationHours', 2)  # Default to 2 hours
        
        # Get merchant price using the helper, passing duration as price_type
        price_spread = get_merchant_price('storage', duration, asset['region'], current_date, monthly_prices, yearly_spreads)
        
        revenue = monthly_volume * price_spread * merchant_percentage
        merchant_revenue = revenue / 1_000_000

    # Calculate average prices (Revenue / Volume) - for storage, this is typically energy price
    contracted_volume = monthly_volume * total_contracted_percentage / 100
    merchant_volume = monthly_volume * merchant_percentage
    total_volume = contracted_volume + merchant_volume
    
    avg_energy_price = ((contracted_revenue + merchant_revenue) * 1_000_000 / total_volume) if total_volume > 0 else 0

    return {
        'total': contracted_revenue + merchant_revenue,
        'contractedGreen': 0, # Storage typically doesn't have green revenue
        'contractedEnergy': contracted_revenue,
        'merchantGreen': 0, # Storage typically doesn't have green revenue
        'merchantEnergy': merchant_revenue,
        'greenPercentage': 0,
        'EnergyPercentage': total_contracted_percentage,
        'monthlyGeneration': monthly_volume,
        'avgGreenPrice': 0,
        'avgEnergyPrice': avg_energy_price
    }

def calculate_revenue_timeseries(assets, monthly_prices, yearly_spreads, start_date, end_date, output_dir='output/model_results'):
    """
    Calculates monthly revenue for each asset over a specified time period.

    Args:
        assets (list): A list of asset dictionaries.
        monthly_prices (pd.DataFrame): A DataFrame with monthly price information.
        yearly_spreads (pd.DataFrame): A DataFrame with yearly spread information.
        start_date (datetime): The start date of the analysis period.
        end_date (datetime): The end date of the analysis period.
        output_dir (str): Output directory for detailed revenue export.

    Returns:
        pd.DataFrame: A DataFrame with columns for asset_id, date, and revenue.
    """
    all_revenue_data = []
    date_range = pd.date_range(start=start_date, end=end_date, freq='MS')

    for asset in assets:
        asset_id = asset['id']
        asset_revenues = []
        
        # Ensure OperatingStartDate is a datetime object for comparison
        asset_start_date = datetime.strptime(asset['OperatingStartDate'], '%Y-%m-%d')
        asset_life_end_date = asset_start_date + relativedelta(years=int(asset.get('assetLife', 25)))

        for current_date in date_range:
            revenue_breakdown = {
                'total': 0, 'contractedGreen': 0, 'contractedEnergy': 0,
                'merchantGreen': 0, 'merchantEnergy': 0, 'greenPercentage': 0,
                'EnergyPercentage': 0, 'monthlyGeneration': 0, 'avgGreenPrice': 0, 'avgEnergyPrice': 0
            }

            # Only calculate revenue if the asset is operational and within its asset life
            if current_date >= asset_start_date and current_date < asset_life_end_date:
                if asset['type'] in ['solar', 'wind']:
                    revenue_breakdown = calculate_renewables_revenue(asset, current_date, monthly_prices, yearly_spreads) # Pass constants if needed
                elif asset['type'] == 'storage':
                    revenue_breakdown = calculate_storage_revenue(asset, current_date, monthly_prices, yearly_spreads)
                else:
                    # Handle unknown asset types by returning zero revenue
                    revenue_breakdown = {
                        'total': 0, 'contractedGreen': 0, 'contractedEnergy': 0,
                        'merchantGreen': 0, 'merchantEnergy': 0, 'greenPercentage': 0,
                        'EnergyPercentage': 0, 'monthlyGeneration': 0, 'avgGreenPrice': 0, 'avgEnergyPrice': 0
                    }
            # If current_date < asset_start_date, revenue_breakdown remains the initialized zero-revenue dict

            # Store for main output
            asset_revenues.append({
                'asset_id': asset_id,
                'date': current_date,
                'revenue': revenue_breakdown['total'],
                'contractedGreenRevenue': revenue_breakdown['contractedGreen'],
                'contractedEnergyRevenue': revenue_breakdown['contractedEnergy'],
                'merchantGreenRevenue': revenue_breakdown['merchantGreen'],
                'merchantEnergyRevenue': revenue_breakdown['merchantEnergy'],
                'monthlyGeneration': revenue_breakdown['monthlyGeneration'],
                'avgGreenPrice': revenue_breakdown['avgGreenPrice'],
                'avgEnergyPrice': revenue_breakdown['avgEnergyPrice']
            })
            
        all_revenue_data.append(pd.DataFrame(asset_revenues))

    if not all_revenue_data:
        return pd.DataFrame(columns=['asset_id', 'date', 'revenue', 'contractedGreenRevenue', 'contractedEnergyRevenue', 'merchantGreenRevenue', 'merchantEnergyRevenue', 'monthlyGeneration', 'avgGreenPrice', 'avgEnergyPrice'])
        
    return pd.concat(all_revenue_data, ignore_index=True)