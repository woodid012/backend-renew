# backend/calculations/revenue.py

import pandas as pd
import numpy as np
import os
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
from .price_curves import get_merchant_price

HOURS_IN_YEAR = 8760
DAYS_IN_MONTH = 30.4375 # Average days in a month
HOURS_IN_MONTH = DAYS_IN_MONTH * 24

def calculate_renewables_revenue(asset, current_date, monthly_prices, yearly_spreads, constants):
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
        contract_start_date = datetime.strptime(contract['startDate'], '%Y-%m-%d')
        years_in_contract = (current_date.year - contract_start_date.year) + (current_date.month - contract_start_date.month) / 12
        indexation = float(contract.get('indexation', 0)) / 100
        indexation_factor = (1 + indexation) ** max(0, years_in_contract)

        if contract['type'] == 'fixed':
            annual_revenue = float(contract.get('strikePrice', 0))
            contract_revenue = annual_revenue / 12 * indexation_factor * degradation_factor
            contracted_energy += contract_revenue
            total_energy_percentage += buyers_percentage * 100

        elif contract['type'] == 'bundled':
            green_price = float(contract.get('greenPrice', 0) or 0)
            energy_price = float(contract.get('EnergyPrice', 0) or 0)

            green_price *= indexation_factor
            energy_price *= indexation_factor

            if contract.get('hasFloor') and (green_price + energy_price) < float(contract.get('floorValue', 0)):
                floor_value = float(contract['floorValue'])
                total_price = green_price + energy_price
                if total_price > 0:
                    green_price = (green_price / total_price) * floor_value
                    energy_price = (energy_price / total_price) * floor_value
                else:
                    green_price = floor_value / 2
                    energy_price = floor_value / 2

            contracted_green += (monthly_generation * buyers_percentage * green_price) / 1_000_000
            contracted_energy += (monthly_generation * buyers_percentage * energy_price) / 1_000_000
            total_green_percentage += buyers_percentage * 100
            total_energy_percentage += buyers_percentage * 100

        else: # Single product contracts (green or Energy)
            price = float(contract.get('strikePrice', 0))
            price *= indexation_factor

            if contract.get('hasFloor') and price < float(contract.get('floorValue', 0)):
                price = float(contract['floorValue'])

            contract_revenue = (monthly_generation * buyers_percentage * price) / 1_000_000

            if contract['type'] == 'green':
                contracted_green += contract_revenue
                total_green_percentage += buyers_percentage * 100
            elif contract['type'] == 'Energy':
                contracted_energy += contract_revenue
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

    merchant_green_price = get_merchant_price(profile, 'green', asset['region'], current_date, monthly_prices, yearly_spreads, constants)
    merchant_energy_price = get_merchant_price(profile, 'Energy', asset['region'], current_date, monthly_prices, yearly_spreads, constants)

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

def calculate_storage_revenue(asset, current_date, monthly_prices, yearly_spreads, constants):
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
        contract_start_date = datetime.strptime(contract['startDate'], '%Y-%m-%d')
        years_in_contract = (current_date.year - contract_start_date.year) + (current_date.month - contract_start_date.month) / 12
        indexation = float(contract.get('indexation', 0)) / 100
        indexation_factor = (1 + indexation) ** max(0, years_in_contract)

        if contract['type'] == 'fixed':
            annual_revenue = float(contract.get('strikePrice', 0))
            contracted_revenue += (annual_revenue / 12 * indexation_factor * degradation_factor)
            total_contracted_percentage += buyers_percentage * 100

        elif contract['type'] == 'cfd':
            price_spread = float(contract.get('strikePrice', 0))
            adjusted_spread = price_spread * indexation_factor
            revenue = monthly_volume * adjusted_spread * buyers_percentage
            contracted_revenue += revenue / 1_000_000
            total_contracted_percentage += buyers_percentage * 100

        elif contract['type'] == 'tolling':
            hourly_rate = float(contract.get('strikePrice', 0))
            adjusted_rate = hourly_rate * indexation_factor
            revenue = capacity * HOURS_IN_MONTH * adjusted_rate * degradation_factor * volume_loss_adjustment
            contracted_revenue += (revenue / 1_000_000)
            total_contracted_percentage += buyers_percentage * 100

    merchant_percentage = max(0, 100 - total_contracted_percentage) / 100
    merchant_revenue = 0

    if merchant_percentage > 0:
        calculated_duration = volume / capacity if capacity > 0 else 0
        
        # Get merchant price using the helper, passing duration as price_type
        price_spread = get_merchant_price('storage', calculated_duration, asset['region'], current_date, monthly_prices, yearly_spreads, constants)
        
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

def export_detailed_revenue(revenue_data_list, output_dir='results'):
    """
    Export detailed revenue breakdown to JSON files.
    
    Args:
        revenue_data_list (list): List of revenue data dictionaries
        output_dir (str): Output directory path
    """
    detailed_output_dir = os.path.join(output_dir, 'detailed-revenue')
    
    # Ensure output directory exists
    os.makedirs(detailed_output_dir, exist_ok=True)
    
    # Convert to DataFrame for easier manipulation
    df = pd.DataFrame(revenue_data_list)
    
    if df.empty:
        print("No revenue data to export")
        return
    
    # Convert date to string for JSON serialization
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    
    # Export by asset
    for asset_id in df['asset_id'].unique():
        asset_data = df[df['asset_id'] == asset_id].to_dict('records')
        asset_file = os.path.join(detailed_output_dir, f'asset_{asset_id}_revenue.json')
        
        with open(asset_file, 'w') as f:
            json.dump(asset_data, f, indent=2)
        
        print(f"Exported detailed revenue for asset {asset_id} to {asset_file}")
    
    # Export combined data
    combined_file = os.path.join(detailed_output_dir, 'all_assets_revenue.json')
    with open(combined_file, 'w') as f:
        json.dump(df.to_dict('records'), f, indent=2)
    
    print(f"Exported combined detailed revenue to {combined_file}")

def calculate_revenue_timeseries(assets, monthly_prices, yearly_spreads, start_date, end_date, output_dir='results'):
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
    detailed_revenue_data = []
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
                    revenue_breakdown = calculate_renewables_revenue(asset, current_date, monthly_prices, yearly_spreads, {}) # Pass constants if needed
                elif asset['type'] == 'storage':
                    revenue_breakdown = calculate_storage_revenue(asset, current_date, monthly_prices, yearly_spreads, {}) # Pass constants if needed
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
            
            # Store for detailed export
            detailed_revenue_data.append({
                'asset_id': asset_id,
                'asset_name': asset.get('name', f'Asset_{asset_id}'),
                'asset_type': asset.get('type', 'unknown'),
                'asset_region': asset.get('region', 'unknown'),
                'date': current_date,
                'total_revenue': revenue_breakdown['total'],
                'contracted_green_revenue': revenue_breakdown['contractedGreen'],
                'contracted_energy_revenue': revenue_breakdown['contractedEnergy'],
                'merchant_green_revenue': revenue_breakdown['merchantGreen'],
                'merchant_energy_revenue': revenue_breakdown['merchantEnergy'],
                'green_percentage_contracted': revenue_breakdown['greenPercentage'],
                'energy_percentage_contracted': revenue_breakdown['EnergyPercentage'],
                'monthly_generation_mwh': revenue_breakdown['monthlyGeneration'],
                'avg_green_price_mwh': revenue_breakdown['avgGreenPrice'],
                'avg_energy_price_mwh': revenue_breakdown['avgEnergyPrice']
            })
            
        all_revenue_data.append(pd.DataFrame(asset_revenues))

    # Export detailed revenue data
    export_detailed_revenue(detailed_revenue_data, output_dir)

    if not all_revenue_data:
        return pd.DataFrame(columns=['asset_id', 'date', 'revenue', 'contractedGreenRevenue', 'contractedEnergyRevenue', 'merchantGreenRevenue', 'merchantEnergyRevenue', 'monthlyGeneration', 'avgGreenPrice', 'avgEnergyPrice'])
        
    return pd.concat(all_revenue_data, ignore_index=True)