# backend/calculations/revenue.py

import pandas as pd
import numpy as np
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from .price_curves import get_merchant_price
from .contracts import calculate_contract_revenue, calculate_storage_contract_revenue, get_contract_strikes_used_timeseries

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

    contract_strikes = []
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

        # Capture contract strike(s) used this month for audit/export
        contract_strikes.append(get_contract_strikes_used_timeseries(contract, current_date))
        
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

    # Volumes by product / exposure
    # NOTE: "black" maps to Energy in this model.
    contracted_green_volume_mwh = monthly_generation * (total_green_percentage / 100)
    contracted_black_volume_mwh = monthly_generation * (total_energy_percentage / 100)
    merchant_green_volume_mwh = monthly_generation * green_merchant_percentage
    merchant_black_volume_mwh = monthly_generation * energy_merchant_percentage

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
        # Audit: the exact market prices used by revenue calc for this month
        # (Green maps to 'green' curve, Black maps to 'Energy' curve)
        'market_price_green_used_$': merchant_green_price,
        'market_price_black_used_$': merchant_energy_price,
        # Storage-only market spread (kept as NaN for non-storage)
        'storage_market_price_used_$': np.nan,
        # Audit: exposure splits (percentages are 0-100, volumes in MWh)
        'pct_green_contracted': total_green_percentage,
        'pct_black_contracted': total_energy_percentage,
        'pct_green_merchant': green_merchant_percentage * 100,
        'pct_black_merchant': energy_merchant_percentage * 100,
        'vol_green_contracted_mwh': contracted_green_volume_mwh,
        'vol_black_contracted_mwh': contracted_black_volume_mwh,
        'vol_green_merchant_mwh': merchant_green_volume_mwh,
        'vol_black_merchant_mwh': merchant_black_volume_mwh,
        'contract_strikes_used_timeseries': contract_strikes,
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

    contract_strikes = []
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

        # Capture contract strike(s) used this month for audit/export
        contract_strikes.append(get_contract_strikes_used_timeseries(contract, current_date))

    merchant_percentage = max(0, 100 - total_contracted_percentage) / 100
    merchant_revenue = 0
    price_spread = 0

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
        # Audit: treat the storage merchant spread as the "black" market input used
        'market_price_green_used_$': 0,
        'market_price_black_used_$': price_spread,
        # Storage-only market spread (this is what you mean by "price spread")
        'storage_market_price_used_$': price_spread,
        'pct_green_contracted': 0,
        'pct_black_contracted': total_contracted_percentage,
        'pct_green_merchant': 0,
        'pct_black_merchant': max(0, 100 - total_contracted_percentage),
        'vol_green_contracted_mwh': 0,
        'vol_black_contracted_mwh': monthly_volume * (total_contracted_percentage / 100),
        'vol_green_merchant_mwh': 0,
        'vol_black_merchant_mwh': monthly_volume * merchant_percentage,
        'contract_strikes_used_timeseries': contract_strikes,
        'greenPercentage': 0,
        'EnergyPercentage': total_contracted_percentage,
        'monthlyGeneration': monthly_volume,
        'avgGreenPrice': 0,
        'avgEnergyPrice': avg_energy_price
    }

def calculate_hybrid_solar_bess_revenue(asset, current_date, monthly_prices, yearly_spreads):
    """
    Calculate revenue for a hybrid Solar + BESS asset by combining solar and storage revenue streams.
    
    Args:
        asset: Asset dictionary with hybrid_solar_bess type, containing:
            - solarCapacity: Solar capacity in MW
            - bessCapacity: BESS capacity in MW
            - bessDuration: BESS duration in hours
            - All other standard asset fields (region, contracts, etc.)
        current_date: Current date for revenue calculation
        monthly_prices: DataFrame with monthly price information
        yearly_spreads: DataFrame with yearly spread information
    
    Returns:
        Dictionary with revenue breakdown including component-specific fields
    """
    # Extract hybrid-specific fields
    solar_capacity = float(asset.get('solarCapacity', 0))
    bess_capacity = float(asset.get('bessCapacity', 0))
    bess_duration = float(asset.get('bessDuration', 0))
    
    if solar_capacity <= 0 or bess_capacity <= 0 or bess_duration <= 0:
        # Return zero revenue if hybrid components are not properly configured
        return {
            'total': 0, 'contractedGreen': 0, 'contractedEnergy': 0,
            'merchantGreen': 0, 'merchantEnergy': 0, 'greenPercentage': 0,
            'EnergyPercentage': 0, 'monthlyGeneration': 0, 'avgGreenPrice': 0, 'avgEnergyPrice': 0,
            'market_price_green_used_$': 0, 'market_price_black_used_$': 0,
            'storage_market_price_used_$': 0,
            'pct_green_contracted': 0, 'pct_black_contracted': 0,
            'pct_green_merchant': 0, 'pct_black_merchant': 0,
            'vol_green_contracted_mwh': 0, 'vol_black_contracted_mwh': 0,
            'vol_green_merchant_mwh': 0, 'vol_black_merchant_mwh': 0,
            # Component-specific fields
            'solarRevenue': 0, 'solarContractedGreenRevenue': 0, 'solarContractedEnergyRevenue': 0,
            'solarMerchantGreenRevenue': 0, 'solarMerchantEnergyRevenue': 0,
            'bessRevenue': 0, 'bessContractedEnergyRevenue': 0, 'bessMerchantEnergyRevenue': 0
        }
    
    # Create temporary solar asset dict
    solar_asset = {
        'id': asset.get('id'),
        'type': 'solar',
        'capacity': solar_capacity,
        'region': asset.get('region', 'NSW'),
        'OperatingStartDate': asset.get('OperatingStartDate'),
        'assetLife': asset.get('assetLife', 25),
        'volumeLossAdjustment': asset.get('volumeLossAdjustment', 95),
        'annualDegradation': asset.get('annualDegradation', 0.5),
        'contracts': asset.get('contracts', []),
        # Copy capacity factors
        'qtrCapacityFactor_q1': asset.get('qtrCapacityFactor_q1'),
        'qtrCapacityFactor_q2': asset.get('qtrCapacityFactor_q2'),
        'qtrCapacityFactor_q3': asset.get('qtrCapacityFactor_q3'),
        'qtrCapacityFactor_q4': asset.get('qtrCapacityFactor_q4'),
        'capacityFactor': asset.get('capacityFactor')
    }
    
    # Create temporary storage asset dict
    bess_volume = bess_capacity * bess_duration
    # Use BESS-specific degradation if available, otherwise default to 1.0% (storage default)
    bess_degradation = asset.get('bessDegradation')
    if bess_degradation is None or bess_degradation == '':
        bess_degradation = 1.0  # Storage default degradation
    else:
        bess_degradation = float(bess_degradation)
    
    storage_asset = {
        'id': asset.get('id'),
        'type': 'storage',
        'capacity': bess_capacity,
        'volume': bess_volume,
        'durationHours': bess_duration,
        'region': asset.get('region', 'NSW'),
        'OperatingStartDate': asset.get('OperatingStartDate'),
        'assetLife': asset.get('assetLife', 25),
        'volumeLossAdjustment': asset.get('volumeLossAdjustment', 95),
        'annualDegradation': bess_degradation,  # Use BESS-specific degradation
        'contracts': asset.get('contracts', [])
    }
    
    # Calculate revenue for each component
    solar_revenue = calculate_renewables_revenue(solar_asset, current_date, monthly_prices, yearly_spreads)
    bess_revenue = calculate_storage_revenue(storage_asset, current_date, monthly_prices, yearly_spreads)
    
    # Extract component-specific revenue
    solar_contracted_green = solar_revenue.get('contractedGreen', 0)
    solar_contracted_energy = solar_revenue.get('contractedEnergy', 0)
    solar_merchant_green = solar_revenue.get('merchantGreen', 0)
    solar_merchant_energy = solar_revenue.get('merchantEnergy', 0)
    solar_total = solar_contracted_green + solar_contracted_energy + solar_merchant_green + solar_merchant_energy
    
    bess_contracted_energy = bess_revenue.get('contractedEnergy', 0)
    bess_merchant_energy = bess_revenue.get('merchantEnergy', 0)
    bess_total = bess_contracted_energy + bess_merchant_energy
    
    # Combine standard revenue fields (for backward compatibility)
    combined_contracted_green = solar_contracted_green  # BESS has no green revenue
    combined_contracted_energy = solar_contracted_energy + bess_contracted_energy
    combined_merchant_green = solar_merchant_green  # BESS has no green revenue
    combined_merchant_energy = solar_merchant_energy + bess_merchant_energy
    combined_total = combined_contracted_green + combined_contracted_energy + combined_merchant_green + combined_merchant_energy
    
    # Combine volumes
    combined_monthly_generation = solar_revenue.get('monthlyGeneration', 0) + bess_revenue.get('monthlyGeneration', 0)
    
    # Calculate combined percentages (weighted by generation)
    solar_gen = solar_revenue.get('monthlyGeneration', 0)
    bess_gen = bess_revenue.get('monthlyGeneration', 0)
    total_gen = solar_gen + bess_gen if (solar_gen + bess_gen) > 0 else 1
    
    # Weighted average prices
    combined_avg_green_price = solar_revenue.get('avgGreenPrice', 0)  # Only solar has green
    combined_avg_energy_price = (
        (solar_revenue.get('avgEnergyPrice', 0) * solar_gen + 
         bess_revenue.get('avgEnergyPrice', 0) * bess_gen) / total_gen
        if total_gen > 0 else 0
    )
    
    return {
        'total': combined_total,
        'contractedGreen': combined_contracted_green,
        'contractedEnergy': combined_contracted_energy,
        'merchantGreen': combined_merchant_green,
        'merchantEnergy': combined_merchant_energy,
        'greenPercentage': solar_revenue.get('greenPercentage', 0),  # Only solar has green
        'EnergyPercentage': (solar_revenue.get('EnergyPercentage', 0) * solar_gen + 
                            bess_revenue.get('EnergyPercentage', 0) * bess_gen) / total_gen if total_gen > 0 else 0,
        'monthlyGeneration': combined_monthly_generation,
        'avgGreenPrice': combined_avg_green_price,
        'avgEnergyPrice': combined_avg_energy_price,
        'market_price_green_used_$': solar_revenue.get('market_price_green_used_$', 0),
        'market_price_black_used_$': (solar_revenue.get('market_price_black_used_$', 0) + 
                                      bess_revenue.get('market_price_black_used_$', 0)) / 2,
        'storage_market_price_used_$': bess_revenue.get('storage_market_price_used_$', 0),
        'pct_green_contracted': solar_revenue.get('pct_green_contracted', 0),
        'pct_black_contracted': (solar_revenue.get('pct_black_contracted', 0) + 
                                bess_revenue.get('pct_black_contracted', 0)),
        'pct_green_merchant': solar_revenue.get('pct_green_merchant', 0),
        'pct_black_merchant': (solar_revenue.get('pct_black_merchant', 0) + 
                              bess_revenue.get('pct_black_merchant', 0)),
        'vol_green_contracted_mwh': solar_revenue.get('vol_green_contracted_mwh', 0),
        'vol_black_contracted_mwh': (solar_revenue.get('vol_black_contracted_mwh', 0) + 
                                    bess_revenue.get('vol_black_contracted_mwh', 0)),
        'vol_green_merchant_mwh': solar_revenue.get('vol_green_merchant_mwh', 0),
        'vol_black_merchant_mwh': (solar_revenue.get('vol_black_merchant_mwh', 0) + 
                                   bess_revenue.get('vol_black_merchant_mwh', 0)),
        # Component-specific fields
        'solarRevenue': solar_total,
        'solarContractedGreenRevenue': solar_contracted_green,
        'solarContractedEnergyRevenue': solar_contracted_energy,
        'solarMerchantGreenRevenue': solar_merchant_green,
        'solarMerchantEnergyRevenue': solar_merchant_energy,
        'bessRevenue': bess_total,
        'bessContractedEnergyRevenue': bess_contracted_energy,
        'bessMerchantEnergyRevenue': bess_merchant_energy
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

    max_contracts = max((len(a.get('contracts', []) or []) for a in assets), default=0)

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
                'EnergyPercentage': 0, 'monthlyGeneration': 0, 'avgGreenPrice': 0, 'avgEnergyPrice': 0,
                'market_price_green_used_$': 0, 'market_price_black_used_$': 0,
                'storage_market_price_used_$': 0,
                'pct_green_contracted': 0, 'pct_black_contracted': 0,
                'pct_green_merchant': 0, 'pct_black_merchant': 0,
                'vol_green_contracted_mwh': 0, 'vol_black_contracted_mwh': 0,
                'vol_green_merchant_mwh': 0, 'vol_black_merchant_mwh': 0
            }

            # Only calculate revenue if the asset is operational and within its asset life
            if current_date >= asset_start_date and current_date < asset_life_end_date:
                if asset['type'] in ['solar', 'wind']:
                    revenue_breakdown = calculate_renewables_revenue(asset, current_date, monthly_prices, yearly_spreads) # Pass constants if needed
                elif asset['type'] == 'storage':
                    revenue_breakdown = calculate_storage_revenue(asset, current_date, monthly_prices, yearly_spreads)
                elif asset['type'] == 'hybrid_solar_bess':
                    revenue_breakdown = calculate_hybrid_solar_bess_revenue(asset, current_date, monthly_prices, yearly_spreads)
                else:
                    # Handle unknown asset types by returning zero revenue
                    revenue_breakdown = {
                        'total': 0, 'contractedGreen': 0, 'contractedEnergy': 0,
                        'merchantGreen': 0, 'merchantEnergy': 0, 'greenPercentage': 0,
                        'EnergyPercentage': 0, 'monthlyGeneration': 0, 'avgGreenPrice': 0, 'avgEnergyPrice': 0,
                        'market_price_green_used_$': 0, 'market_price_black_used_$': 0,
                        'storage_market_price_used_$': 0,
                        'pct_green_contracted': 0, 'pct_black_contracted': 0,
                        'pct_green_merchant': 0, 'pct_black_merchant': 0,
                        'vol_green_contracted_mwh': 0, 'vol_black_contracted_mwh': 0,
                        'vol_green_merchant_mwh': 0, 'vol_black_merchant_mwh': 0
                    }
            # If current_date < asset_start_date, revenue_breakdown remains the initialized zero-revenue dict

            # Store for main output
            row = {
                'asset_id': asset_id,
                'date': current_date,
                'revenue': revenue_breakdown['total'],
                'contractedGreenRevenue': revenue_breakdown['contractedGreen'],
                'contractedEnergyRevenue': revenue_breakdown['contractedEnergy'],
                'merchantGreenRevenue': revenue_breakdown['merchantGreen'],
                'merchantEnergyRevenue': revenue_breakdown['merchantEnergy'],
                'market_price_green_used_$': revenue_breakdown.get('market_price_green_used_$', 0),
                'market_price_black_used_$': revenue_breakdown.get('market_price_black_used_$', 0),
                'storage_market_price_used_$': revenue_breakdown.get('storage_market_price_used_$', 0),
                'pct_green_contracted': revenue_breakdown.get('pct_green_contracted', 0),
                'pct_black_contracted': revenue_breakdown.get('pct_black_contracted', 0),
                'pct_green_merchant': revenue_breakdown.get('pct_green_merchant', 0),
                'pct_black_merchant': revenue_breakdown.get('pct_black_merchant', 0),
                'vol_green_contracted_mwh': revenue_breakdown.get('vol_green_contracted_mwh', 0),
                'vol_black_contracted_mwh': revenue_breakdown.get('vol_black_contracted_mwh', 0),
                # Component-specific revenue fields (for hybrid assets)
                'solarRevenue': revenue_breakdown.get('solarRevenue', 0),
                'solarContractedGreenRevenue': revenue_breakdown.get('solarContractedGreenRevenue', 0),
                'solarContractedEnergyRevenue': revenue_breakdown.get('solarContractedEnergyRevenue', 0),
                'solarMerchantGreenRevenue': revenue_breakdown.get('solarMerchantGreenRevenue', 0),
                'solarMerchantEnergyRevenue': revenue_breakdown.get('solarMerchantEnergyRevenue', 0),
                'bessRevenue': revenue_breakdown.get('bessRevenue', 0),
                'bessContractedEnergyRevenue': revenue_breakdown.get('bessContractedEnergyRevenue', 0),
                'bessMerchantEnergyRevenue': revenue_breakdown.get('bessMerchantEnergyRevenue', 0),
                'vol_green_merchant_mwh': revenue_breakdown.get('vol_green_merchant_mwh', 0),
                'vol_black_merchant_mwh': revenue_breakdown.get('vol_black_merchant_mwh', 0),
                'monthlyGeneration': revenue_breakdown['monthlyGeneration'],
                'avgGreenPrice': revenue_breakdown['avgGreenPrice'],
                'avgEnergyPrice': revenue_breakdown['avgEnergyPrice']
            }

            # Contract time series columns (1..max_contracts, stable order from asset['contracts'])
            # Note: We export "used strike" values for the current month if the contract is active; else blank.
            contracts = asset.get('contracts', []) or []
            for idx in range(max_contracts):
                n = idx + 1
                prefix = f'contract_{n}'

                if idx < len(contracts):
                    c = contracts[idx]
                    c_start = datetime.strptime(c['startDate'], '%Y-%m-%d')
                    c_end = datetime.strptime(c['endDate'], '%Y-%m-%d')
                    is_active = c_start <= current_date.to_pydatetime() <= c_end

                    strikes = get_contract_strikes_used_timeseries(c, current_date.to_pydatetime()) if is_active else {
                        'indexation_factor': None,
                        'strike_green_used_$': None,
                        'strike_black_used_$': None,
                        'strike_storage_used_$': None,
                    }

                    contract_name = c.get('name') or c.get('contractName') or c.get('buyer') or c.get('buyerName')

                    row.update({
                        f'{prefix}_name': contract_name,
                        f'{prefix}_type': c.get('type'),
                        f'{prefix}_buyers_percentage': c.get('buyersPercentage'),
                        f'{prefix}_start_date': c_start,
                        f'{prefix}_end_date': c_end,
                        f'{prefix}_is_active': bool(is_active),
                        f'{prefix}_indexation_factor': strikes.get('indexation_factor'),
                        f'{prefix}_strike_green_used_$': strikes.get('strike_green_used_$'),
                        f'{prefix}_strike_black_used_$': strikes.get('strike_black_used_$'),
                        f'{prefix}_strike_storage_used_$': strikes.get('strike_storage_used_$'),
                    })
                else:
                    row.update({
                        f'{prefix}_name': None,
                        f'{prefix}_type': None,
                        f'{prefix}_buyers_percentage': None,
                        f'{prefix}_start_date': pd.NaT,
                        f'{prefix}_end_date': pd.NaT,
                        f'{prefix}_is_active': False,
                        f'{prefix}_indexation_factor': None,
                        f'{prefix}_strike_green_used_$': None,
                        f'{prefix}_strike_black_used_$': None,
                        f'{prefix}_strike_storage_used_$': None,
                    })

            asset_revenues.append(row)
            
        all_revenue_data.append(pd.DataFrame(asset_revenues))

    if not all_revenue_data:
        return pd.DataFrame(columns=[
            'asset_id', 'date', 'revenue',
            'contractedGreenRevenue', 'contractedEnergyRevenue',
            'merchantGreenRevenue', 'merchantEnergyRevenue',
            'market_price_green_used_$', 'market_price_black_used_$',
            'storage_market_price_used_$',
            'pct_green_contracted', 'pct_black_contracted',
            'pct_green_merchant', 'pct_black_merchant',
            'vol_green_contracted_mwh', 'vol_black_contracted_mwh',
            'vol_green_merchant_mwh', 'vol_black_merchant_mwh',
            'monthlyGeneration', 'avgGreenPrice', 'avgEnergyPrice'
        ])
        
    return pd.concat(all_revenue_data, ignore_index=True)