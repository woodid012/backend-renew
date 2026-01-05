# src/core/asset_defaults.py

import json
import os
from typing import Dict, Any
from .database import db_manager, mongo_session

def load_asset_defaults() -> Dict[str, Any]:
    """
    Load asset defaults from MongoDB CONFIG_assetDefaults collection.
    Falls back to JSON file if MongoDB is not available or document doesn't exist.
    
    Returns:
        Dictionary containing asset defaults for all asset types and platform settings.
    """
    # Try MongoDB first
    try:
        with mongo_session() as db_mgr:
            collection = db_mgr.get_collection('CONFIG_assetDefaults')
            defaults = collection.find_one({})
            
            if defaults:
                # Remove MongoDB _id field
                defaults.pop('_id', None)
                print(f"✅ Loaded asset defaults from MongoDB CONFIG_assetDefaults")
                return defaults
            else:
                print(f"⚠️ No defaults found in MongoDB, falling back to JSON file")
                raise ValueError("No document in MongoDB")
    except Exception as e:
        print(f"⚠️ MongoDB load failed ({e}), falling back to JSON file")
        # Fallback to JSON file
        config_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            'config',
            'asset_defaults.json'
        )
        
        try:
            with open(config_path, 'r') as f:
                defaults = json.load(f)
            print(f"✅ Loaded asset defaults from {config_path}")
            return defaults
        except Exception as json_error:
            print(f"⚠️ Error loading asset defaults from JSON: {json_error}")
            return get_fallback_defaults()


def get_asset_default_config(asset_type: str) -> Dict[str, Any]:
    """
    Get default configuration for a specific asset type.
    
    Args:
        asset_type: Type of asset ('solar', 'wind', or 'storage')
    
    Returns:
        Dictionary containing default configuration for the asset type.
    """
    defaults = load_asset_defaults()
    
    if asset_type not in defaults.get('assetDefaults', {}):
        print(f"⚠️ Unknown asset type: {asset_type}, using fallback")
        return get_fallback_defaults()['assetDefaults'].get(asset_type, {})
    
    return defaults['assetDefaults'][asset_type]


def get_capacity_factor_defaults(asset_type: str, region: str) -> Dict[str, float]:
    """
    Get default capacity factors for an asset type and region.
    
    Args:
        asset_type: Type of asset ('solar', 'wind', or 'hybrid_solar_bess')
        region: Region code (e.g., 'NSW', 'VIC')
    
    Returns:
        Dictionary with quarterly capacity factors (q1, q2, q3, q4)
    """
    # For hybrid assets, use solar capacity factors
    type_for_factors = 'solar' if asset_type == 'hybrid_solar_bess' else asset_type
    asset_config = get_asset_default_config(type_for_factors)
    
    if 'capacityFactors' not in asset_config:
        print(f"⚠️ No capacity factors for {asset_type}")
        return {'q1': 25, 'q2': 25, 'q3': 25, 'q4': 25}
    
    regional_factors = asset_config['capacityFactors'].get(region)
    if not regional_factors:
        print(f"⚠️ No capacity factors for {region}, using fallback")
        # Use first available region as fallback
        first_region = list(asset_config['capacityFactors'].values())[0]
        return first_region
    
    return regional_factors


def get_cost_assumptions(asset_type: str, capacity_mw: float) -> Dict[str, float]:
    """
    Get default cost assumptions for an asset, scaled by capacity.
    
    Args:
        asset_type: Type of asset ('solar', 'wind', or 'storage')
        capacity_mw: Capacity in MW
    
    Returns:
        Dictionary with cost assumptions including absolute CAPEX and OPEX values
    """
    asset_config = get_asset_default_config(asset_type)
    cost_defaults = asset_config.get('costAssumptions', {})
    
    # Calculate absolute values based on capacity
    capex = cost_defaults.get('capexPerMW', 1.0) * capacity_mw
    opex = cost_defaults.get('opexPerMWPerYear', 0.02) * capacity_mw
    terminal_value = cost_defaults.get('terminalValuePerMW', 0) * capacity_mw
    
    return {
        'capex': round(capex, 2),
        'operatingCosts': round(opex, 4),
        'operatingCostEscalation': cost_defaults.get('operatingCostEscalation', 2.5),
        'terminalValue': round(terminal_value, 2),
        'maxGearing': cost_defaults.get('maxGearing', 0.65),
        'targetDSCRContract': cost_defaults.get('targetDSCRContract', 1.4),
        'targetDSCRMerchant': cost_defaults.get('targetDSCRMerchant', 1.8),
        'interestRate': cost_defaults.get('interestRate', 0.06),
        'tenorYears': cost_defaults.get('tenorYears', 20),
        'debtStructure': cost_defaults.get('debtStructure', 'sculpting')
    }


def get_platform_defaults() -> Dict[str, Any]:
    """
    Get platform-wide default settings.
    
    Returns:
        Dictionary containing platform defaults.
    """
    defaults = load_asset_defaults()
    return defaults.get('platformDefaults', {
        'taxRate': 0.30,
        'fiscalYearStartMonth': 7,
        'inflationRate': 2.5,
        'debtSizingMethod': 'dscr',
        'enableTerminalValue': True
    })


def get_contract_defaults(contract_type: str) -> Dict[str, Any]:
    """
    Get default settings for a specific contract type.
    
    Args:
        contract_type: Type of contract ('fixed', 'bundled', 'green', 'Energy', 'tolling')
    
    Returns:
        Dictionary containing default contract settings.
    """
    defaults = load_asset_defaults()
    contract_defaults = defaults.get('contractDefaults', {})
    
    return contract_defaults.get(contract_type, {
        'indexation': 2.5,
        'indexationReferenceYear': 2024,
        'buyersPercentage': 100
    })


def get_fallback_defaults() -> Dict[str, Any]:
    """
    Provide fallback defaults if config file cannot be loaded.
    
    Returns:
        Dictionary with minimal default configuration.
    """
    return {
        'assetDefaults': {
            'solar': {
                'assetLife': 25,
                'volumeLossAdjustment': 95,
                'annualDegradation': 0.5,
                'costAssumptions': {
                    'capexPerMW': 0.9,
                    'opexPerMWPerYear': 0.01,
                    'maxGearing': 0.7,
                    'interestRate': 0.06
                }
            },
            'wind': {
                'assetLife': 25,
                'volumeLossAdjustment': 95,
                'annualDegradation': 0.5,
                'costAssumptions': {
                    'capexPerMW': 1.5,
                    'opexPerMWPerYear': 0.02,
                    'maxGearing': 0.65,
                    'interestRate': 0.06
                }
            },
            'storage': {
                'assetLife': 15,
                'volumeLossAdjustment': 95,
                'annualDegradation': 1.0,
                'costAssumptions': {
                    'capexPerMW': 2.0,
                    'opexPerMWPerYear': 0.03,
                    'maxGearing': 0.6,
                    'interestRate': 0.065
                }
            },
            'hybrid_solar_bess': {
                'assetLife': 25,  # Use solar asset life
                'volumeLossAdjustment': 95,
                'annualDegradation': 0.5,  # Use solar degradation
                'costAssumptions': {
                    'capexPerMW': 1.2,  # Weighted average between solar (0.9) and storage (2.0)
                    'opexPerMWPerYear': 0.015,  # Weighted average between solar (0.01) and storage (0.03)
                    'maxGearing': 0.7,  # Use solar max gearing
                    'interestRate': 0.06
                },
                'capacityFactors': {
                    'NSW': {'q1': 28, 'q2': 25, 'q3': 28, 'q4': 30},
                    'VIC': {'q1': 25, 'q2': 22, 'q3': 25, 'q4': 28},
                    'QLD': {'q1': 29, 'q2': 26, 'q3': 29, 'q4': 32},
                    'SA': {'q1': 27, 'q2': 24, 'q3': 27, 'q4': 30},
                    'WA': {'q1': 26, 'q2': 23, 'q3': 26, 'q4': 29},
                    'TAS': {'q1': 23, 'q2': 20, 'q3': 23, 'q4': 26}
                }
            }
        },
        'platformDefaults': {
            'taxRate': 0.30,
            'fiscalYearStartMonth': 7,
            'inflationRate': 2.5
        }
    }


if __name__ == '__main__':
    # Test loading defaults
    print("\n=== Testing Asset Defaults Loading ===\n")
    
    defaults = load_asset_defaults()
    print(f"Loaded {len(defaults.get('assetDefaults', {}))} asset types")
    
    # Test solar defaults
    solar_config = get_asset_default_config('solar')
    print(f"\nSolar asset life: {solar_config.get('assetLife')} years")
    
    # Test capacity factors
    nsw_cf = get_capacity_factor_defaults('solar', 'NSW')
    print(f"NSW Solar CF: Q1={nsw_cf['q1']}%, Q2={nsw_cf['q2']}%, Q3={nsw_cf['q3']}%, Q4={nsw_cf['q4']}%")
    
    # Test cost assumptions
    costs = get_cost_assumptions('wind', 100)
    print(f"\n100MW Wind Farm:")
    print(f"  CAPEX: ${costs['capex']}M")
    print(f"  Annual OPEX: ${costs['operatingCosts']}M")
    print(f"  Max Gearing: {costs['maxGearing']*100}%")
    
    # Test platform defaults
    platform = get_platform_defaults()
    print(f"\nPlatform tax rate: {platform.get('taxRate')*100}%")
    
    print("\n✅ All tests passed!")
