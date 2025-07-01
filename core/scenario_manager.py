import json
import os
import pandas as pd

def apply_scenario_overrides(
    assets: list,
    asset_cost_assumptions: dict,
    monthly_prices: pd.DataFrame,
    yearly_spreads: pd.DataFrame,
    scenario_overrides: dict
):
    """
    Applies scenario overrides to asset data, cost assumptions, and price data.

    Args:
        assets (list): List of asset dictionaries.
        asset_cost_assumptions (dict): Dictionary of asset cost assumptions.
        monthly_prices (pd.DataFrame): DataFrame of monthly prices.
        yearly_spreads (pd.DataFrame): DataFrame of yearly spreads.
        scenario_overrides (dict): Dictionary of overrides from the scenario file.

    Returns:
        tuple: (modified_assets, modified_asset_cost_assumptions, modified_monthly_prices, modified_yearly_spreads)
    """
    modified_assets = [asset.copy() for asset in assets]
    modified_asset_cost_assumptions = {k: v.copy() for k, v in asset_cost_assumptions.items()}
    modified_monthly_prices = monthly_prices.copy()
    modified_yearly_spreads = yearly_spreads.copy()

    overrides = scenario_overrides.get('overrides', {})

    # Apply global multipliers/adjustments
    global_volume_multiplier = overrides.get('global_volume_multiplier', 1.0)
    global_capex_multiplier = overrides.get('global_capex_multiplier', 1.0)
    global_opex_multiplier = overrides.get('global_opex_multiplier', 1.0)
    global_electricity_price_adjustment_per_mwh = overrides.get('global_electricity_price_adjustment_per_mwh', 0.0)
    global_green_price_adjustment_per_mwh = overrides.get('global_green_price_adjustment_per_mwh', 0.0)
    global_debt_interest_rate_adjustment_bps = overrides.get('global_debt_interest_rate_adjustment_bps', 0)
    global_terminal_value_multiplier = overrides.get('global_terminal_value_multiplier', 1.0)

    # Apply volume multiplier to assets (e.g., to 'capacity_mw' or 'production_mwh')
    for asset in modified_assets:
        if 'capacity_mw' in asset:
            asset['capacity_mw'] *= global_volume_multiplier
        # Add other volume-related fields as needed

    # Apply CAPEX and OPEX multipliers to asset_cost_assumptions
    for asset_name, costs in modified_asset_cost_assumptions.items():
        if 'initial_capex_per_mw' in costs:
            costs['initial_capex_per_mw'] *= global_capex_multiplier
        if 'opex_per_mwh' in costs:
            costs['opex_per_mwh'] *= global_opex_multiplier
        if 'terminalValue' in costs:
            costs['terminalValue'] *= global_terminal_value_multiplier

    # Apply price adjustments to monthly_prices
    # Assuming 'price' column exists and represents electricity/green price
    if 'price' in modified_monthly_prices.columns:
        # This is a simplified application. You might need more complex logic
        # if different price types (e.g., electricity vs. green) are in the same column
        modified_monthly_prices['price'] += global_electricity_price_adjustment_per_mwh
        # If there's a separate 'green_price' column or similar, apply green_price_adjustment_per_mwh there

    # Apply interest rate adjustment to assets (assuming a 'debt_interest_rate' field exists)
    for asset in modified_assets:
        if 'debt_interest_rate' in asset:
            asset['debt_interest_rate'] += (global_debt_interest_rate_adjustment_bps / 10000) # Convert bps to decimal

    return modified_assets, modified_asset_cost_assumptions, modified_monthly_prices, modified_yearly_spreads

def load_scenario(scenario_file_path: str) -> dict:
    """
    Loads a scenario definition from a JSON file.
    """
    if not os.path.exists(scenario_file_path):
        raise FileNotFoundError(f"Scenario file not found: {scenario_file_path}")
    with open(scenario_file_path, 'r') as f:
        scenario_data = json.load(f)
    return scenario_data
