# src/core/scenario_manager.py

import json
import os
import pandas as pd
import numpy as np

def load_scenario(scenario_file_path: str) -> dict:
    """
    Loads a scenario definition from a JSON file.
    """
    if not os.path.exists(scenario_file_path):
        raise FileNotFoundError(f"Scenario file not found: {scenario_file_path}")
    with open(scenario_file_path, 'r') as f:
        scenario_data = json.load(f)
    return scenario_data

def apply_all_scenarios_to_timeseries(
    revenue_df: pd.DataFrame,
    opex_df: pd.DataFrame, 
    capex_df: pd.DataFrame,
    assets: list,
    asset_cost_assumptions: dict,
    monthly_prices: pd.DataFrame,
    yearly_spreads: pd.DataFrame,
    start_date,
    end_date,
    scenario_overrides: dict
):
    """
    Apply ALL scenario overrides directly to calculated timeseries data.
    This centralizes all scenario logic in one place after timeseries calculation.
    
    FIXED: CAPEX scenarios are now applied BEFORE debt sizing so debt is sized correctly
    based on the sensitivity-adjusted CAPEX amounts.
    """
    
    # Make copies to avoid modifying originals
    modified_revenue_df = revenue_df.copy()
    modified_opex_df = opex_df.copy()
    modified_capex_df = capex_df.copy()  # CAPEX timeseries will be modified here for CAPEX scenarios
    modified_asset_cost_assumptions = {k: v.copy() for k, v in asset_cost_assumptions.items()}
    
    overrides = scenario_overrides.get('overrides', {})
    
    if not overrides:
        print("No scenario overrides to apply")
        return modified_revenue_df, modified_opex_df, modified_capex_df, modified_asset_cost_assumptions
    
    print(f"\n=== APPLYING PRE-DEBT-SIZING SCENARIO OVERRIDES ===")
    print(f"Available overrides: {list(overrides.keys())}")
    
    # Extract all scenario parameters
    global_volume_multiplier = overrides.get('global_volume_multiplier', 1.0)
    global_capex_multiplier = overrides.get('global_capex_multiplier', 1.0)  # Will be handled post-debt-sizing
    global_opex_multiplier = overrides.get('global_opex_multiplier', 1.0)
    global_electricity_price_adjustment_per_mwh = overrides.get('global_electricity_price_adjustment_per_mwh', 0.0)
    global_green_price_adjustment_per_mwh = overrides.get('global_green_price_adjustment_per_mwh', 0.0)
    global_debt_interest_rate_adjustment_bps = overrides.get('global_debt_interest_rate_adjustment_bps', 0)
    global_terminal_value_multiplier = overrides.get('global_terminal_value_multiplier', 1.0)
    
    changes_made = []
    
    # 1. VOLUME SCENARIOS - Multiply all volume-dependent revenue components
    if global_volume_multiplier != 1.0:
        print(f"  Applying volume multiplier: {global_volume_multiplier}")
        
        volume_fields = [
            'revenue', 'contractedGreenRevenue', 'contractedEnergyRevenue', 
            'merchantGreenRevenue', 'merchantEnergyRevenue', 'monthlyGeneration'
        ]
        
        for field in volume_fields:
            if field in modified_revenue_df.columns:
                old_total = modified_revenue_df[field].sum()
                modified_revenue_df[field] *= global_volume_multiplier
                new_total = modified_revenue_df[field].sum()
                print(f"    {field}: {old_total:.2f} -> {new_total:.2f}")
                changes_made.append(f"Volume: {field}")
    
    # 2. CAPEX SCENARIOS - Apply BEFORE debt sizing so debt is sized correctly
    if global_capex_multiplier != 1.0:
        print(f"  Applying CAPEX multiplier: {global_capex_multiplier} BEFORE debt sizing")
        
        # Apply multiplier to asset_cost_assumptions capex values
        # This ensures debt sizing uses the sensitivity-adjusted CAPEX
        capex_changes = 0
        for asset_name, costs in modified_asset_cost_assumptions.items():
            if 'capex' in costs and costs['capex'] is not None:
                old_capex = costs['capex']
                costs['capex'] = float(costs['capex']) * global_capex_multiplier
                capex_changes += 1
                print(f"    {asset_name}: capex ${old_capex:,.2f}M -> ${costs['capex']:,.2f}M")
        
        # Also apply multiplier to initial CAPEX timeseries
        if 'capex' in modified_capex_df.columns:
            old_total = modified_capex_df['capex'].sum()
            modified_capex_df['capex'] *= global_capex_multiplier
            new_total = modified_capex_df['capex'].sum()
            print(f"    CAPEX timeseries: ${old_total:,.2f}M -> ${new_total:,.2f}M")
            changes_made.append("CAPEX: asset_cost_assumptions and timeseries")
        
        if capex_changes == 0:
            print(f"    WARNING: No CAPEX fields found to modify in asset_cost_assumptions")
    
    # 3. OPEX SCENARIOS - Multiply OPEX
    if global_opex_multiplier != 1.0:
        print(f"  Applying OPEX multiplier: {global_opex_multiplier}")
        
        if 'opex' in modified_opex_df.columns:
            old_total = modified_opex_df['opex'].sum()
            modified_opex_df['opex'] *= global_opex_multiplier
            new_total = modified_opex_df['opex'].sum()
            print(f"    opex: {old_total:.2f} -> {new_total:.2f}")
            changes_made.append("OPEX: opex")
    
    # 4. PRICE SCENARIOS - Recalculate revenue with adjusted prices
    if global_electricity_price_adjustment_per_mwh != 0.0 or global_green_price_adjustment_per_mwh != 0.0:
        print(f"  Applying price adjustments:")
        print(f"    Electricity: {global_electricity_price_adjustment_per_mwh:.2f} $/MWh")
        print(f"    Green: {global_green_price_adjustment_per_mwh:.2f} $/MWh")
        
        # Apply price adjustments to revenue components based on generation volumes
        if global_electricity_price_adjustment_per_mwh != 0.0:
            # Adjust electricity-based revenue (both contracted and merchant energy)
            energy_fields = ['contractedEnergyRevenue', 'merchantEnergyRevenue']
            
            for field in energy_fields:
                if field in modified_revenue_df.columns and 'monthlyGeneration' in modified_revenue_df.columns:
                    # Calculate adjustment: $/MWh * MWh = $ (in millions since generation is in MWh)
                    price_adjustment = (modified_revenue_df['monthlyGeneration'] * 
                                      global_electricity_price_adjustment_per_mwh / 1_000_000)
                    
                    # Apply proportionally to contracted vs merchant based on existing split
                    total_energy_rev = (modified_revenue_df['contractedEnergyRevenue'] + 
                                       modified_revenue_df['merchantEnergyRevenue'])
                    
                    # Avoid division by zero
                    field_proportion = modified_revenue_df[field] / total_energy_rev.where(total_energy_rev != 0, 1)
                    field_adjustment = price_adjustment * field_proportion
                    
                    old_total = modified_revenue_df[field].sum()
                    modified_revenue_df[field] += field_adjustment
                    new_total = modified_revenue_df[field].sum()
                    
                    print(f"    {field}: {old_total:.2f} -> {new_total:.2f}")
                    changes_made.append(f"Energy Price: {field}")
        
        if global_green_price_adjustment_per_mwh != 0.0:
            # Adjust green-based revenue (both contracted and merchant green)
            green_fields = ['contractedGreenRevenue', 'merchantGreenRevenue']
            
            for field in green_fields:
                if field in modified_revenue_df.columns and 'monthlyGeneration' in modified_revenue_df.columns:
                    # Calculate adjustment: $/MWh * MWh = $ (in millions)
                    price_adjustment = (modified_revenue_df['monthlyGeneration'] * 
                                      global_green_price_adjustment_per_mwh / 1_000_000)
                    
                    # Apply proportionally to contracted vs merchant based on existing split
                    total_green_rev = (modified_revenue_df['contractedGreenRevenue'] + 
                                      modified_revenue_df['merchantGreenRevenue'])
                    
                    # Avoid division by zero
                    field_proportion = modified_revenue_df[field] / total_green_rev.where(total_green_rev != 0, 1)
                    field_adjustment = price_adjustment * field_proportion
                    
                    old_total = modified_revenue_df[field].sum()
                    modified_revenue_df[field] += field_adjustment
                    new_total = modified_revenue_df[field].sum()
                    
                    print(f"    {field}: {old_total:.2f} -> {new_total:.2f}")
                    changes_made.append(f"Green Price: {field}")
        
        # Recalculate total revenue
        revenue_components = ['contractedGreenRevenue', 'contractedEnergyRevenue', 
                             'merchantGreenRevenue', 'merchantEnergyRevenue']
        
        available_components = [c for c in revenue_components if c in modified_revenue_df.columns]
        if available_components:
            old_total_revenue = modified_revenue_df['revenue'].sum()
            modified_revenue_df['revenue'] = modified_revenue_df[available_components].sum(axis=1)
            new_total_revenue = modified_revenue_df['revenue'].sum()
            print(f"    total revenue: {old_total_revenue:.2f} -> {new_total_revenue:.2f}")
            changes_made.append("Price: total revenue")
    
    # 5. INTEREST RATE SCENARIOS - Modify cost assumptions for debt sizing
    if global_debt_interest_rate_adjustment_bps != 0:
        print(f"  Applying interest rate adjustment: {global_debt_interest_rate_adjustment_bps} bps")
        adjustment_decimal = global_debt_interest_rate_adjustment_bps / 10000
        
        rate_changes = 0
        for asset_name, costs in modified_asset_cost_assumptions.items():
            # Check multiple possible interest rate field names
            for rate_field in ['interestRate', 'interest_rate', 'debtInterestRate', 'debt_interest_rate']:
                if rate_field in costs and costs[rate_field] is not None:
                    old_value = costs[rate_field]
                    costs[rate_field] = float(costs[rate_field]) + adjustment_decimal
                    rate_changes += 1
                    print(f"    {asset_name}: {rate_field} {old_value:.4f} -> {costs[rate_field]:.4f}")
                    changes_made.append(f"Interest Rate: {asset_name}")
                    break
        
        if rate_changes == 0:
            print(f"    WARNING: No interest rate fields found to modify")
    
    # 6. TERMINAL VALUE SCENARIOS - Modify cost assumptions
    if global_terminal_value_multiplier != 1.0:
        print(f"  Applying terminal value multiplier: {global_terminal_value_multiplier}")
        
        tv_changes = 0
        for asset_name, costs in modified_asset_cost_assumptions.items():
            # Check multiple possible terminal value field names
            for tv_field in ['terminalValue', 'terminal_value', 'TerminalValue']:
                if tv_field in costs and costs[tv_field] is not None:
                    old_value = costs[tv_field]
                    costs[tv_field] = float(costs[tv_field]) * global_terminal_value_multiplier
                    tv_changes += 1
                    print(f"    {asset_name}: {tv_field} {old_value:.2f} -> {costs[tv_field]:.2f}")
                    changes_made.append(f"Terminal Value: {asset_name}")
                    break
        
        if tv_changes == 0:
            print(f"    WARNING: No terminal value fields found to modify")
    
    # Summary
    if changes_made:
        print(f"  Successfully applied {len(changes_made)} modifications:")
        for change in changes_made:
            print(f"    - {change}")
    else:
        print(f"  No modifications applied")
    
    # SAFEGUARD: Ensure revenue is zero before OperatingStartDate for each asset
    # This prevents scenarios from accidentally adding revenue before operations start
    print(f"  Applying safeguard: Zeroing revenue before OperatingStartDate...")
    from datetime import datetime
    import pandas as pd
    
    for asset in assets:
        asset_id = asset.get('id')
        if 'OperatingStartDate' not in asset or not asset['OperatingStartDate']:
            continue
        
        asset_start_date = pd.to_datetime(asset['OperatingStartDate'])
        
        # Zero out all revenue-related fields before OperatingStartDate
        mask = (modified_revenue_df['asset_id'] == asset_id) & (modified_revenue_df['date'] < asset_start_date)
        
        if mask.any():
            revenue_fields = ['revenue', 'contractedGreenRevenue', 'contractedEnergyRevenue', 
                            'merchantGreenRevenue', 'merchantEnergyRevenue', 'monthlyGeneration']
            
            for field in revenue_fields:
                if field in modified_revenue_df.columns:
                    count = mask.sum()
                    modified_revenue_df.loc[mask, field] = 0
            
            print(f"    Asset {asset_id}: Zeroed revenue for {mask.sum()} periods before {asset_start_date.strftime('%Y-%m-%d')}")
    
    print(f"=== PRE-DEBT-SIZING SCENARIO OVERRIDES COMPLETE ===\n")
    
    return modified_revenue_df, modified_opex_df, modified_capex_df, modified_asset_cost_assumptions

def apply_post_debt_sizing_capex_scenarios(
    final_capex_df: pd.DataFrame,
    scenario_overrides: dict
):
    """
    Apply CAPEX scenarios to the debt-sized CAPEX schedule.
    Called AFTER debt sizing to ensure CAPEX changes affect final cash flows.
    
    FIXED: Enhanced debugging to track the issue
    """
    
    overrides = scenario_overrides.get('overrides', {})
    global_capex_multiplier = overrides.get('global_capex_multiplier', 1.0)
    
    if global_capex_multiplier == 1.0:
        print(f"\n=== NO CAPEX SCENARIO TO APPLY ===")
        print(f"  CAPEX multiplier is 1.0, no changes needed")
        return final_capex_df
    
    print(f"\n=== APPLYING POST-DEBT-SIZING CAPEX SCENARIOS ===")
    print(f"  Input CAPEX schedule summary:")
    print(f"    Total rows: {len(final_capex_df)}")
    print(f"    Total CAPEX: ${final_capex_df['capex'].sum():,.2f}M")
    print(f"    Total Debt CAPEX: ${final_capex_df['debt_capex'].sum():,.2f}M")
    print(f"    Total Equity CAPEX: ${final_capex_df['equity_capex'].sum():,.2f}M")
    print(f"  Applying CAPEX multiplier: {global_capex_multiplier}")
    
    modified_capex_df = final_capex_df.copy()
    
    capex_fields = ['capex', 'equity_capex', 'debt_capex']
    
    for field in capex_fields:
        if field in modified_capex_df.columns:
            old_total = modified_capex_df[field].sum()
            modified_capex_df[field] *= global_capex_multiplier
            new_total = modified_capex_df[field].sum()
            print(f"    {field}: ${old_total:,.2f}M -> ${new_total:,.2f}M")
    
    print(f"  Output CAPEX schedule summary:")
    print(f"    Total CAPEX: ${modified_capex_df['capex'].sum():,.2f}M")
    print(f"    Total Debt CAPEX: ${modified_capex_df['debt_capex'].sum():,.2f}M")
    print(f"    Total Equity CAPEX: ${modified_capex_df['equity_capex'].sum():,.2f}M")
    
    print(f"=== POST-DEBT-SIZING CAPEX SCENARIOS COMPLETE ===\n")
    
    return modified_capex_df

# Legacy function for backward compatibility - can be removed later
def apply_scenario_overrides(assets, asset_cost_assumptions, monthly_prices, yearly_spreads, scenario_overrides):
    """
    Legacy function - scenarios are now applied to timeseries instead.
    This is kept for backward compatibility but does nothing.
    """
    print("WARNING: apply_scenario_overrides is deprecated. Use apply_all_scenarios_to_timeseries instead.")
    return assets, asset_cost_assumptions, monthly_prices, yearly_spreads