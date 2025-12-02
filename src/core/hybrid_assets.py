# src/core/hybrid_assets.py

import pandas as pd
from typing import Dict, List, Optional


def get_hybrid_groups(assets: List[Dict]) -> Dict[str, List[int]]:
    """
    Identifies hybrid asset groups from asset data.
    
    Assets with the same 'hybridGroup' field are considered part of the same hybrid project.
    
    Args:
        assets: List of asset dictionaries
        
    Returns:
        Dictionary mapping hybrid group names to lists of asset IDs
    """
    hybrid_groups = {}
    
    for asset in assets:
        asset_id = asset.get('id')
        hybrid_group = asset.get('hybridGroup')
        
        if hybrid_group:
            if hybrid_group not in hybrid_groups:
                hybrid_groups[hybrid_group] = []
            hybrid_groups[hybrid_group].append(asset_id)
    
    # Only return groups with more than one asset
    return {group: asset_ids for group, asset_ids in hybrid_groups.items() if len(asset_ids) > 1}


def combine_hybrid_cashflows(
    cashflow_df: pd.DataFrame,
    hybrid_group_name: str,
    asset_ids: List[int],
    assets: List[Dict]
) -> pd.DataFrame:
    """
    Combines cashflows for multiple assets in a hybrid group into a single cashflow.
    
    Args:
        cashflow_df: DataFrame with cashflow data (must have 'asset_id' and 'date' columns)
        hybrid_group_name: Name of the hybrid group (e.g., "Solar River")
        asset_ids: List of asset IDs to combine
        assets: List of asset dictionaries (for getting asset names)
        
    Returns:
        DataFrame with combined cashflows, using the first asset_id as the identifier
    """
    if not asset_ids:
        return pd.DataFrame()
    
    # Filter cashflows for assets in this hybrid group
    hybrid_mask = cashflow_df['asset_id'].isin(asset_ids)
    hybrid_cashflows = cashflow_df[hybrid_mask].copy()
    
    if hybrid_cashflows.empty:
        return pd.DataFrame()
    
    # Get the primary asset ID (first one) and name
    primary_asset_id = asset_ids[0]
    
    # Get asset names for the hybrid group
    asset_names = []
    for asset in assets:
        if asset.get('id') in asset_ids:
            asset_names.append(asset.get('name', f'Asset_{asset.get("id")}'))
    
    # Combine name
    combined_name = f"{hybrid_group_name} (Hybrid)"
    
    # Group by date and sum all numerical columns
    date_col = 'date'
    numeric_cols = hybrid_cashflows.select_dtypes(include=['number']).columns.tolist()
    
    # Exclude asset_id from numeric columns if it exists
    if 'asset_id' in numeric_cols:
        numeric_cols.remove('asset_id')
    
    # Group by date and sum
    combined = hybrid_cashflows.groupby(date_col)[numeric_cols].sum().reset_index()
    
    # Set the primary asset_id
    combined['asset_id'] = primary_asset_id
    
    # Add metadata columns if they exist in original
    if 'period_type' in hybrid_cashflows.columns:
        # For period_type, take the first non-empty value per date
        period_type_map = hybrid_cashflows.groupby(date_col)['period_type'].first().to_dict()
        combined['period_type'] = combined[date_col].map(period_type_map).fillna('')
    
    # Add hybrid group metadata
    combined['hybrid_group'] = hybrid_group_name
    combined['component_asset_ids'] = str(asset_ids)
    combined['component_asset_names'] = ' + '.join(asset_names)
    
    return combined


def add_hybrid_asset_summaries(
    final_cash_flow: pd.DataFrame,
    assets: List[Dict],
    asset_irrs: Dict[int, float]
) -> pd.DataFrame:
    """
    Adds combined cashflow rows for hybrid asset groups to the cashflow DataFrame.
    
    Args:
        final_cash_flow: Main cashflow DataFrame
        assets: List of asset dictionaries
        asset_irrs: Dictionary of asset IRRs by asset_id
        
    Returns:
        DataFrame with hybrid asset combinations added
    """
    hybrid_groups = get_hybrid_groups(assets)
    
    if not hybrid_groups:
        return final_cash_flow
    
    print(f"\n=== PROCESSING HYBRID ASSETS ===")
    print(f"Found {len(hybrid_groups)} hybrid asset groups")
    
    hybrid_cashflows = []
    
    for group_name, asset_ids in hybrid_groups.items():
        print(f"  Combining {group_name}: assets {asset_ids}")
        
        combined_cf = combine_hybrid_cashflows(
            final_cash_flow,
            group_name,
            asset_ids,
            assets
        )
        
        if not combined_cf.empty:
            hybrid_cashflows.append(combined_cf)
            
            # Calculate combined IRR for the hybrid group
            # Filter for construction and operations periods
            if 'period_type' in combined_cf.columns:
                co_periods = combined_cf[combined_cf['period_type'].isin(['C', 'O'])].copy()
            else:
                co_periods = combined_cf.copy()
            
            if 'equity_cash_flow_pre_distributions' in co_periods.columns:
                equity_cf = co_periods[co_periods['equity_cash_flow_pre_distributions'] != 0].copy()
                
                if not equity_cf.empty:
                    from src.core.equity_irr import calculate_equity_irr
                    equity_irr_summary = equity_cf.groupby('date')['equity_cash_flow_pre_distributions'].sum().reset_index()
                    equity_irr_summary = equity_irr_summary.rename(columns={'equity_cash_flow_pre_distributions': 'equity_cash_flow'})
                    hybrid_irr = calculate_equity_irr(equity_irr_summary)
                    
                    if not pd.isna(hybrid_irr):
                        primary_asset_id = asset_ids[0]
                        asset_irrs[primary_asset_id] = hybrid_irr
                        print(f"    Combined IRR: {hybrid_irr:.2%}")
    
    if hybrid_cashflows:
        # Concatenate hybrid cashflows with original
        all_cashflows = pd.concat([final_cash_flow] + hybrid_cashflows, ignore_index=True)
        print(f"Added {len(hybrid_cashflows)} hybrid asset combinations")
    else:
        all_cashflows = final_cash_flow
    
    print("=== HYBRID ASSETS PROCESSING COMPLETE ===\n")
    
    return all_cashflows

