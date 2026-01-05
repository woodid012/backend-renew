# backend-renew/src/core/auditable_revenue.py
"""
AuditableRevenueModule: Wrapper around existing revenue calculation functions.
Adds Glassbox-style transparency WITHOUT changing any calculation logic.

Usage:
    from core.auditable_revenue import AuditableRevenueModule
    
    # Create module and run
    revenue_module = AuditableRevenueModule(assets, monthly_prices, yearly_spreads, start_date, end_date)
    revenue_df = revenue_module.run()
    
    # Get audit data
    audit_data = revenue_module.to_audit_dict()
"""

from datetime import datetime
from typing import List, Any, Dict
import pandas as pd

from core.auditable_module import AuditableModule
from calculations.revenue import (
    calculate_revenue_timeseries,
    calculate_renewables_revenue,
    calculate_storage_revenue
)


class AuditableRevenueModule(AuditableModule):
    """
    Wraps the revenue calculation functions with audit logging.
    Does NOT change any calculation logic - just captures inputs/outputs.
    """
    
    def __init__(self, assets: List[Dict], monthly_prices: pd.DataFrame, 
                 yearly_spreads: pd.DataFrame, start_date: datetime, end_date: datetime):
        super().__init__(
            name="RevenueModule",
            description="Calculates contracted and merchant revenue for all assets"
        )
        
        # Store references for calculation
        self._assets = assets
        self._monthly_prices = monthly_prices
        self._yearly_spreads = yearly_spreads
        self._start_date = start_date
        self._end_date = end_date
        
        # Register inputs for audit trail
        self.register_input("asset_count", len(assets), "Number of assets")
        self.register_input("asset_ids", [a.get('id', a.get('name', 'unknown')) for a in assets], "Asset identifiers")
        self.register_input("asset_types", list(set(a.get('type', 'unknown') for a in assets)), "Asset types in portfolio")
        self.register_input("start_date", start_date, "Analysis start date")
        self.register_input("end_date", end_date, "Analysis end date")
        self.register_input("monthly_prices_shape", monthly_prices.shape if hasattr(monthly_prices, 'shape') else None, "Price data dimensions")
        self.register_input("yearly_spreads_shape", yearly_spreads.shape if hasattr(yearly_spreads, 'shape') else None, "Spread data dimensions")
        
        # Capture contract summary
        total_contracts = sum(len(a.get('contracts', []) or []) for a in assets)
        self.register_input("total_contracts", total_contracts, "Total contracts across all assets")
    
    def run(self) -> pd.DataFrame:
        """
        Execute the underlying revenue calculation and capture outputs.
        Returns the exact same DataFrame as the original function.
        """
        # Call the UNCHANGED original function
        revenue_df = calculate_revenue_timeseries(
            self._assets,
            self._monthly_prices,
            self._yearly_spreads,
            self._start_date,
            self._end_date
        )
        
        # Capture outputs for audit trail
        self._capture_outputs(revenue_df)
        
        # Return the original result unchanged
        return revenue_df
    
    def _capture_outputs(self, revenue_df: pd.DataFrame):
        """Capture summary statistics from the revenue calculation."""
        if revenue_df.empty:
            self.set_output("total_revenue", 0, "Total revenue ($M)")
            self.set_output("row_count", 0, "Number of data rows")
            return
        
        # Summary statistics
        self.set_output("total_revenue", float(revenue_df['revenue'].sum()), "Total revenue ($M)")
        self.set_output("row_count", len(revenue_df), "Number of data rows")
        
        # Revenue breakdown
        if 'contractedGreenRevenue' in revenue_df.columns:
            self.set_output("contracted_green_total", float(revenue_df['contractedGreenRevenue'].sum()), "Contracted green revenue ($M)")
        if 'contractedEnergyRevenue' in revenue_df.columns:
            self.set_output("contracted_energy_total", float(revenue_df['contractedEnergyRevenue'].sum()), "Contracted energy revenue ($M)")
        if 'merchantGreenRevenue' in revenue_df.columns:
            self.set_output("merchant_green_total", float(revenue_df['merchantGreenRevenue'].sum()), "Merchant green revenue ($M)")
        if 'merchantEnergyRevenue' in revenue_df.columns:
            self.set_output("merchant_energy_total", float(revenue_df['merchantEnergyRevenue'].sum()), "Merchant energy revenue ($M)")
        
        # Per-asset summary
        if 'asset_id' in revenue_df.columns:
            asset_totals = revenue_df.groupby('asset_id')['revenue'].sum().to_dict()
            self.set_output("revenue_by_asset", asset_totals, "Revenue by asset ($M)")
        
        # Time range covered
        if 'date' in revenue_df.columns:
            self.set_output("date_range", {
                "min": revenue_df['date'].min().isoformat() if hasattr(revenue_df['date'].min(), 'isoformat') else str(revenue_df['date'].min()),
                "max": revenue_df['date'].max().isoformat() if hasattr(revenue_df['date'].max(), 'isoformat') else str(revenue_df['date'].max())
            }, "Data date range")
        
        # Generation summary
        if 'monthlyGeneration' in revenue_df.columns:
            self.set_output("total_generation_mwh", float(revenue_df['monthlyGeneration'].sum()), "Total generation (MWh)")


# Convenience function to wrap existing code with minimal changes
def calculate_revenue_with_audit(assets, monthly_prices, yearly_spreads, start_date, end_date):
    """
    Drop-in replacement for calculate_revenue_timeseries that includes audit data.
    
    Returns:
        tuple: (revenue_df, audit_dict)
    """
    module = AuditableRevenueModule(assets, monthly_prices, yearly_spreads, start_date, end_date)
    revenue_df = module.run()
    audit_dict = module.to_audit_dict()
    return revenue_df, audit_dict
