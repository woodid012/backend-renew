import unittest
import pandas as pd
import numpy as np
from datetime import datetime
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.calculations.cashflow import aggregate_cashflows

class TestQualifyingCFADS(unittest.TestCase):
    def setUp(self):
        # Create dummy data
        dates = pd.date_range(start='2024-01-01', periods=12, freq='MS')
        self.revenue = pd.DataFrame({
            'asset_id': [1] * 12,
            'date': dates,
            'revenue': [100] * 12
        })
        self.opex = pd.DataFrame({
            'asset_id': [1] * 12,
            'date': dates,
            'opex': [20] * 12
        })
        self.capex = pd.DataFrame({
            'asset_id': [1] * 12,
            'date': dates,
            'capex': [0] * 12,
            'equity_capex': [0] * 12
        })
        self.debt_schedule = pd.DataFrame({
            'asset_id': [1] * 12,
            'date': dates,
            'interest': [5] * 12,
            'principal': [0] * 12,
            'ending_balance': [1000] * 12,
            'drawdowns': [0] * 12
        })
        # Set principal payment only on quarter ends
        for i in [2, 5, 8, 11]:
            self.debt_schedule.loc[i, 'principal'] = 50
            
        self.d_and_a = pd.DataFrame({
            'asset_id': [1] * 12,
            'date': dates,
            'd_and_a': [10] * 12
        })
        self.assets_data = [{'id': 1, 'name': 'Test Asset', 'OperatingStartDate': '2024-01-01'}]
        self.asset_cost_assumptions = {}
        self.end_date = datetime(2024, 12, 31)

    def test_quarterly_qualifying_cfads(self):
        # Run with quarterly frequency
        result = aggregate_cashflows(
            self.revenue, self.opex, self.capex, self.debt_schedule, 
            self.d_and_a, self.end_date, self.assets_data, 
            self.asset_cost_assumptions, repayment_frequency='quarterly'
        )
        
        # Check CFADS (Revenue - Opex = 100 - 20 = 80)
        self.assertTrue(all(result['cfads'] == 80))
        
        # Check Qualifying CFADS
        # Jan: 80
        # Feb: 80 + 80 = 160
        # Mar: 80 + 80 + 80 = 240
        expected_qualifying = [80, 160, 240] * 4
        np.testing.assert_array_equal(result['qualifying_cfads'].values, expected_qualifying)
        
        # Check DSCR at quarter end (March)
        # Qualifying CFADS = 240
        # Debt Service = Interest (5) + Principal (50) = 55
        # DSCR = 240 / 55 = 4.36
        mar_row = result[result['date'] == '2024-03-01'].iloc[0]
        self.assertAlmostEqual(mar_row['dscr'], 240/55, places=2)

    def test_monthly_qualifying_cfads(self):
        # Run with monthly frequency
        result = aggregate_cashflows(
            self.revenue, self.opex, self.capex, self.debt_schedule, 
            self.d_and_a, self.end_date, self.assets_data, 
            self.asset_cost_assumptions, repayment_frequency='monthly'
        )
        
        # Check Qualifying CFADS = CFADS
        np.testing.assert_array_equal(result['qualifying_cfads'].values, result['cfads'].values)

if __name__ == '__main__':
    unittest.main()
