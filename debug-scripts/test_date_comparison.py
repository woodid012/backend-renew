# debug-scripts/test_date_comparison.py
"""
Test date comparison logic between pandas Timestamp and datetime objects
"""

import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

print("Testing Date Comparison Logic")
print("="*80)

# Simulate what happens in revenue.py
operating_start_date_str = '2026-07-01'
asset_start_date = datetime.strptime(operating_start_date_str, '%Y-%m-%d')

# Simulate date_range creation
start_date = datetime(2025, 1, 1)  # Model start
end_date = datetime(2030, 12, 31)  # Model end
date_range = pd.date_range(start=start_date, end=end_date, freq='MS')

print(f"OperatingStartDate (datetime): {asset_start_date}")
print(f"Date range type: {type(date_range[0])}")
print()

# Test dates around the start date
test_dates = [
    ('2026-06-01', 'June 2026 (before start)'),
    ('2026-07-01', 'July 2026 (start date)'),
    ('2026-07-15', 'July 15, 2026 (mid-month)'),
    ('2026-08-01', 'August 2026 (after start)'),
]

print("Date Comparison Tests:")
print("-"*80)
for date_str, description in test_dates:
    test_date = pd.Timestamp(date_str)
    comparison_result = test_date >= asset_start_date
    
    print(f"{description}:")
    print(f"  test_date: {test_date} (type: {type(test_date)})")
    print(f"  asset_start_date: {asset_start_date} (type: {type(asset_start_date)})")
    print(f"  test_date >= asset_start_date: {comparison_result}")
    print(f"  Should calculate revenue: {comparison_result}")
    print()

# Test with actual date_range values
print("Testing with actual date_range values:")
print("-"*80)
for current_date in date_range:
    if current_date.year == 2026 and current_date.month in [6, 7, 8]:
        comparison_result = current_date >= asset_start_date
        should_calc = comparison_result and current_date < (asset_start_date + relativedelta(years=25))
        print(f"  {current_date.strftime('%Y-%m-%d')}: >= start_date? {comparison_result}, "
              f"Should calc revenue? {should_calc}")

print()
print("="*80)
print("Conclusion:")
print("If OperatingStartDate is 2026-07-01:")
print("  - 2026-06-01 should NOT calculate revenue (False)")
print("  - 2026-07-01 SHOULD calculate revenue (True)")
print("  - 2026-08-01 SHOULD calculate revenue (True)")


