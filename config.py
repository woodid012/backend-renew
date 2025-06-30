# backend/config.py

# Date and Time Settings
DATE_FORMAT = '%Y-%m-%d'
OUTPUT_DATE_FORMAT = '%Y-%m-%dT%H:%MZ' # ISO 8601 format for JSON output

# CAPEX Funding Options
# 'equity_first': Equity is used first until exhausted, then debt.
# 'pari_passu': Equity and debt are drawn down proportionally to their gearing.
DEFAULT_CAPEX_FUNDING_TYPE = 'equity_first'

# Debt Repayment Options
# 'monthly': Debt is repaid monthly.
# 'quarterly': Debt is repaid quarterly.
DEFAULT_DEBT_REPAYMENT_FREQUENCY = 'quarterly'  # Changed from 'monthly' to 'quarterly'

# Grace Period for Debt Repayment (in months)
# If operations start mid-period, this determines if the first payment is delayed or pro-rated.
# 'none': No grace period, payment starts immediately (pro-rated if partial period).
# 'full_period': Payment starts after the first full period of operations.
DEFAULT_DEBT_GRACE_PERIOD = 'full_period'

# User-defined Model Period (Optional)
# If set, these dates will override the dynamic calculation from asset data.
# Format: 'YYYY-MM-DD'
USER_MODEL_START_DATE = None # e.g., '2023-01-01'
USER_MODEL_END_DATE = None   # e.g., '2045-12-31'

ENABLE_TERMINAL_VALUE = True # Enable or disable terminal value calculation

# Debt Sizing Options
# 'dscr': Debt is sized based on Debt Service Coverage Ratio (DSCR).
# 'annuity': Debt is sized based on a fixed annuity payment (traditional approach).
DEFAULT_DEBT_SIZING_METHOD = 'dscr'
DSCR_CALCULATION_FREQUENCY = 'quarterly' # 'monthly' or 'quarterly'

# Merchant Price Escalation Settings
MERCHANT_PRICE_ESCALATION_RATE = 0.025  # 2.5% annual escalation
MERCHANT_PRICE_ESCALATION_REFERENCE_DATE = '2025-01-01'  # Reference date for escalation calculation