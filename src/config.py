# src/config.py

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

# Grace Period for Debt Repayment
# If operations start mid-period, this determines if the first payment is delayed or pro-rated.
# 'none': No grace period, payment starts immediately (pro-rated if partial period).
# 'prorate': Immediate prorated payment (default) - payment starts immediately, prorated for partial period.
# 'full_period': Payment starts after the first full period of operations.
DEFAULT_DEBT_GRACE_PERIOD = 'prorate'

# User-defined Model Period (Optional)
# If set, these dates will override the dynamic calculation from asset data.
# Format: 'YYYY-MM-DD'
USER_MODEL_START_DATE = None # e.g., '2023-01-01'
USER_MODEL_END_DATE = None   # e.g., '2045-12-31'

ENABLE_TERMINAL_VALUE = True # Enable or disable terminal value calculation

# MongoDB Collections
MONGO_ASSET_OUTPUT_COLLECTION = 'ASSET_cash_flows'
MONGO_ASSET_INPUTS_SUMMARY_COLLECTION = 'ASSET_inputs_summary'

MONGO_PRICE_SERIES_COLLECTION = 'PRICE_series'
MONGO_PNL_COLLECTION = '3WAY_P&L'
MONGO_CASH_FLOW_STATEMENT_COLLECTION = '3WAY_CASH'
MONGO_BALANCE_SHEET_COLLECTION = '3WAY_BS'

# Sensitivity Analysis Collections
MONGO_SENSITIVITY_COLLECTION = 'SENS_Asset_Outputs'
MONGO_SENSITIVITY_PNL_COLLECTION = 'SENS_3WAY_P&L'
MONGO_SENSITIVITY_CASH_COLLECTION = 'SENS_3WAY_CASH'
MONGO_SENSITIVITY_BS_COLLECTION = 'SENS_3WAY_BS'

# New Summary Collection
MONGO_ASSET_OUTPUT_SUMMARY_COLLECTION = 'ASSET_Output_Summary'

# Debt Sizing Options
# 'dscr': Debt is sized based on Debt Service Coverage Ratio (DSCR).
# 'annuity': Debt is sized based on a fixed annuity payment (traditional approach).
DEFAULT_DEBT_SIZING_METHOD = 'dscr'
DSCR_CALCULATION_FREQUENCY = 'quarterly' # 'monthly' or 'quarterly'

# Merchant Price Escalation Settings
MERCHANT_PRICE_ESCALATION_RATE = 0.025  # 2.5% annual escalation
MERCHANT_PRICE_ESCALATION_REFERENCE_DATE = '2025-01-01'  # Reference date for escalation calculation

# Tax Settings
TAX_RATE = 0.00  # 00% tax rate

# Distribution Settings
MIN_CASH_BALANCE_FOR_DISTRIBUTION = 2.0 # $M

# Depreciation Settings
DEFAULT_ASSET_LIFE_YEARS = 20  # Default asset life for depreciation in years