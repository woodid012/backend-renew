# IRR Calculation Assessment - Findings and Fixes

## Overview
Assessment of IRR calculation logic to identify and fix issues causing inflated IRR values.

## Assessment Date
December 2024

## Key Findings

### 1. Cash Flow Formula Review
**File**: `src/calculations/cashflow.py`

The equity cash flow formula is correct:
```python
equity_cash_flow_pre_distributions = CFADS - interest - principal - equity_capex - tax_expense
```

- **Equity CAPEX**: Correctly subtracted (negative cash flow for equity investors)
- **Terminal Value**: Correctly added to `equity_cash_flow_pre_distributions` at the terminal date
- **Formula Logic**: Mathematically sound

### 2. IRR Calculation Logic Review
**File**: `src/main.py`

The IRR calculation logic was mostly correct, but improvements were made:

**Issues Identified:**
1. **Filtering Logic**: The filter excluded periods with zero `equity_cash_flow_pre_distributions`, which could potentially exclude periods with equity_capex if other components exactly canceled out (unlikely but possible)
2. **Missing Validation**: No validation to ensure all equity contributions are properly included in IRR calculation
3. **Limited Debugging**: Insufficient output to diagnose IRR calculation issues

**Fixes Implemented:**
1. Enhanced filtering to explicitly include periods with `equity_capex > 0` to ensure all equity contributions are captured
2. Added validation checks to verify equity CAPEX is properly included in IRR calculation
3. Added debugging output showing equity invested vs. net cash flow for validation

### 3. XIRR Function Review
**File**: `src/core/equity_irr.py`

The XIRR calculation function is correct:
- Properly handles irregular dates
- Correctly groups cash flows by date
- Handles zero cash flows appropriately (preserves timing)
- Uses multiple solving methods for robustness

## Fixes Implemented

### Fix 1: Enhanced Period Filtering
**Location**: `src/main.py` lines 545-549 and 611-614

**Change**: Added explicit inclusion of periods with `equity_capex > 0` to ensure all equity contributions are captured, even if net cash flow is zero.

```python
# Before:
equity_irr_df = co_periods_df[
    (co_periods_df['equity_cash_flow_pre_distributions'] != 0) | 
    (co_periods_df['terminal_value'] > 0)
].copy()

# After:
equity_irr_df = co_periods_df[
    (co_periods_df['equity_cash_flow_pre_distributions'] != 0) | 
    (co_periods_df['terminal_value'] > 0) |
    (co_periods_df.get('equity_capex', 0) != 0)
].copy()
```

### Fix 2: Added Validation Checks
**Location**: `src/main.py` lines 551-560 and 616-625

**Change**: Added validation to verify equity contributions are properly included and warn if there are discrepancies.

```python
# Validation: Verify equity contributions are included
if 'equity_capex' in co_periods_df.columns:
    total_equity_capex = co_periods_df['equity_capex'].sum()
    equity_capex_in_irr = equity_irr_df['equity_capex'].sum() if 'equity_capex' in equity_irr_df.columns else 0
    if abs(total_equity_capex - equity_capex_in_irr) > 0.01:
        print(f"  ⚠️  WARNING: Equity CAPEX mismatch...")
```

### Fix 3: Enhanced Debugging Output
**Location**: `src/main.py` lines 561-565

**Change**: Added output showing equity invested and net cash flow for validation.

```python
if 'equity_capex' in equity_irr_df.columns:
    total_equity_invested = equity_irr_df['equity_capex'].sum()
    total_equity_cf = equity_irr_summary['equity_cash_flow'].sum()
    print(f"  Equity invested (CAPEX): ${total_equity_invested:,.2f}M")
    print(f"  Net equity cash flow: ${total_equity_cf:,.2f}M")
```

### Fix 4: Created Diagnostic Script
**Location**: `debug-scripts/irr_cashflow_diagnostic.py`

**Purpose**: New diagnostic tool to analyze IRR calculations and identify issues.

**Features**:
- Extracts actual cash flows used in IRR calculation
- Shows equity contributions vs. returns
- Calculates NPV at various discount rates to validate IRR
- Identifies potential issues (missing equity contributions, sign changes, etc.)

## Potential Root Causes of High IRRs

Based on the assessment, potential causes of inflated IRRs include:

1. **Missing Equity Contributions**: If equity_capex periods are excluded from IRR calculation
   - **Mitigation**: Enhanced filtering now explicitly includes equity_capex periods

2. **Terminal Value Issues**: If terminal value is incorrectly calculated or double-counted
   - **Status**: Terminal value logic appears correct (added once at terminal date)

3. **Timing Issues**: If equity contributions are timed incorrectly relative to returns
   - **Status**: Timing appears correct based on construction/operations period filtering

4. **Cash Flow Aggregation**: If cash flows are incorrectly aggregated across assets or dates
   - **Status**: Grouping logic appears correct (groups by date, sums cash flows)

## Recommendations

1. **Run Diagnostic Script**: Use the new `irr_cashflow_diagnostic.py` script to analyze specific assets with high IRRs
   ```bash
   python debug-scripts/irr_cashflow_diagnostic.py --scenario <scenario_id> --asset <asset_id>
   ```

2. **Validate Equity Contributions**: Check that total equity_capex matches expected values for each asset

3. **Review Terminal Values**: Verify terminal value amounts and timing are correct

4. **Compare with Expected Values**: If you have expected IRR ranges, compare calculated IRRs and investigate significant deviations

5. **Check Input Data**: Verify that revenue, opex, and other inputs are correct, as these affect CFADS and ultimately IRR

## Testing

To validate the fixes:

1. Run the model with the updated code
2. Check for any validation warnings in the output
3. Use the diagnostic script to analyze IRR calculations
4. Compare IRR values before and after fixes (if you have baseline data)

## Files Modified

1. `src/main.py` - Enhanced IRR calculation logic with validation
2. `src/calculations/cashflow.py` - Added comments for clarity
3. `debug-scripts/irr_cashflow_diagnostic.py` - New diagnostic tool

## Next Steps

1. Run the model and review validation output
2. Use diagnostic script to analyze specific assets
3. If IRRs are still high, investigate:
   - Input data (revenue, opex, terminal values)
   - Debt sizing and gearing assumptions
   - Tax calculations
   - Terminal value calculations


