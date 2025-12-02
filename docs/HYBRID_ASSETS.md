# Hybrid Assets Documentation

## Overview

The system now supports **hybrid assets** - projects that combine multiple asset types (e.g., solar + battery storage) but are effectively one project. This feature allows you to combine cashflows from multiple assets while maintaining their individual calculations.

## How It Works

1. **Individual Calculations**: Each component asset (e.g., "Solar River (Solar)" and "Solar River (BESS)") is still calculated separately with their own cashflows, revenue, CAPEX, etc.

2. **Automatic Combination**: Assets with the same `hybridGroup` field are automatically combined into a single cashflow for reporting and analysis.

3. **Combined Reporting**: The combined hybrid asset appears in summaries, exports, and IRR calculations as a single entity.

## Usage

### Step 1: Add `hybridGroup` Field to Asset Data

In your asset JSON file, add a `hybridGroup` field to assets that should be combined:

```json
{
  "assets": {
    "2": {
      "id": "2",
      "name": "Solar River (Solar)",
      "type": "solar",
      "hybridGroup": "Solar River",  // <-- Add this field
      ...
    },
    "3": {
      "id": "3",
      "name": "Solar River (BESS)",
      "type": "storage",
      "hybridGroup": "Solar River",  // <-- Same group name
      ...
    }
  }
}
```

### Step 2: Run the Model

The model will automatically:
- Calculate individual cashflows for each component asset
- Create a combined cashflow for the hybrid group
- Calculate a combined IRR for the hybrid project
- Include the hybrid asset in summaries and exports

### Step 3: Access Combined Data

#### Backend (Python)
The combined cashflows are automatically added to the `final_cash_flow` DataFrame with:
- `hybrid_group`: The group name
- `component_asset_ids`: List of component asset IDs
- `component_asset_names`: Names of component assets

#### Frontend (API)
Use the hybrid assets API endpoint:
```
GET /api/hybrid-assets?hybrid_group=Solar River&period=yearly
```

Or query the regular asset endpoint - hybrid assets will appear with their combined name:
```
GET /api/output-asset-data
```

## Example: Solar River

For the "Solar River" project with both solar and BESS components:

**Before (Separate Assets):**
- Asset 2: "Solar River (Solar)" (232 MW solar)
- Asset 3: "Solar River (BESS)" (256 MW storage)

**After (Hybrid Asset):**
- Combined: "Solar River (Hybrid)" 
  - Combines cashflows from both assets
  - Single IRR calculation
  - Combined CAPEX, revenue, OPEX, etc.
- Individual components:
  - "Solar River (Solar)" - 232 MW solar
  - "Solar River (BESS)" - 256 MW storage

## Features

- ✅ Individual asset calculations preserved
- ✅ Automatic cashflow combination
- ✅ Combined IRR calculation
- ✅ Hybrid assets in summaries and exports
- ✅ API support for hybrid asset queries
- ✅ Works with all time periods (monthly, quarterly, yearly, fiscal)

## Notes

- The **first asset ID** in a hybrid group becomes the primary ID for the combined asset
- Component assets are still available individually in the database
- Hybrid groups must have **2 or more assets** to be processed
- The combined asset name format is: `"{hybridGroup} (Hybrid)"`
- Individual component assets should be named: `"{hybridGroup} ({ComponentType})"` (e.g., "Solar River (Solar)", "Solar River (BESS)")

## API Endpoints

### Get Hybrid Asset Data
```
GET /api/hybrid-assets?hybrid_group={groupName}&period={period}
```

Parameters:
- `hybrid_group` (required): Name of the hybrid group
- `period` (optional): Time period (monthly, quarterly, yearly, fiscal_yearly)

### List All Assets (includes hybrid groups)
```
GET /api/output-asset-data
```

Returns:
- `uniqueAssetIds`: All assets including hybrid combinations
- `hybridGroups`: Mapping of hybrid group names to component assets

