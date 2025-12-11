# Asset Defaults Configuration System

## Overview
A comprehensive system for managing default asset configurations through a centralized JSON file and user-friendly Settings interface.

## Files Created

### Backend (c:\Projects\backend-renew)

1. **config/asset_defaults.json**
   - Central configuration file containing all defaults
   - Includes settings for solar, wind, and storage assets
   - Regional capacity factors for all Australian states
   - Cost assumptions (CAPEX, OPEX, financing terms)
   - Contract type defaults
   - Platform-wide settings
   - Validation rules

2. **src/core/asset_defaults.py**
   - Python utility module for loading defaults
   - Helper functions:
     - `load_asset_defaults()` - Load entire config
     - `get_asset_default_config(asset_type)` - Get specific asset type defaults
     - `get_capacity_factor_defaults(asset_type, region)` - Get regional capacity factors
     - `get_cost_assumptions(asset_type, capacity_mw)` - Calculate costs for specific capacity
     - `get_platform_defaults()` - Get platform-wide settings
     - `get_contract_defaults(contract_type)` - Get contract type defaults
     - `get_fallback_defaults()` - Fallback if config file unavailable

### Frontend (c:\Projects\renew-front)

1. **app/pages/settings/page.jsx** (Updated)
   - Modern card-based layout
   - Links to Asset Defaults and legacy Calculation Inputs

2. **app/pages/settings/asset-defaults/page.jsx** (New)
   - Comprehensive interface for managing defaults
   - Tabbed interface for Solar / Wind / Storage / Platform
   - Edit general settings, cost assumptions, and regional capacity factors
   - Real-time change tracking
   - Visual feedback for unsaved changes

3. **app/api/asset-defaults/route.js** (New)
   - GET: Read defaults from config file
   - POST: Save updated defaults to config file
   - Validates structure before saving
   - Updates metadata (lastUpdated timestamp)

## Configuration Structure

```json
{
  "assetDefaults": {
    "solar": { ... },
    "wind": { ... },
    "storage": { ... }
  },
  "contractDefaults": {
    "fixed": { ... },
    "bundled": { ... },
    "green": { ... },
    "Energy": { ... },
    "tolling": { ... }
  },
  "platformDefaults": { ... },
  "validationRules": { ... },
  "metadata": { ... }
}
```

## Asset Type Configuration

Each asset type includes:
- **General Settings**: Asset life, degradation, loss adjustments, construction duration
- **Cost Assumptions**: CAPEX/OPEX per MW, financing terms, terminal value
- **Regional Capacity Factors**: Quarterly capacity factors for NSW, VIC, QLD, SA, WA, TAS

### Storage-Specific Settings
- Duration hours
- Round-trip efficiency
- Higher degradation rate (1.0% vs 0.5%)
- Shorter asset life (15 years vs 25)

## How to Use

### Frontend
1. Navigate to **Settings** → **Asset Defaults**
2. Select asset type tab (Solar / Wind / Storage / Platform)
3. Edit values in the form
4. Click **Save Changes** to persist to backend

### Backend (Python)
```python
from src.core.asset_defaults import (
    get_asset_default_config,
    get_capacity_factor_defaults,
    get_cost_assumptions
)

# Get all solar defaults
solar_config = get_asset_default_config('solar')

# Get NSW capacity factors for solar
nsw_cf = get_capacity_factor_defaults('solar', 'NSW')
# Returns: {'q1': 28, 'q2': 25, 'q3': 27, 'q4': 30}

# Get cost assumptions for 100MW wind farm
costs = get_cost_assumptions('wind', 100)
# Returns: {'capex': 150.0, 'operatingCosts': 2.0, 'maxGearing': 0.65, ...}
```

## Integration with Asset Creation

When creating new assets in your frontend:

```javascript
// Fetch defaults for the user's selected type and region
const response = await fetch('/api/asset-defaults');
const defaults = await response.json();

// Apply to new asset
const newAsset = {
  type: 'solar',
  region: 'NSW',
  assetLife: defaults.assetDefaults.solar.assetLife,
  qtrCapacityFactor_q1: defaults.assetDefaults.solar.capacityFactors.NSW.q1,
  // ... etc
};
```

## Benefits

✅ **Centralized Configuration** - Single source of truth for all default values
✅ **User-Friendly Interface** - No code changes needed to update defaults
✅ **Regional Customization** - State-specific capacity factors
✅ **Type-Specific Defaults** - Optimized for solar, wind, and storage
✅ **Contract Type Support** - Defaults for all contract types from refactored contracts.py
✅ **Python Integration** - Easy access from backend calculations
✅ **Version Tracking** - Metadata includes version and last updated timestamp
✅ **Validation Rules** - Min/max bounds for all configurable values
✅ **Fallback Safety** - Graceful degradation if config file unavailable

## Access URLs

- **Settings**: http://localhost:3000/pages/settings
- **Asset Defaults**: http://localhost:3000/pages/settings/asset-defaults
- **API Endpoint**: http://localhost:3000/api/asset-defaults

## Next Steps

1. Test the settings page in your browser
2. Update existing asset creation forms to use these defaults
3. Consider adding import/export functionality for bulk updates
4. Add user roles/permissions if multiple people manage settings
