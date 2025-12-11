# MongoDB Asset Defaults Integration

This document describes the MongoDB integration for asset defaults, which replaces the file-based system with a MongoDB-backed configuration.

## Overview

Asset defaults are now stored in MongoDB in the `CONFIG_Asset_Defaults` collection instead of the `config/asset_defaults.json` file. This provides:
- Centralized configuration management
- Real-time updates across all services
- Better version control and audit trail
- Easier deployment and scaling

## Initialization

### First-Time Setup

Before using the MongoDB-based defaults, you need to initialize the database with your current defaults:

```bash
# From the backend-renew directory
python scripts/initialize_asset_defaults_mongo.py
```

This script will:
1. Load defaults from `config/asset_defaults.json`
2. Insert them into MongoDB `CONFIG_Asset_Defaults` collection
3. Add metadata (version, lastUpdated, etc.)

### Overwriting Existing Defaults

If defaults already exist in MongoDB and you want to overwrite them:

```bash
python scripts/initialize_asset_defaults_mongo.py --force
```

Or answer "yes" when prompted.

## Architecture

### Frontend (Next.js)

**File**: `app/api/asset-defaults/route.js`

- **GET**: Reads from MongoDB `CONFIG_Asset_Defaults` collection
- **POST**: Writes/updates MongoDB `CONFIG_Asset_Defaults` collection
- Falls back gracefully if MongoDB is unavailable (returns 404/500 with helpful messages)

### Backend (Python)

**File**: `src/core/asset_defaults.py`

- `load_asset_defaults()`: Reads from MongoDB first, falls back to JSON file if MongoDB unavailable
- All other functions (`get_asset_default_config()`, `get_platform_defaults()`, etc.) work unchanged

## MongoDB Collection Structure

The `CONFIG_Asset_Defaults` collection contains a single document with this structure:

```json
{
  "_id": ObjectId("..."),
  "assetDefaults": {
    "solar": { ... },
    "wind": { ... },
    "storage": { ... }
  },
  "contractDefaults": { ... },
  "platformDefaults": { ... },
  "validationRules": { ... },
  "metadata": {
    "version": "1.0.0",
    "lastUpdated": "2024-12-03T...",
    "initializedFrom": "asset_defaults.json"
  }
}
```

## Fallback Behavior

### Frontend
- If MongoDB is unavailable, the API returns a 404/500 error
- The Settings page will display an error message
- Users should ensure MongoDB is running and accessible

### Backend
- If MongoDB is unavailable or document doesn't exist:
  1. Falls back to reading `config/asset_defaults.json`
  2. If JSON file is also unavailable, uses hardcoded fallback defaults

This ensures the system continues to work even if MongoDB is temporarily unavailable.

## Migration Notes

- The JSON file (`config/asset_defaults.json`) is still used as a fallback
- You can keep the JSON file for backup/reference
- Future updates should be made through the Settings page (which writes to MongoDB)
- The JSON file can be updated manually if needed, but changes won't be reflected until MongoDB is re-initialized

## Testing

1. **Initialize MongoDB**:
   ```bash
   python scripts/initialize_asset_defaults_mongo.py
   ```

2. **Test Frontend**:
   - Navigate to Settings > Asset Defaults
   - Verify defaults load correctly
   - Make a change and save
   - Refresh page and verify change persists

3. **Test Backend**:
   ```bash
   python src/core/asset_defaults.py
   ```
   This will test loading defaults from MongoDB

## Troubleshooting

### "Asset defaults not found in MongoDB"
- Run the initialization script: `python scripts/initialize_asset_defaults_mongo.py`

### "MongoDB connection failed"
- Check your `.env.local` file has `MONGODB_URI` and `MONGODB_DB` set
- Verify MongoDB is running and accessible
- Check network connectivity

### Backend falls back to JSON file
- This is normal if MongoDB is unavailable
- Check MongoDB connection settings in `.env.local`
- Verify the `CONFIG_Asset_Defaults` collection exists and has a document





