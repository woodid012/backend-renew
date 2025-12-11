#!/usr/bin/env python3
"""
Initialize MongoDB CONFIG_assetDefaults collection with current defaults from JSON file.
This script should be run once to migrate from file-based to MongoDB-based defaults.
"""

import json
import os
import sys
from datetime import datetime

# Add parent directory to path to import from src
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.database import db_manager, mongo_session

def load_json_defaults():
    """Load defaults from the JSON file."""
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'config',
        'asset_defaults.json'
    )
    
    try:
        with open(config_path, 'r') as f:
            defaults = json.load(f)
        print(f"✅ Loaded defaults from {config_path}")
        return defaults
    except Exception as e:
        print(f"❌ Error loading JSON defaults: {e}")
        raise

def initialize_mongodb_defaults():
    """Initialize MongoDB with defaults from JSON file."""
    print("\n=== Initializing MongoDB CONFIG_assetDefaults ===\n")
    
    # Load defaults from JSON
    defaults = load_json_defaults()
    
    # Add metadata
    defaults['metadata'] = defaults.get('metadata', {})
    defaults['metadata']['lastUpdated'] = datetime.now().isoformat()
    defaults['metadata']['initializedFrom'] = 'asset_defaults.json'
    defaults['metadata']['version'] = defaults['metadata'].get('version', '1.0.0')
    
    try:
        with mongo_session() as db_mgr:
            collection = db_mgr.get_collection('CONFIG_assetDefaults')
            
            # Check if document already exists
            existing = collection.find_one({})
            
            if existing:
                print("⚠️  CONFIG_assetDefaults already exists in MongoDB")
                # Check for non-interactive mode (for automation)
                import sys
                if '--force' in sys.argv or '--overwrite' in sys.argv:
                    print("   Using --force flag, overwriting existing defaults...")
                else:
                    response = input("Do you want to overwrite it? (yes/no): ")
                    if response.lower() != 'yes':
                        print("❌ Aborted. Existing defaults preserved.")
                        return
                
                # Update existing document
                result = collection.update_one(
                    {},
                    {'$set': defaults}
                )
                print(f"✅ Updated existing CONFIG_assetDefaults document")
            else:
                # Insert new document
                result = collection.insert_one(defaults)
                print(f"✅ Inserted new CONFIG_assetDefaults document with ID: {result.inserted_id}")
            
            # Verify the document
            verify_doc = collection.find_one({})
            if verify_doc:
                print(f"\n✅ Verification successful!")
                print(f"   - Asset types: {list(verify_doc.get('assetDefaults', {}).keys())}")
                print(f"   - Platform defaults: {list(verify_doc.get('platformDefaults', {}).keys())}")
                print(f"   - Last updated: {verify_doc.get('metadata', {}).get('lastUpdated', 'N/A')}")
            else:
                print("⚠️  Warning: Could not verify inserted document")
                
    except Exception as e:
        print(f"❌ Error initializing MongoDB: {e}")
        raise

if __name__ == '__main__':
    try:
        initialize_mongodb_defaults()
        print("\n✅ Initialization complete!")
    except Exception as e:
        print(f"\n❌ Initialization failed: {e}")
        sys.exit(1)

