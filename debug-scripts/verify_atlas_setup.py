# verify-atlas-setup.py
# Quick verification script for MongoDB Atlas setup

import os
import sys
from dotenv import load_dotenv
from pymongo import MongoClient

# Load environment
project_root = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(project_root, '.env.local')
load_dotenv(dotenv_path=env_path)

MONGO_URI = os.getenv('MONGODB_URI')
MONGO_DB_NAME = os.getenv('MONGODB_DB')

print("=== MongoDB Atlas Setup Verification ===")
print(f"Target Database: {MONGO_DB_NAME}")
print(f"Cluster: cluster0.quuwlhb.mongodb.net")
print(f"User: ProjectHalo")

def verify_atlas_setup():
    client = None
    try:
        # Connect to MongoDB Atlas
        print("\n1. Testing connection to Atlas...")
        client = MongoClient(MONGO_URI)
        
        # Test connection
        client.admin.command('ping')
        print("   ✓ Successfully connected to MongoDB Atlas")
        
        # List all databases accessible to this user
        print("\n2. Checking accessible databases...")
        db_list = client.list_database_names()
        print(f"   Available databases: {db_list}")
        
        if MONGO_DB_NAME in db_list:
            print(f"   ✓ Target database '{MONGO_DB_NAME}' exists and is accessible")
        else:
            print(f"   ⚠️  Target database '{MONGO_DB_NAME}' not found in accessible databases")
            print(f"   This is normal for new databases - it will be created on first write")
        
        # Test access to target database
        print(f"\n3. Testing access to '{MONGO_DB_NAME}' database...")
        db = client[MONGO_DB_NAME]
        
        # List collections (will be empty for new database)
        collections = db.list_collection_names()
        print(f"   Current collections: {collections}")
        
        # Test write permissions
        print("\n4. Testing write permissions...")
        test_collection = db['setup_test']
        
        # Insert test document
        test_doc = {
            "test": "setup_verification",
            "database": MONGO_DB_NAME,
            "timestamp": "2025-01-03"
        }
        
        result = test_collection.insert_one(test_doc)
        print(f"   ✓ Successfully inserted test document")
        print(f"   Document ID: {result.inserted_id}")
        
        # Read test document
        found_doc = test_collection.find_one({"_id": result.inserted_id})
        if found_doc:
            print(f"   ✓ Successfully read test document")
        
        # Clean up test document
        delete_result = test_collection.delete_one({"_id": result.inserted_id})
        if delete_result.deleted_count == 1:
            print(f"   ✓ Successfully deleted test document")
        
        print(f"\n5. Testing collections that your app will use...")
        expected_collections = [
            'ASSET_cash_flows',
            'ASSET_inputs_summary',
            'SENS_Asset_Outputs'
        ]
        
        for collection_name in expected_collections:
            try:
                collection = db[collection_name]
                count = collection.count_documents({})
                print(f"   ✓ {collection_name}: accessible ({count} documents)")
            except Exception as e:
                print(f"   ✗ {collection_name}: error - {e}")
        
        # Show cluster information
        print(f"\n6. Cluster information...")
        try:
            # Get server status (limited info available to non-admin users)
            status = client.admin.command("hello")
            print(f"   Cluster type: {status.get('msg', 'Unknown')}")
            print(f"   Server version: {status.get('version', 'Unknown')}")
        except Exception as e:
            print(f"   Could not get cluster info: {e}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Verification failed: {e}")
        
        # Common error scenarios
        if "authentication failed" in str(e).lower():
            print("\n🔍 Possible issues:")
            print("   - Wrong username/password")
            print("   - User doesn't have access to this database")
            print("   - User might not be created in Atlas")
        elif "network" in str(e).lower() or "timeout" in str(e).lower():
            print("\n🔍 Possible issues:")
            print("   - Network connectivity problems")
            print("   - Cluster might be paused")
            print("   - IP address not whitelisted")
        elif "database" in str(e).lower():
            print("\n🔍 Possible issues:")
            print("   - Database name might be incorrect")
            print("   - User doesn't have permissions for this database")
        
        return False
        
    finally:
        if client:
            client.close()

def check_user_permissions():
    """Additional check for user permissions in Atlas"""
    print(f"\n=== User Permission Checklist ===")
    print(f"Please verify in MongoDB Atlas GUI:")
    print(f"1. Go to Database Access → Users")
    print(f"2. Find user 'ProjectHalo'")
    print(f"3. Check that user has:")
    print(f"   - 'readWrite' role on '{MONGO_DB_NAME}' database")
    print(f"   - OR 'readWriteAnyDatabase' role")
    print(f"   - OR custom role with read/write permissions")
    print(f"4. If user doesn't exist or lacks permissions:")
    print(f"   - Edit user permissions")
    print(f"   - Add database-specific role: readWrite on {MONGO_DB_NAME}")

if __name__ == "__main__":
    print("Starting MongoDB Atlas verification...\n")
    
    if not MONGO_URI or not MONGO_DB_NAME:
        print("❌ Missing environment variables!")
        print("Make sure .env.local contains MONGODB_URI and MONGODB_DB")
        sys.exit(1)
    
    success = verify_atlas_setup()
    
    if success:
        print("\n🎉 MongoDB Atlas setup verification PASSED!")
        print("Your configuration should work with your Python application.")
    else:
        print("\n❌ MongoDB Atlas setup verification FAILED!")
        check_user_permissions()
        print("\nAfter fixing permissions, run this script again to verify.")
    
    print(f"\nNext steps:")
    print(f"1. If verification passed, try running: python src/main.py")
    print(f"2. Check data was written with: python scripts/check_db_size.py")