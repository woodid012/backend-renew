
import os
import sys
from pymongo import MongoClient
from dotenv import load_dotenv

# Add parent directory to path to find .env if needed, though we load directly here
current_dir = os.path.dirname(os.path.abspath(__file__))
backend_dir = os.path.dirname(current_dir)
sys.path.append(backend_dir)

# Load environment variables
# Try to find .env.local in common locations
potential_paths = [
    r'c:\Projects\renew\.env.local',
    r'c:\Projects\renew\backend-renew\.env.local',
    os.path.join(os.getcwd(), '.env.local'),
    os.path.join(os.path.dirname(os.getcwd()), '.env.local')
]

env_path = None
for p in potential_paths:
    if os.path.exists(p):
        env_path = p
        break

if env_path:
    print(f"Loading environment from: {env_path}")
    load_dotenv(env_path)
else:
    print("Warning: Could not find .env.local")

MONGO_URI = os.getenv('MONGODB_URI')
DB_NAME = os.getenv('MONGODB_DB')

if not MONGO_URI or not DB_NAME:
    print("Error: MONGO_URI or DB_NAME not found in .env")
    sys.exit(1)

def migrate():
    try:
        print(f"Connecting to MongoDB: {DB_NAME}")
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection_name = 'PRICE_Curves_2'
        
        if collection_name not in db.list_collection_names():
            print(f"Collection {collection_name} does not exist. Nothing to migrate.")
            return

        collection = db[collection_name]
        
        # Count documents without curve_name
        query = {'curve_name': {'$exists': False}}
        count = collection.count_documents(query)
        
        print(f"Found {count} documents missing 'curve_name'.")
        
        if count > 0:
            print("Updating documents to set curve_name='Backend'...")
            result = collection.update_many(
                query,
                {'$set': {'curve_name': 'Backend'}}
            )
            print(f"Modified {result.modified_count} documents.")
        else:
            print("No documents needed updating.")
            
        # Verify
        remaining = collection.count_documents(query)
        print(f"Remaining documents without curve_name: {remaining}")
        
        # List distinct curve names
        distinct_curves = collection.distinct('curve_name')
        print(f"Distinct curve names now in collection: {distinct_curves}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    migrate()
