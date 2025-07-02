import os
from dotenv import load_dotenv
from pymongo import MongoClient

# Load environment variables
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
env_path = os.path.join(project_root, '.env.local')
load_dotenv(dotenv_path=env_path)

MONGO_URI = os.getenv('MONGODB_URI')
MONGO_DB_NAME = os.getenv('MONGODB_DB')

print(f"MONGO_URI: {MONGO_URI}")
print(f"MONGO_DB_NAME: {MONGO_DB_NAME}")

COLLECTIONS_TO_DELETE = [
    "price_curve_monthly",
    "price_curve_quarterly",
    "price_curve_calendar_year",
    "price_curve_fiscal_year"
]

def delete_collections():
    """
    Connects to MongoDB and deletes the specified collections.
    """
    client = None
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB_NAME]
        
        for collection_name in COLLECTIONS_TO_DELETE:
            if collection_name in db.list_collection_names():
                db.drop_collection(collection_name)
                print(f"Successfully dropped collection: '{collection_name}'")
            else:
                print(f"Collection '{collection_name}' not found.")
            
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if client:
            client.close()

if __name__ == "__main__":
    delete_collections()