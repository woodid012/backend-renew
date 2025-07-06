
import sys
import os
import json
from pymongo import MongoClient

# Add the parent directory to the Python path for module imports
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.insert(0, project_root)

from src.core.database import get_mongo_client, MONGO_DB_NAME

def upload_config_inputs(file_path, collection_name):
    """
    Uploads a JSON file to a specified MongoDB collection, clearing existing data.
    """
    client = None
    try:
        client = get_mongo_client()
        db = client[MONGO_DB_NAME]
        collection = db[collection_name]

        # Clear existing data
        print(f"Clearing existing data from collection: {collection_name}")
        collection.delete_many({})
        print("Collection cleared.")

        # Load JSON data
        with open(file_path, 'r') as f:
            data = json.load(f)

        # Insert new data
        if isinstance(data, list):
            collection.insert_many(data)
            print(f"Successfully inserted {len(data)} documents.")
        else:
            collection.insert_one(data)
            print("Successfully inserted 1 document.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if client:
            client.close()
            print("MongoDB connection closed.")

if __name__ == "__main__":
    file_to_upload = os.path.join(project_root, 'data', 'processed_inputs', 'ZEBRE_Inputs.json')
    target_collection = "CONFIG_Inputs"
    upload_config_inputs(file_to_upload, target_collection)
