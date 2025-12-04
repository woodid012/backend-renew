
import os
import sys
from dotenv import load_dotenv
from pymongo import MongoClient
import pprint

# Add parent directory to path to import src
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

env_path = os.path.join(parent_dir, '.env.local')
load_dotenv(env_path)

MONGO_URI = os.getenv('MONGODB_URI')
MONGO_DB_NAME = os.getenv('MONGODB_DB')

def inspect_config_inputs():
    if not MONGO_URI or not MONGO_DB_NAME:
        print("Error: MONGODB_URI or MONGODB_DB not found in environment variables")
        return

    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB_NAME]
        collection = db['CONFIG_Inputs']
        
        print(f"Connected to {MONGO_DB_NAME}.CONFIG_Inputs")
        
        # Count documents
        count = collection.count_documents({})
        print(f"Total documents: {count}")
        
        # Get one document
        doc = collection.find_one()
        if doc:
            print("\nFirst document structure:")
            pprint.pprint(doc)
            
            print(f"\nKeys: {list(doc.keys())}")
            
            if 'PlatformName' in doc:
                print(f"PlatformName: {doc['PlatformName']}")
            else:
                print("No PlatformName found.")
                
            # Check unique portfolios if possible
            portfolios = collection.distinct('portfolio')
            print(f"\nDistinct portfolios: {portfolios}")
        else:
            print("Collection is empty.")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    inspect_config_inputs()
