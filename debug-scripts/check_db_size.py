
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

def get_collection_sizes():
    """
    Connects to MongoDB and prints the size of each collection.
    """
    client = None
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB_NAME]
        
        collections = db.list_collection_names()
        
        print(f"--- Sizes for collections in database: '{MONGO_DB_NAME}' ---")
        
        stats = []
        for collection_name in collections:
            # The scale factor of 1024*1024 converts the size from bytes to megabytes
            collection_stats = db.command('collStats', collection_name, scale=1024*1024)
            stats.append({
                'Collection': collection_name,
                'Size (MB)': round(collection_stats['size'], 2)
            })
        
        # Sort by size descending
        stats.sort(key=lambda x: x['Size (MB)'], reverse=True)
        
        for stat in stats:
            print(f"- {stat['Collection']}: {stat['Size (MB)']} MB")
            
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if client:
            client.close()

if __name__ == "__main__":
    get_collection_sizes()
