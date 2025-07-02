# src/core/database.py

import os
from dotenv import load_dotenv
from pymongo import MongoClient
import pandas as pd

# Find and load environment variables from .env.local
# Look for .env.local starting from current file location up to project root
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))  # Go up two levels from src/core/
env_path = os.path.join(project_root, '.env.local')

if os.path.exists(env_path):
    load_dotenv(dotenv_path=env_path)
    print(f"Loaded environment variables from: {env_path}")
else:
    # Try alternative locations
    alternative_paths = [
        os.path.join(os.getcwd(), '.env.local'),  # Current working directory
        os.path.join(os.path.dirname(os.getcwd()), '.env.local'),  # Parent of working directory
    ]
    
    env_loaded = False
    for alt_path in alternative_paths:
        if os.path.exists(alt_path):
            load_dotenv(dotenv_path=alt_path)
            print(f"Loaded environment variables from: {alt_path}")
            env_loaded = True
            break
    
    if not env_loaded:
        print(f"Warning: .env.local not found. Searched locations:")
        print(f"  - {env_path}")
        for alt_path in alternative_paths:
            print(f"  - {alt_path}")

MONGO_URI = os.getenv('MONGODB_URI')
MONGO_DB_NAME = os.getenv('MONGODB_DB')

def get_mongo_client():
    """
    Establishes and returns a MongoDB client connection.
    """
    if not MONGO_URI:
        raise ValueError("MONGODB_URI not found in environment variables. Please set it in .env.local")
    try:
        client = MongoClient(MONGO_URI)
        # The ping command is cheap and does not require auth. 
        # It confirms that the client can connect to the deployment.
        client.admin.command('ping')
        print("MongoDB connection successful!")
        return client
    except Exception as e:
        print(f"MongoDB connection error: {e}")
        raise

def insert_dataframe_to_mongodb(df: pd.DataFrame, collection_name: str, scenario_id: str = None):
    """
    Inserts a pandas DataFrame into a specified MongoDB collection.
    Each row in the DataFrame becomes a document in the collection.

    Args:
        df (pd.DataFrame): The DataFrame to insert.
        collection_name (str): The name of the MongoDB collection.
        scenario_id (str, optional): A unique identifier for the scenario run.
    """
    if df.empty:
        print(f"DataFrame for collection '{collection_name}' is empty. No data inserted.")
        return

    client = None
    try:
        client = get_mongo_client()
        db = client[MONGO_DB_NAME]
        collection = db[collection_name]

        # Convert DataFrame to a list of dictionaries (JSON-like objects)
        # Ensure datetime objects are handled correctly for MongoDB
        
        # Convert 'quarter' column to string if it exists, for MongoDB compatibility
        if 'quarter' in df.columns:
            df['quarter'] = df['quarter'].astype(str)

        records = df.to_dict(orient='records')
        
        # Add scenario_id to each document if provided
        if scenario_id:
            for record in records:
                record['scenario_id'] = scenario_id
        
        # Insert records
        result = collection.insert_many(records)
        print(f"Successfully inserted {len(result.inserted_ids)} documents into '{collection_name}' collection.")
    except Exception as e:
        print(f"Error inserting data into MongoDB collection '{collection_name}': {e}")
    finally:
        if client:
            client.close()
            print("MongoDB connection closed.")