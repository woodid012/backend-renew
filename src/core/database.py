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

def insert_dataframe_to_mongodb(df: pd.DataFrame, collection_name: str, scenario_id: str = None, replace_scenario: bool = False):
    """
    Inserts a pandas DataFrame into a specified MongoDB collection.
    Each row in the DataFrame becomes a document in the collection.

    Args:
        df (pd.DataFrame): The DataFrame to insert.
        collection_name (str): The name of the MongoDB collection.
        scenario_id (str, optional): A unique identifier for the scenario run.
        replace_scenario (bool): If True and scenario_id is provided, deletes existing records for this scenario first.
    """
    if df.empty:
        print(f"DataFrame for collection '{collection_name}' is empty. No data inserted.")
        return

    client = None
    try:
        client = get_mongo_client()
        db = client[MONGO_DB_NAME]
        collection = db[collection_name]

        # If replace_scenario is True and scenario_id is provided, delete existing records first
        if replace_scenario and scenario_id:
            existing_count = collection.count_documents({"scenario_id": scenario_id})
            if existing_count > 0:
                print(f"Replacing {existing_count} existing records for scenario '{scenario_id}' in '{collection_name}'")
                delete_result = collection.delete_many({"scenario_id": scenario_id})
                print(f"Deleted {delete_result.deleted_count} existing records")

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
        action = "Replaced and inserted" if (replace_scenario and scenario_id) else "Inserted"
        print(f"Successfully {action.lower()} {len(result.inserted_ids)} documents into '{collection_name}' collection.")
    except Exception as e:
        print(f"Error inserting data into MongoDB collection '{collection_name}': {e}")
    finally:
        if client:
            client.close()
            print("MongoDB connection closed.")

def replace_scenario_data(collection_name: str, scenario_id: str, df: pd.DataFrame):
    """
    Convenience function to replace all data for a specific scenario.
    
    Args:
        collection_name (str): The name of the MongoDB collection.
        scenario_id (str): The scenario identifier to replace.
        df (pd.DataFrame): The new data to insert.
    """
    insert_dataframe_to_mongodb(df, collection_name, scenario_id, replace_scenario=True)

def clear_all_scenario_data(scenario_id: str, collections: list = None):
    """
    Clear all data for a specific scenario across multiple collections.
    
    Args:
        scenario_id (str): The scenario identifier to clear.
        collections (list, optional): List of collection names. If None, clears from common collections.
    """
    if collections is None:
        # Default collections that typically store scenario data
        collections = [
            'ASSET_cash_flows',
            'ASSET_inputs_summary',
            '3WAY_P&L',
            '3WAY_CASH',
            '3WAY_BS',
            'SENS_Asset_Outputs',
            'SENS_3WAY_P&L',
            'SENS_3WAY_CASH',
            'SENS_3WAY_BS'
        ]
    
    client = None
    total_deleted = 0
    
    try:
        client = get_mongo_client()
        db = client[MONGO_DB_NAME]
        
        print(f"Clearing scenario '{scenario_id}' from {len(collections)} collections...")
        
        for collection_name in collections:
            collection = db[collection_name]
            
            # Count existing records
            existing_count = collection.count_documents({"scenario_id": scenario_id})
            
            if existing_count > 0:
                # Delete records for this scenario
                delete_result = collection.delete_many({"scenario_id": scenario_id})
                print(f"  {collection_name}: Deleted {delete_result.deleted_count} records")
                total_deleted += delete_result.deleted_count
            else:
                print(f"  {collection_name}: No records found")
        
        print(f"Total records deleted: {total_deleted}")
        
    except Exception as e:
        print(f"Error clearing scenario data: {e}")
    finally:
        if client:
            client.close()

def get_data_from_mongodb(collection_name: str, query: dict = None):
    """
    Retrieves data from a specified MongoDB collection.

    Args:
        collection_name (str): The name of the MongoDB collection.
        query (dict, optional): A query to filter the results. Defaults to None.
    """
    client = None
    try:
        client = get_mongo_client()
        db = client[MONGO_DB_NAME]
        collection = db[collection_name]
        
        if query:
            data = list(collection.find(query))
        else:
            data = list(collection.find({}))
        
        print(f"Successfully retrieved {len(data)} documents from '{collection_name}' collection.")
        return data
    except Exception as e:
        print(f"Error retrieving data from MongoDB collection '{collection_name}': {e}")
        return []
    finally:
        if client:
            client.close()
            print("MongoDB connection closed.")

def clear_base_case_data(collections: list = None):
    """
    Clear base case data (records without scenario_id or with scenario_id = None).
    
    Args:
        collections (list, optional): List of collection names. If None, clears from common collections.
    """
    if collections is None:
        collections = [
            'ASSET_cash_flows',
            'ASSET_inputs_summary',
            '3WAY_P&L',
            '3WAY_CASH',
            '3WAY_BS'
        ]
    
    client = None
    total_deleted = 0
    
    try:
        client = get_mongo_client()
        db = client[MONGO_DB_NAME]
        
        print(f"Clearing base case data from {len(collections)} collections...")
        
        for collection_name in collections:
            collection = db[collection_name]
            
            # Query for records without scenario_id or with scenario_id = None
            query = {"$or": [{"scenario_id": {"$exists": False}}, {"scenario_id": None}]}
            existing_count = collection.count_documents(query)
            
            if existing_count > 0:
                delete_result = collection.delete_many(query)
                print(f"  {collection_name}: Deleted {delete_result.deleted_count} base case records")
                total_deleted += delete_result.deleted_count
            else:
                print(f"  {collection_name}: No base case records found")
        
        print(f"Total base case records deleted: {total_deleted}")
        
    except Exception as e:
        print(f"Error clearing base case data: {e}")
    finally:
        if client:
            client.close()