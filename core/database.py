import os
from dotenv import load_dotenv
from pymongo import MongoClient
import pandas as pd

# Load environment variables from .env.local
load_dotenv(dotenv_path='.env.local')

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