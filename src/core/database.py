# src/core/database.py

import os
from dotenv import load_dotenv
from pymongo import MongoClient
import pandas as pd
from contextlib import contextmanager
import threading
from typing import Optional, Dict, Any, List

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

class DatabaseManager:
    """
    Singleton MongoDB connection manager that maintains a single connection
    throughout the application lifecycle and provides thread-safe operations.
    """
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(DatabaseManager, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        self._client: Optional[MongoClient] = None
        self._db = None
        self._connection_lock = threading.Lock()
        self._initialized = True
    
    def connect(self):
        """Establish MongoDB connection if not already connected."""
        if self._client is None:
            with self._connection_lock:
                if self._client is None:
                    if not MONGO_URI:
                        raise ValueError("MONGODB_URI not found in environment variables. Please set it in .env.local")
                    
                    try:
                        self._client = MongoClient(MONGO_URI)
                        # Test connection
                        self._client.admin.command('ping')
                        self._db = self._client[MONGO_DB_NAME]
                        print("MongoDB connection established!")
                    except Exception as e:
                        print(f"MongoDB connection error: {e}")
                        raise
    
    def disconnect(self):
        """Close MongoDB connection."""
        if self._client is not None:
            with self._connection_lock:
                if self._client is not None:
                    self._client.close()
                    self._client = None
                    self._db = None
                    print("MongoDB connection closed.")
    
    def get_client(self):
        """Get the MongoDB client, connecting if necessary."""
        if self._client is None:
            self.connect()
        return self._client
    
    def get_database(self):
        """Get the database handle, connecting if necessary."""
        if self._db is None:
            self.connect()
        return self._db
    
    def get_collection(self, collection_name: str):
        """Get a collection handle."""
        return self.get_database()[collection_name]
    
    def is_connected(self) -> bool:
        """Check if currently connected to MongoDB."""
        return self._client is not None

# Global database manager instance
db_manager = DatabaseManager()

@contextmanager
def mongo_session():
    """
    Context manager for MongoDB operations.
    Ensures connection is established and optionally handles cleanup.
    """
    db_manager.connect()
    try:
        yield db_manager
    finally:
        # Don't automatically disconnect - let the application manage lifecycle
        pass

def get_mongo_client():
    """
    Legacy function for backward compatibility.
    Returns the managed client instead of creating new connections.
    """
    return db_manager.get_client()

def ensure_connection():
    """Ensure MongoDB connection is established."""
    db_manager.connect()

def close_connection():
    """Close the MongoDB connection."""
    db_manager.disconnect()

def insert_dataframe_to_mongodb(df: pd.DataFrame, collection_name: str, scenario_id: str = None, replace_scenario: bool = False):
    """
    Optimized version that uses the persistent connection.
    """
    if df.empty:
        print(f"DataFrame for collection '{collection_name}' is empty. No data inserted.")
        return

    try:
        with mongo_session() as db_mgr:
            collection = db_mgr.get_collection(collection_name)

            # If replace_scenario is True and scenario_id is provided, delete existing records first
            if replace_scenario and scenario_id:
                existing_count = collection.count_documents({"scenario_id": scenario_id})
                if existing_count > 0:
                    print(f"Replacing {existing_count} existing records for scenario '{scenario_id}' in '{collection_name}'")
                    delete_result = collection.delete_many({"scenario_id": scenario_id})
                    print(f"Deleted {delete_result.deleted_count} existing records")

            # Convert DataFrame to records
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
        raise

def replace_scenario_data(collection_name: str, scenario_id: str, df: pd.DataFrame):
    """
    Convenience function to replace all data for a specific scenario.
    """
    insert_dataframe_to_mongodb(df, collection_name, scenario_id, replace_scenario=True)

def clear_all_scenario_data(scenario_id: str, collections: list = None):
    """
    Optimized version that uses the persistent connection.
    """
    if collections is None:
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
    
    total_deleted = 0
    
    try:
        with mongo_session() as db_mgr:
            print(f"Clearing scenario '{scenario_id}' from {len(collections)} collections...")
            
            for collection_name in collections:
                collection = db_mgr.get_collection(collection_name)
                
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
        raise

def get_data_from_mongodb(collection_name: str, query: dict = None) -> List[Dict[Any, Any]]:
    """
    Optimized version that uses the persistent connection.
    """
    try:
        with mongo_session() as db_mgr:
            collection = db_mgr.get_collection(collection_name)
            
            if query:
                data = list(collection.find(query))
            else:
                data = list(collection.find({}))
            
            print(f"Successfully retrieved {len(data)} documents from '{collection_name}' collection.")
            return data
            
    except Exception as e:
        print(f"Error retrieving data from MongoDB collection '{collection_name}': {e}")
        return []

def clear_base_case_data(collections: list = None):
    """
    Optimized version that uses the persistent connection.
    """
    if collections is None:
        collections = [
            'ASSET_cash_flows',
            'ASSET_inputs_summary',
            '3WAY_P&L',
            '3WAY_CASH',
            '3WAY_BS'
        ]
    
    total_deleted = 0
    
    try:
        with mongo_session() as db_mgr:
            print(f"Clearing base case data from {len(collections)} collections...")
            
            for collection_name in collections:
                collection = db_mgr.get_collection(collection_name)
                
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
        raise

# Application lifecycle management functions
def initialize_database():
    """Initialize database connection at application start."""
    print("Initializing database connection...")
    ensure_connection()

def cleanup_database():
    """Clean up database connection at application end."""
    print("Cleaning up database connection...")
    close_connection()

# Context manager for application lifecycle
@contextmanager
def database_lifecycle():
    """
    Context manager for managing database connection across entire application lifecycle.
    Use this in your main application entry point.
    """
    try:
        initialize_database()
        yield
    finally:
        cleanup_database()
