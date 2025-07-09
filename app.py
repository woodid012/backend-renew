# app.py

from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import sys
from collections import defaultdict
from bson import json_util
from dotenv import load_dotenv
import json

load_dotenv()

MONGO_DB_NAME = os.getenv('MONGODB_DB')

# Add src directory to path for imports (since we're in root, src is a subdirectory)
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, 'src')
sys.path.insert(0, src_dir)
sys.path.insert(0, current_dir)

# Initialize with None - will be set after successful imports
get_data_from_mongodb = None
get_mongo_client = None
run_cashflow_model = None
MONGO_ASSET_OUTPUT_COLLECTION = 'ASSET_cash_flows'

try:
    from src.main import run_cashflow_model
    print("✅ Successfully imported run_cashflow_model")
except ImportError as e:
    print(f"⚠️ Could not import run_cashflow_model: {e}")
    run_cashflow_model = None

try:
    from src.core.database import get_data_from_mongodb, get_mongo_client
    print("✅ Successfully imported database functions")
except ImportError as e:
    print(f"⚠️ Could not import database functions: {e}")
    get_data_from_mongodb = None
    get_mongo_client = None

try:
    from src.config import MONGO_ASSET_OUTPUT_COLLECTION, MONGO_ASSET_INPUTS_SUMMARY_COLLECTION
    print(f"✅ Successfully imported config: {MONGO_ASSET_OUTPUT_COLLECTION}")
except ImportError as e:
    print(f"⚠️ Could not import config: {e}")
    MONGO_ASSET_OUTPUT_COLLECTION = 'ASSET_cash_flows'
    MONGO_ASSET_INPUTS_SUMMARY_COLLECTION = 'ASSET_inputs_summary'

app = Flask(__name__)

# Enable CORS for all domains and all routes
CORS(app, origins=["*"])

@app.route('/', methods=['GET'])
def health_check():
    return jsonify({
        "status": "healthy", 
        "message": "Renewable Finance Backend API",
        "platform": "Render",
        "imports": {
            "run_cashflow_model": run_cashflow_model is not None,
            "get_data_from_mongodb": get_data_from_mongodb is not None,
            "get_mongo_client": get_mongo_client is not None
        },
        "mongo_db": MONGO_DB_NAME,
        "collection": MONGO_ASSET_OUTPUT_COLLECTION
    })

@app.route('/api/run-model', methods=['POST'])
def run_model():
    if run_cashflow_model is None:
        return jsonify({
            "status": "error",
            "message": "Model functionality not available - import failed"
        }), 500
    
    try:
        data = request.get_json() or {}
        scenario_file = data.get('scenario_file')
        scenario_id = data.get('scenario_id')
        
        result = run_cashflow_model(
            scenario_file=scenario_file, 
            scenario_id=scenario_id
        )
        
        return jsonify({
            "status": "success",
            "message": result
        })
    except Exception as e:
        return jsonify({
            "status": "error", 
            "message": str(e),
            "type": type(e).__name__
        }), 500

@app.route('/api/sensitivity', methods=['POST'])
def run_sensitivity():
    try:
        # Import sensitivity runner
        try:
            scripts_dir = os.path.join(current_dir, 'scripts')
            sys.path.insert(0, scripts_dir)
            from run_sensitivity_analysis import run_sensitivity_analysis_improved
        except ImportError as import_err:
            return jsonify({
                "status": "error",
                "message": f"Sensitivity analysis module not available: {import_err}"
            }), 500
        
        data = request.get_json() or {}
        config_file = data.get('config_file', 'config/sensitivity_config.json')
        prefix = data.get('prefix', 'sensitivity_results')
        
        # Run sensitivity analysis
        run_sensitivity_analysis_improved(config_file, prefix)
        
        return jsonify({
            "status": "success",
            "message": "Sensitivity analysis completed"
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/asset-cashflows', methods=['GET'])
def get_asset_cashflows():
    if get_data_from_mongodb is None:
        return jsonify({
            "status": "error",
            "message": "Database functionality not available - import failed"
        }), 500
    
    try:
        asset_id = request.args.get('asset_id')
        variables_str = request.args.get('variables')
        granularity = request.args.get('granularity') # 'monthly', 'quarterly', 'yearly'

        query = {}
        if asset_id:
            try:
                asset_id_int = int(asset_id)
                query = {'asset_id': asset_id_int}
            except ValueError:
                query = {'asset_id': asset_id}
        
        # Fetch data from MongoDB
        data = get_data_from_mongodb(collection_name=MONGO_ASSET_OUTPUT_COLLECTION, query=query)
        
        # Convert to DataFrame for easier processing
        df = pd.DataFrame(data)

        if not df.empty:
            # Ensure 'date' column is datetime objects
            df['date'] = pd.to_datetime(df['date'])
            
            # Filter variables if specified
            if variables_str:
                variables = [v.strip() for v in variables_str.split(',')]
                # Always include 'date' and 'asset_id' for grouping/identification
                cols_to_keep = list(set(['date', 'asset_id'] + variables))
                df = df[cols_to_keep]

            # Aggregate by granularity
            if granularity == 'quarterly':
                df['period'] = df['date'].dt.to_period('Q').dt.start_time
            elif granularity == 'yearly':
                df['period'] = df['date'].dt.to_period('Y').dt.start_time
            else: # Default to monthly or if 'monthly' is explicitly passed
                df['period'] = df['date'].dt.to_period('M').dt.start_time
            
            # Group by asset_id and the new 'period' column, summing numerical values
            # Exclude non-numeric columns from sum, like 'period_type' if it exists
            numeric_cols = df.select_dtypes(include=np.number).columns.tolist()
            
            # Ensure 'asset_id' is not summed if it's numeric
            if 'asset_id' in numeric_cols:
                numeric_cols.remove('asset_id')

            # Group and sum only numeric columns, keep 'asset_id' and 'period' as grouping keys
            grouped_df = df.groupby(['asset_id', 'period'])[numeric_cols].sum().reset_index()
            
            # Rename 'period' back to 'date' for consistency with frontend expectation
            grouped_df = grouped_df.rename(columns={'period': 'date'})

            # Convert dates back to string for JSON serialization
            grouped_df['date'] = grouped_df['date'].dt.strftime('%Y-%m-%d')
            
            result_data = grouped_df.to_dict(orient='records')
        else:
            result_data = []

        return jsonify(result_data), 200
    except Exception as e:
        print(f"Error in get_asset_cashflows: {e}")
        return jsonify({
            "status": "error", 
            "message": str(e)
        }), 500

@app.route('/api/asset-ids', methods=['GET'])
def get_asset_ids():
    if get_mongo_client is None:
        return jsonify({
            "status": "error",
            "message": "Database functionality not available - import failed"
        }), 500
    
    try:
        client = get_mongo_client()
        if not client:
            return jsonify({
                "status": "error",
                "message": "Database connection not available"
            }), 500
            
        db = client[MONGO_DB_NAME]
        
        # Fetch asset_id and asset_name from ASSET_inputs_summary_collection
        inputs_collection = db[MONGO_ASSET_INPUTS_SUMMARY_COLLECTION]
        asset_info = inputs_collection.find({}, {'asset_id': 1, 'asset_name': 1, '_id': 0}).to_list(length=None)
        
        # Ensure unique asset_id and name pairs
        unique_assets = {}
        for asset in asset_info:
            if 'asset_id' in asset and 'asset_name' in asset:
                unique_assets[asset['asset_id']] = {'_id': asset['asset_id'], 'name': asset['asset_name']}
        
        asset_list = list(unique_assets.values())
        
        client.close()
        return jsonify(asset_list), 200
    except Exception as e:
        return jsonify({
            "status": "error", 
            "message": str(e)
        }), 500

@app.route('/api/revenue-summary', methods=['GET'])
def get_revenue_summary():
    if get_data_from_mongodb is None:
        return jsonify({
            "status": "error",
            "message": "Database functionality not available - import failed"
        }), 500
    
    try:
        all_cashflows = get_data_from_mongodb(collection_name=MONGO_ASSET_OUTPUT_COLLECTION)

        aggregated_data = defaultdict(lambda: defaultdict(float))
        periods = set()

        for doc in all_cashflows:
            period = doc.get('period')
            asset_id = doc.get('asset_id')
            total_revenue = doc.get('total_revenue', 0) 

            if period and asset_id is not None:
                aggregated_data[period][asset_id] += total_revenue
                periods.add(period)

        summary_list = []
        sorted_periods = sorted(list(periods)) 

        unique_asset_ids = sorted(list(set(doc.get('asset_id') for doc in all_cashflows if doc.get('asset_id') is not None)))

        for period in sorted_periods:
            period_data = {'period': period}
            for asset_id in unique_asset_ids:
                period_data[asset_id] = aggregated_data[period][asset_id]
            summary_list.append(period_data)

        return jsonify(summary_list), 200
    except Exception as e:
        return jsonify({
            "status": "error", 
            "message": str(e)
        }), 500

@app.route('/api/inputs-summary', methods=['GET'])
def get_inputs_summary():
    if get_data_from_mongodb is None:
        return jsonify({
            "status": "error",
            "message": "Database functionality not available - import failed"
        }), 500
    
    try:
        asset_id = request.args.get('asset_id')
        
        query = {}
        if asset_id:
            try:
                asset_id_int = int(asset_id)
                query = {'asset_id': asset_id_int}
            except ValueError:
                query = {'asset_id': asset_id}

        data = get_data_from_mongodb(collection_name=MONGO_ASSET_INPUTS_SUMMARY_COLLECTION, query=query)
        return json.loads(json_util.dumps(data)), 200
    except Exception as e:
        print(f"Error in get_inputs_summary: {e}")
        return jsonify({
            "status": "error", 
            "message": str(e)
        }), 500

# Test endpoint to debug imports
@app.route('/api/debug', methods=['GET'])
def debug_imports():
    return jsonify({
        "python_path": sys.path,
        "current_dir": current_dir,
        "src_dir": src_dir,
        "files_in_current": os.listdir(current_dir) if os.path.exists(current_dir) else [],
        "files_in_src": os.listdir(src_dir) if os.path.exists(src_dir) else [],
        "imports": {
            "run_cashflow_model": run_cashflow_model is not None,
            "get_data_from_mongodb": get_data_from_mongodb is not None,
            "get_mongo_client": get_mongo_client is not None
        },
        "env_vars": {
            "MONGODB_URI": "***" if os.getenv('MONGODB_URI') else None,
            "MONGODB_DB": MONGO_DB_NAME
        }
    })

# For development and production
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port, debug=False)

# For production (some platforms expect this)
application = app