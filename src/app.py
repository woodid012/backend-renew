from flask import Flask, request, jsonify
import os
import sys
from collections import defaultdict
from bson import json_util
from dotenv import load_dotenv

load_dotenv()

MONGO_DB_NAME = os.getenv('MONGODB_DB')

# Add src to path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(current_dir, 'src'))

from main import run_cashflow_model

app = Flask(__name__)

@app.route('/', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "message": "Renewable Finance Backend API"})

@app.route('/api/run-model', methods=['POST'])
def run_model():
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
            "message": str(e)
        }), 500

@app.route('/api/sensitivity', methods=['POST'])
def run_sensitivity():
    try:
        # Import sensitivity runner
        from scripts.run_sensitivity_analysis import run_sensitivity_analysis_improved
        
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

from core.database import get_data_from_mongodb
from bson import json_util
import json

@app.route('/api/asset-cashflows', methods=['GET'])
def get_asset_cashflows():
    try:
        asset_id = request.args.get('asset_id')
        
        query = {}
        if asset_id:
            try:
                asset_id_int = int(asset_id)
                query = {'asset_id': asset_id_int}
            except ValueError:
                # Fallback to string if conversion fails, though unlikely for this specific issue
                query = {'asset_id': asset_id}
        
        # Assuming 'ASSET_cash_flow' is the collection name
        data = get_data_from_mongodb(collection_name='ASSET_cash_flows', query=query)
        
        # Convert ObjectId to string for JSON serialization
        return json.loads(json_util.dumps(data)), 200
    except Exception as e:
        print(f"Error in get_asset_cashflows: {e}") # Debug print
        return jsonify({
            "status": "error", 
            "message": str(e)
        }), 500

@app.route('/api/asset-ids', methods=['GET'])
def get_asset_ids():
    try:
        client = get_mongo_client()
        db = client[MONGO_DB_NAME]
        collection = db['ASSET_cash_flow'] # Assuming this is the collection where asset_id is stored
        asset_ids = collection.distinct('asset_id')
        client.close()
        return jsonify(asset_ids), 200
    except Exception as e:
        return jsonify({
            "status": "error", 
            "message": str(e)
        }), 500

@app.route('/api/revenue-summary', methods=['GET'])
def get_revenue_summary():
    try:
        all_cashflows = get_data_from_mongodb(collection_name='ASSET_cash_flows')

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

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port, debug=False)