from flask import Flask, request, jsonify
import os
import sys

# Add src to path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(current_dir, 'src'))

from src.main import run_cashflow_model

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

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port, debug=False)