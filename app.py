# app.py

from flask import Flask, request, jsonify, Response, stream_with_context
from flask_cors import CORS
import os
import sys
from collections import defaultdict
from bson import json_util
from dotenv import load_dotenv
import json
import pandas as pd
import numpy as np
import traceback
import threading
import queue

# Force unbuffered output for Vercel logs
sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None
sys.stderr.reconfigure(line_buffering=True) if hasattr(sys.stderr, 'reconfigure') else None

load_dotenv('.env.local')

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
database_lifecycle = None
load_price_data = None
MONGO_ASSET_OUTPUT_COLLECTION = 'ASSET_cash_flows'

try:
    from src.main import run_cashflow_model
    print("✅ Successfully imported run_cashflow_model", flush=True)
except ImportError as e:
    print(f"⚠️ Could not import run_cashflow_model: {e}", flush=True)
    traceback.print_exc()
    run_cashflow_model = None

try:
    from src.core.database import get_data_from_mongodb, get_mongo_client, database_lifecycle
    print("✅ Successfully imported database functions", flush=True)
except ImportError as e:
    print(f"⚠️ Could not import database functions: {e}", flush=True)
    traceback.print_exc()
    get_data_from_mongodb = None
    get_mongo_client = None
    database_lifecycle = None

try:
    from src.core.input_processor import load_price_data
    print("✅ Successfully imported load_price_data", flush=True)
except ImportError as e:
    print(f"⚠️ Could not import load_price_data: {e}", flush=True)
    traceback.print_exc()
    load_price_data = None

try:
    from src.config import MONGO_ASSET_OUTPUT_COLLECTION, MONGO_ASSET_INPUTS_SUMMARY_COLLECTION
    print(f"✅ Successfully imported config: {MONGO_ASSET_OUTPUT_COLLECTION}", flush=True)
except ImportError as e:
    print(f"⚠️ Could not import config: {e}", flush=True)
    traceback.print_exc()
    MONGO_ASSET_OUTPUT_COLLECTION = 'ASSET_cash_flows'
    MONGO_ASSET_INPUTS_SUMMARY_COLLECTION = 'ASSET_inputs_summary'

app = Flask(__name__)

# Enable CORS for all domains and all routes
CORS(app, origins=["*"])

# Global error handler to log all exceptions
@app.errorhandler(Exception)
def handle_exception(e):
    print(f"\n❌ UNHANDLED EXCEPTION: {type(e).__name__}: {str(e)}", flush=True)
    traceback.print_exc()
    return jsonify({
        "status": "error",
        "message": str(e),
        "type": type(e).__name__
    }), 500

# Print server startup information
print("\n" + "="*80, flush=True)
print("🚀 FLASK SERVER STARTING UP", flush=True)
print("="*80, flush=True)
print(f"📁 Working Directory: {current_dir}", flush=True)
print(f"📁 Source Directory: {src_dir}", flush=True)
print(f"🗄️  MongoDB Database: {MONGO_DB_NAME}", flush=True)
print(f"📦 MongoDB Collection: {MONGO_ASSET_OUTPUT_COLLECTION}", flush=True)
print("="*80 + "\n", flush=True)

@app.route('/', methods=['GET'])
def health_check():
    print("\n[HEALTH CHECK] Route: GET /", flush=True)
    print(f"  → Executing: health_check() function in app.py", flush=True)
    try:
        response = {
            "status": "healthy", 
            "message": "Renewable Finance Backend API",
            "platform": "Vercel",
            "imports": {
                "run_cashflow_model": run_cashflow_model is not None,
                "get_data_from_mongodb": get_data_from_mongodb is not None,
                "get_mongo_client": get_mongo_client is not None
            },
            "mongo_db": MONGO_DB_NAME,
            "collection": MONGO_ASSET_OUTPUT_COLLECTION
        }
        print(f"  ✅ Health check successful", flush=True)
        return jsonify(response)
    except Exception as e:
        print(f"  ❌ Health check error: {e}", flush=True)
        traceback.print_exc()
    return jsonify({
        "status": "error",
        "message": str(e),
        "type": type(e).__name__
    }), 500

# Import Price Curve Manager
try:
    from src.core.price_curve_manager import analyze_excel_file, ingest_excel_file, get_price_curves_list, load_price_data_from_mongo
    print("✅ Successfully imported price_curve_manager", flush=True)
except ImportError as e:
    print(f"⚠️ Could not import price_curve_manager: {e}", flush=True)
    traceback.print_exc()
    analyze_excel_file = None
    ingest_excel_file = None
    get_price_curves_list = None
    load_price_data_from_mongo = None

@app.route('/api/list-price-curves', methods=['GET'])
def list_price_curves():
    print("[PRICE] List curves request received", flush=True)
    try:
        if get_mongo_client:
            client = get_mongo_client()
            db = client[MONGO_DB_NAME]
            curves = get_price_curves_list(db)
            # define a sort key to sort by date if possible (assuming format "AC Mon Year")
            # For now just alphabetical or simple sort
            curves.sort()
            return jsonify({"status": "success", "curves": curves})
        else:
            return jsonify({"status": "error", "message": "DB Connection not available"}), 500
    except Exception as e:
        print(f"List Curves Error: {e}")
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/price-curves/analyze', methods=['POST'])
def analyze_price_curve():
    print("[PRICE] Analyze request received", flush=True)
    if 'file' not in request.files:
        return jsonify({"status": "error", "message": "No file part"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"status": "error", "message": "No selected file"}), 400
        
    try:
        # Save temp file
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            file.save(tmp.name)
            tmp_path = tmp.name
            
        result = analyze_excel_file(tmp_path, file.filename)
        
        # Clean up
        os.unlink(tmp_path)
        
        return jsonify({"status": "success", "data": result})
    except Exception as e:
        print(f"Analyze Error: {e}")
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/price-curves/upload', methods=['POST'])
def upload_price_curve():
    print("[PRICE] Upload request received", flush=True)
    if 'file' not in request.files:
        return jsonify({"status": "error", "message": "No file part"}), 400
    file = request.files['file']
    curve_name = request.form.get('curve_name')
    
    if not curve_name:
         return jsonify({"status": "error", "message": "Curve name is required"}), 400
         
    try:
        # Save temp file
        import tempfile
        import json as _json
        import time as _time
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            file.save(tmp.name)
            tmp_path = tmp.name
        
        print(f"[PRICE] Upload starting for curve '{curve_name}' using temp file {tmp_path}", flush=True)
            
        # Get DB connection
        if get_mongo_client:
            print("[PRICE] Obtaining MongoDB client for price curve ingest...", flush=True)
            # region agent log
            try:
                with open(r'c:\Projects\renew\.cursor\debug.log', 'a', encoding='utf-8') as _f:
                    _f.write(_json.dumps({
                        "sessionId": "debug-session",
                        "runId": "upload-price-curve",
                        "hypothesisId": "H6",
                        "location": "app.py:upload_price_curve",
                        "message": "before_get_mongo_client",
                        "data": {"curve_name": curve_name},
                        "timestamp": int(_time.time() * 1000),
                    }) + "\n")
            except Exception:
                pass
            # endregion
            client = get_mongo_client()
            # region agent log
            try:
                with open(r'c:\Projects\renew\.cursor\debug.log', 'a', encoding='utf-8') as _f:
                    _f.write(_json.dumps({
                        "sessionId": "debug-session",
                        "runId": "upload-price-curve",
                        "hypothesisId": "H7",
                        "location": "app.py:upload_price_curve",
                        "message": "after_get_mongo_client",
                        "data": {"client_is_none": client is None},
                        "timestamp": int(_time.time() * 1000),
                    }) + "\n")
            except Exception:
                pass
            # endregion
            print("[PRICE] MongoDB client obtained, selecting database...", flush=True)
            db = client[MONGO_DB_NAME]
            print(f"[PRICE] Calling ingest_excel_file for curve '{curve_name}'", flush=True)
            count = ingest_excel_file(tmp_path, curve_name, db)
            print(f"[PRICE] ingest_excel_file completed for '{curve_name}' with {count} records", flush=True)
        else:
            raise Exception("DB Connection not available")
        
        os.unlink(tmp_path)
        
        return jsonify({"status": "success", "message": f"Ingested {count} records for {curve_name}"})
    except Exception as e:
        print(f"Upload Error: {e}")
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/run-model', methods=['POST'])
def run_model():
    print("\n" + "="*80, flush=True)
    print("[RUN MODEL] Route: POST /api/run-model", flush=True)
    print(f"  → Executing: run_model() function in app.py", flush=True)
    print("="*80, flush=True)
    
    if run_cashflow_model is None:
        return jsonify({
            "status": "error",
            "message": "Model functionality not available - import failed"
        }), 500
    
    if get_data_from_mongodb is None or database_lifecycle is None:
        return jsonify({
            "status": "error",
            "message": "Database functionality not available - import failed"
        }), 500
    
    if load_price_data_from_mongo is None:
        return jsonify({
            "status": "error",
            "message": "Price data loading functionality not available - import failed"
        }), 500
    
    try:
        data = request.get_json() or {}
        scenario_file = data.get('scenario_file')
        scenario_id = data.get('scenario_id')
        portfolio_name = data.get('portfolio')
        price_curve_name = data.get('price_curve')
        
        print(f"  📥 Request Data:", flush=True)
        print(f"     - scenario_file: {scenario_file}", flush=True)
        print(f"     - scenario_id: {scenario_id}", flush=True)
        print(f"     - portfolio: {portfolio_name}", flush=True)
        print(f"     - price_curve: {price_curve_name}", flush=True)
        print(f"  🔄 Setting up database connection...", flush=True)
        
        # Use database_lifecycle context manager to manage connection
        with database_lifecycle():
            print(f"  ✅ Database connection established", flush=True)
            
            # --- Load Assets ---
            print(f"  📊 Loading assets from MongoDB collection: CONFIG_Inputs", flush=True)
            query = {}
            if portfolio_name:
                query['unique_id'] = portfolio_name
                print(f"     - Filtering by unique_id: {portfolio_name}", flush=True)
            
            config_data = get_data_from_mongodb('CONFIG_Inputs', query=query)
            
            if not config_data:
                msg = f"Could not load config data from MongoDB for portfolio unique_id: {portfolio_name}" if portfolio_name else "Could not load config data from MongoDB"
                return jsonify({
                    "status": "error",
                    "message": msg
                }), 500
            
            selected_config = config_data[-1]
            assets = selected_config.get('asset_inputs', [])
            
            # Use PortfolioTitle (or fallback to PlatformName) from the config document for display
            portfolio_name = selected_config.get('PortfolioTitle') or selected_config.get('PlatformName')
            if portfolio_name:
                print(f"     - Using PortfolioTitle from config: {portfolio_name}", flush=True)
            
            portfolio_unique_id = selected_config.get('unique_id')
            if portfolio_unique_id:
                print(f"     - Portfolio unique_id: {portfolio_unique_id}", flush=True)
            else:
                print(f"     - ⚠️  Warning: Portfolio unique_id not found in CONFIG_Inputs", flush=True)
            
            print(f"  ✅ Loaded {len(assets)} assets from MongoDB", flush=True)
            
            if not assets:
                return jsonify({
                    "status": "error",
                    "message": "No assets found in CONFIG_Inputs collection"
                }), 500
            
            # --- Load Price Data from MongoDB ---
            print(f"  📄 Loading price data from MongoDB...", flush=True)
            
            client = get_mongo_client()
            db = client[MONGO_DB_NAME]
            
            # If no curve name provided, find the default (most recent or alphabetical?)
            # The client said: "old data (AC Nov 2024), new data (AC Oct 2025)"
            # Let's verify what curves exist first
            available_curves = get_price_curves_list(db)
            
            if not price_curve_name:
                if not available_curves:
                    return jsonify({
                        "status": "error",
                        "message": "No price curves found in database. Please upload a price curve first."
                    }), 500
                
                # Default to the "default" logic - maybe the last one sorted alphabetically?
                # "AC Oct 2025" comes after "AC Nov 2024". 
                # Sort descending to get "Oct 2025" (O) after "Nov 2024" (N)? No.
                # Actually "AC O..." < "AC N..."
                # Let's rely on simple string sort for now, or just pick the first one and warn.
                # Ideally the frontend forces a selection.
                # Let's sort and pick the last one (assuming YYYY or useful naming)
                # But naming is "AC Oct 2025".
                # Let's just pick the last one in the list for now.
                available_curves.sort()
                price_curve_name = available_curves[-1] # Pick last
                print(f"     - ⚠️  No price curve specified. Defaulting to: {price_curve_name}", flush=True)
            
            if price_curve_name not in available_curves:
                return jsonify({
                    "status": "error",
                    "message": f"Price curve '{price_curve_name}' not found. Available: {available_curves}"
                }), 400
            
            print(f"     - Using price curve: {price_curve_name}", flush=True)
            
            monthly_prices, yearly_spreads = load_price_data_from_mongo(db, price_curve_name)
            
            print(f"  ✅ Price data loaded successfully from MongoDB", flush=True)
            print(f"     - Monthly prices shape: {monthly_prices.shape if monthly_prices is not None else 'None'}", flush=True)
            print(f"     - Yearly spreads shape: {yearly_spreads.shape if yearly_spreads is not None else 'None'}", flush=True)
            
            # Validate that price data was loaded correctly
            if monthly_prices.empty and yearly_spreads.empty:
                print("     ⚠️  Warning: Loaded Empty Price Dataframes")
            
            # Load model settings from MongoDB
            print(f"  ⚙️  Loading model settings from MongoDB...", flush=True)
            model_settings_data = get_data_from_mongodb('CONFIG_modelSettings', query={})
            model_settings = None
            if model_settings_data and len(model_settings_data) > 0:
                # Get the most recent settings document
                settings_doc = model_settings_data[-1]
                # Remove MongoDB _id field
                settings_doc.pop('_id', None)
                settings_doc.pop('updated_at', None)
                model_settings = settings_doc
                print(f"  ✅ Model settings loaded from MongoDB", flush=True)
            else:
                print(f"  ℹ️  No model settings found in MongoDB, using config.py defaults", flush=True)
            
            # Call run_cashflow_model with all required arguments
            # Pass required arguments positionally, optional as keywords
            print(f"\n  🎯 CALLING MAIN MODEL FUNCTION:", flush=True)
            print(f"     → Function: run_cashflow_model() from src/main.py", flush=True)
            print(f"     → Arguments:", flush=True)
            print(f"        - assets: {len(assets)} assets", flush=True)
            print(f"        - monthly_prices: DataFrame with shape {monthly_prices.shape}", flush=True)
            print(f"        - yearly_spreads: DataFrame with shape {yearly_spreads.shape}", flush=True)
            print(f"        - scenario_file: {scenario_file}", flush=True)
            print(f"        - scenario_id: {scenario_id}", flush=True)
            print(f"        - portfolio_name: {portfolio_name}", flush=True)
            print(f"        - portfolio_unique_id: {portfolio_unique_id}", flush=True)
            print(f"        - model_settings: {'Loaded from MongoDB' if model_settings else 'Using config.py defaults'}", flush=True)
            print(f"  " + "-"*76, flush=True)
            result = run_cashflow_model(
                assets,
                monthly_prices,
                yearly_spreads,
                portfolio_name,
                scenario_file=scenario_file, 
                scenario_id=scenario_id,
                model_settings=model_settings,
                portfolio_unique_id=portfolio_unique_id
            )
            
            print(f"  ✅ Model execution completed", flush=True)
            print(f"  📤 Returning result to client", flush=True)
            print("="*80 + "\n", flush=True)
        
        return jsonify({
            "status": "success",
            "message": result
        })
    except Exception as e:
        error_msg = str(e)
        error_type = type(e).__name__
        print(f"  ❌ Error in run_model: {error_type}: {error_msg}", flush=True)
        traceback.print_exc()
        return jsonify({
            "status": "error", 
            "message": error_msg,
            "type": error_type
        }), 500

@app.route('/api/run-model-stream', methods=['POST'])
def run_model_stream():
    """Stream model execution progress via Server-Sent Events"""
    print("\n" + "="*80, flush=True)
    print("[RUN MODEL STREAM] Route: POST /api/run-model-stream", flush=True)
    print(f"  → Executing: run_model_stream() function in app.py", flush=True)
    print("="*80, flush=True)
    
    if run_cashflow_model is None:
        def error_gen():
            yield f"data: {json.dumps({'type': 'error', 'message': 'Model functionality not available - import failed'})}\n\n"
        return Response(error_gen(), mimetype='text/event-stream')
    
    if get_data_from_mongodb is None or database_lifecycle is None:
        def error_gen():
            yield f"data: {json.dumps({'type': 'error', 'message': 'Database functionality not available - import failed'})}\n\n"
        return Response(error_gen(), mimetype='text/event-stream')
    
    if load_price_data_from_mongo is None:
        def error_gen():
            yield f"data: {json.dumps({'type': 'error', 'message': 'Price data loading functionality not available - import failed'})}\n\n"
        return Response(error_gen(), mimetype='text/event-stream')
    
    def send_progress(message, progress_type='info'):
        """Helper to send progress message"""
        return f"data: {json.dumps({'type': progress_type, 'message': message})}\n\n"
    
    @stream_with_context
    def generate():
        try:
            data = request.get_json() or {}
            scenario_file = data.get('scenario_file')
            scenario_id = data.get('scenario_id')
            portfolio_name = data.get('portfolio')
            price_curve_name = data.get('price_curve')
            
            # Create a queue for progress updates
            progress_queue = queue.Queue()
            
            def progress_callback(message, progress_type='info'):
                """Progress callback that adds messages to queue"""
                progress_queue.put({'type': progress_type, 'message': message})
            
            # Start model execution in a separate thread
            def run_model_thread():
                try:
                    with database_lifecycle():
                        progress_callback("Database connection established", 'info')
                        progress_callback("Loading assets from MongoDB...", 'info')
                        
                        query = {}
                        if portfolio_name:
                            query['unique_id'] = portfolio_name
                        
                        config_data = get_data_from_mongodb('CONFIG_Inputs', query=query)
                        
                        if not config_data:
                            progress_callback("Error: Could not load config data from MongoDB", 'error')
                            progress_queue.put({'type': 'done', 'status': 'error'})
                            return
                        
                        selected_config = config_data[-1]
                        assets = selected_config.get('asset_inputs', [])
                        
                        # Use PortfolioTitle (or fallback to PlatformName) for display
                        actual_portfolio_name = portfolio_name
                        portfolio_title = selected_config.get('PortfolioTitle') or selected_config.get('PlatformName')
                        if portfolio_title:
                            actual_portfolio_name = portfolio_title
                        
                        portfolio_unique_id = selected_config.get('unique_id')
                        
                        progress_callback(f"Loaded {len(assets)} assets from MongoDB", 'info')
                        
                        if not assets:
                            progress_callback("Error: No assets found", 'error')
                            progress_queue.put({'type': 'done', 'status': 'error'})
                            return
                        
                        # --- Load Price Data ---
                        progress_callback("Loading price data from MongoDB...", 'info')
                        
                        client = get_mongo_client()
                        db = client[MONGO_DB_NAME]
                        available_curves = get_price_curves_list(db)
                        
                        # Handle curve name
                        selected_curve = price_curve_name
                        if not selected_curve:
                            if not available_curves:
                                progress_callback("Error: No price curves in database", 'error')
                                progress_queue.put({'type': 'done', 'status': 'error'})
                                return
                            
                            available_curves.sort()
                            selected_curve = available_curves[-1]
                            progress_callback(f"No curve selected, using default: {selected_curve}", 'warning')
                        
                        if selected_curve not in available_curves:
                             progress_callback(f"Error: Price curve '{selected_curve}' not found", 'error')
                             progress_queue.put({'type': 'done', 'status': 'error'})
                             return
                             
                        monthly_prices, yearly_spreads = load_price_data_from_mongo(db, selected_curve)
                        progress_callback(f"Price data loaded ({selected_curve})", 'info')
                        
                        # Load model settings
                        model_settings_data = get_data_from_mongodb('CONFIG_modelSettings', query={})
                        model_settings = None
                        if model_settings_data and len(model_settings_data) > 0:
                            settings_doc = model_settings_data[-1]
                            settings_doc.pop('_id', None)
                            settings_doc.pop('updated_at', None)
                            model_settings = settings_doc
                        
                        # Run model with progress callback
                        result = run_cashflow_model(
                            assets,
                            monthly_prices,
                            yearly_spreads,
                            actual_portfolio_name,
                            scenario_file=scenario_file,
                            scenario_id=scenario_id,
                            model_settings=model_settings,
                            portfolio_unique_id=portfolio_unique_id,
                            progress_callback=progress_callback
                        )
                        
                        progress_callback("Base case complete!", 'success')
                        progress_queue.put({'type': 'done', 'status': 'success', 'result': result})
                        
                except Exception as e:
                    error_msg = str(e)
                    error_type = type(e).__name__
                    progress_callback(f"Error: {error_type}: {error_msg}", 'error')
                    progress_queue.put({'type': 'done', 'status': 'error', 'message': error_msg})
                    traceback.print_exc()
            
            # Start the model thread
            thread = threading.Thread(target=run_model_thread)
            thread.daemon = True
            thread.start()
            
            # Stream progress updates
            while True:
                try:
                    # Wait for progress update with timeout
                    item = progress_queue.get(timeout=1)
                    
                    if item['type'] == 'done':
                        # Send final message
                        yield send_progress(item.get('message', 'Model execution completed'), item.get('status', 'success'))
                        break
                    else:
                        # Send progress update
                        yield send_progress(item['message'], item.get('type', 'info'))
                        
                except queue.Empty:
                    # Send keepalive
                    yield ": keepalive\n\n"
                    continue
                except Exception as e:
                    yield send_progress(f"Error in stream: {str(e)}", 'error')
                    break
            
            # Wait for thread to complete
            thread.join(timeout=300)
            
        except Exception as e:
            yield send_progress(f"Error: {str(e)}", 'error')
    
    return Response(generate(), mimetype='text/event-stream', headers={
        'Cache-Control': 'no-cache',
        'X-Accel-Buffering': 'no'
    })

@app.route('/api/sensitivity', methods=['POST'])
def run_sensitivity():
    print("\n" + "="*80, flush=True)
    print("[SENSITIVITY] Route: POST /api/sensitivity", flush=True)
    print(f"  → Executing: run_sensitivity() function in app.py", flush=True)
    print("="*80, flush=True)
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
        portfolio_name = data.get('portfolio')
        
        print(f"  📥 Request Data:", flush=True)
        print(f"     - config_file: {config_file}", flush=True)
        print(f"     - prefix: {prefix}", flush=True)
        print(f"     - portfolio: {portfolio_name}", flush=True)
        
        # Try to load sensitivity config from MongoDB first
        sensitivity_config = None
        if get_mongo_client is not None:
            try:
                client = get_mongo_client()
                if client:
                    db = client[MONGO_DB_NAME]
                    collection = db['SENSITIVITY_Config']
                    
                    # First try portfolio-specific config if portfolio_name provided and is a string
                    if portfolio_name and isinstance(portfolio_name, str):
                        config_doc = collection.find_one(
                            {'portfolio_name': portfolio_name},
                            sort=[('_id', -1)]
                        )
                        if config_doc:
                            # Remove MongoDB-specific fields
                            sensitivity_config = {k: v for k, v in config_doc.items() 
                                                if k not in ['_id', 'updated_at', 'portfolio_name', 'unique_id']}
                            print(f"  ✅ Loaded portfolio-specific sensitivity config from MongoDB for portfolio: {portfolio_name}", flush=True)
                    
                    # If no portfolio-specific config found, try general/default config
                    if not sensitivity_config:
                        config_doc = collection.find_one({'unique_id': 'default'})
                        if config_doc:
                            # Remove MongoDB-specific fields
                            sensitivity_config = {k: v for k, v in config_doc.items() 
                                                if k not in ['_id', 'updated_at', 'portfolio_name', 'unique_id']}
                            print(f"  ✅ Loaded general/default sensitivity config from MongoDB", flush=True)
                    
                    # Note: Do not close the client here - it's managed by DatabaseManager singleton
            except Exception as mongo_err:
                print(f"  ⚠️  Could not load from MongoDB: {mongo_err}, falling back to file", flush=True)
        
        # Run sensitivity analysis with config from MongoDB if available, otherwise use config_file
        if sensitivity_config:
            run_sensitivity_analysis_improved(
                config_file=None, 
                sensitivity_prefix=prefix, 
                config=sensitivity_config,
                portfolio_name=portfolio_name
            )
        else:
            run_sensitivity_analysis_improved(
                config_file, 
                prefix, 
                portfolio_name=portfolio_name
            )
        
        return jsonify({
            "status": "success",
            "message": "Sensitivity analysis completed",
            "config_source": "MongoDB" if sensitivity_config else "file"
        })
    except Exception as e:
        error_msg = str(e)
        print(f"  ❌ Error in run_sensitivity: {error_msg}", flush=True)
        traceback.print_exc()
        return jsonify({
            "status": "error",
            "message": error_msg
        }), 500

@app.route('/api/sensitivity-stream', methods=['POST'])
def run_sensitivity_stream():
    """Stream sensitivity analysis progress via Server-Sent Events"""
    print("\n" + "="*80, flush=True)
    print("[SENSITIVITY STREAM] Route: POST /api/sensitivity-stream", flush=True)
    print(f"  → Executing: run_sensitivity_stream() function in app.py", flush=True)
    print("="*80, flush=True)
    
    def send_progress(message, progress_type='info'):
        """Helper to send progress message"""
        return f"data: {json.dumps({'type': progress_type, 'message': message})}\n\n"
    
    @stream_with_context
    def generate():
        try:
            # Import sensitivity runner
            try:
                scripts_dir = os.path.join(current_dir, 'scripts')
                sys.path.insert(0, scripts_dir)
                from run_sensitivity_analysis import run_sensitivity_analysis_improved
            except ImportError as import_err:
                yield send_progress(f"Sensitivity analysis module not available: {import_err}", 'error')
                return
            
            data = request.get_json() or {}
            config_file = data.get('config_file', 'config/sensitivity_config.json')
            prefix = data.get('prefix', 'sensitivity_results')
            portfolio_name = data.get('portfolio')
            
            # Create a queue for progress updates
            progress_queue = queue.Queue()
            
            def progress_callback(message, progress_type='info'):
                """Progress callback that adds messages to queue"""
                progress_queue.put({'type': progress_type, 'message': message})
            
            # Start sensitivity analysis in a separate thread
            def run_sensitivity_thread():
                try:
                    progress_callback("Initializing sensitivity analysis...", 'info')
                    
                    # Wrap in database_lifecycle to ensure proper connection management
                    with database_lifecycle():
                        progress_callback("Database connection established", 'info')
                        
                        # Try to load sensitivity config from MongoDB first
                        sensitivity_config = None
                        if get_mongo_client is not None:
                            try:
                                progress_callback("Loading sensitivity configuration from MongoDB...", 'info')
                                client = get_mongo_client()
                                if client:
                                    db = client[MONGO_DB_NAME]
                                    collection = db['SENSITIVITY_Config']
                                    
                                    # First try portfolio-specific config if portfolio_name provided and is a string
                                    if portfolio_name and isinstance(portfolio_name, str):
                                        config_doc = collection.find_one(
                                            {'portfolio_name': portfolio_name},
                                            sort=[('_id', -1)]
                                        )
                                        if config_doc:
                                            sensitivity_config = {k: v for k, v in config_doc.items() 
                                                                if k not in ['_id', 'updated_at', 'portfolio_name', 'unique_id']}
                                            progress_callback("Loaded portfolio-specific sensitivity configuration from MongoDB", 'success')
                                    
                                    # If no portfolio-specific config found, try general/default config
                                    if not sensitivity_config:
                                        config_doc = collection.find_one({'unique_id': 'default'})
                                        if config_doc:
                                            sensitivity_config = {k: v for k, v in config_doc.items() 
                                                                if k not in ['_id', 'updated_at', 'portfolio_name', 'unique_id']}
                                            progress_callback("Loaded general/default sensitivity configuration from MongoDB", 'success')
                                        else:
                                            progress_callback("No sensitivity configuration found in MongoDB, using default", 'info')
                                # Note: Do not close the client here - it's managed by DatabaseManager singleton
                            except Exception as mongo_err:
                                progress_callback(f"Could not load from MongoDB: {mongo_err}, using default config", 'warning')
                        
                        # Run sensitivity analysis with progress callback
                        progress_callback("Starting sensitivity analysis...", 'info')
                        if sensitivity_config:
                            run_sensitivity_analysis_improved(
                                config_file=None,
                                sensitivity_prefix=prefix,
                                config=sensitivity_config,
                                portfolio_name=portfolio_name,
                                progress_callback=progress_callback
                            )
                        else:
                            run_sensitivity_analysis_improved(
                                config_file,
                                prefix,
                                portfolio_name=portfolio_name,
                                progress_callback=progress_callback
                            )
                    
                    progress_callback("Sensitivity analysis complete!", 'success')
                    progress_queue.put({'type': 'done', 'status': 'success'})
                    
                except Exception as e:
                    error_msg = str(e)
                    progress_callback(f"Error: {error_msg}", 'error')
                    progress_queue.put({'type': 'done', 'status': 'error', 'message': error_msg})
                    traceback.print_exc()
            
            # Start the sensitivity thread
            thread = threading.Thread(target=run_sensitivity_thread)
            thread.daemon = True
            thread.start()
            
            # Stream progress updates
            while True:
                try:
                    # Wait for progress update with timeout
                    item = progress_queue.get(timeout=1)
                    
                    if item['type'] == 'done':
                        # Send final message
                        yield send_progress(item.get('message', 'Sensitivity analysis completed'), item.get('status', 'success'))
                        break
                    else:
                        # Send progress update
                        yield send_progress(item['message'], item.get('type', 'info'))
                        
                except queue.Empty:
                    # Send keepalive
                    yield ": keepalive\n\n"
                    continue
                except Exception as e:
                    yield send_progress(f"Error in stream: {str(e)}", 'error')
                    break
            
            # Wait for thread to complete
            thread.join(timeout=1800)  # 30 minutes timeout for sensitivity
            
        except Exception as e:
            yield send_progress(f"Error: {str(e)}", 'error')
    
    return Response(generate(), mimetype='text/event-stream', headers={
        'Cache-Control': 'no-cache',
        'X-Accel-Buffering': 'no'
    })

@app.route('/api/asset-cashflows', methods=['GET'])
def get_asset_cashflows():
    print("\n[ASSET CASHFLOWS] Route: GET /api/asset-cashflows", flush=True)
    print(f"  → Executing: get_asset_cashflows() function in app.py", flush=True)
    asset_id = request.args.get('asset_id')
    variables_str = request.args.get('variables')
    granularity = request.args.get('granularity')
    print(f"  📥 Query Parameters: asset_id={asset_id}, variables={variables_str}, granularity={granularity}", flush=True)
    
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
        print(f"Error in get_asset_cashflows: {e}", flush=True)
        traceback.print_exc()
        return jsonify({
            "status": "error", 
            "message": str(e)
        }), 500

@app.route('/api/asset-ids', methods=['GET'])
def get_asset_ids():
    print("\n[ASSET IDS] Route: GET /api/asset-ids", flush=True)
    print(f"  → Executing: get_asset_ids() function in app.py", flush=True)
    
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
        print(f"Error in get_asset_ids: {e}", flush=True)
        traceback.print_exc()
        return jsonify({
            "status": "error", 
            "message": str(e)
        }), 500

@app.route('/api/revenue-summary', methods=['GET'])
def get_revenue_summary():
    print("\n[REVENUE SUMMARY] Route: GET /api/revenue-summary", flush=True)
    print(f"  → Executing: get_revenue_summary() function in app.py", flush=True)
    
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
        print(f"Error in get_revenue_summary: {e}", flush=True)
        traceback.print_exc()
        return jsonify({
            "status": "error", 
            "message": str(e)
        }), 500

@app.route('/api/inputs-summary', methods=['GET'])
def get_inputs_summary():
    print("\n[INPUTS SUMMARY] Route: GET /api/inputs-summary", flush=True)
    print(f"  → Executing: get_inputs_summary() function in app.py", flush=True)
    asset_id = request.args.get('asset_id')
    print(f"  📥 Query Parameter: asset_id={asset_id}", flush=True)
    
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
        print(f"Error in get_inputs_summary: {e}", flush=True)
        traceback.print_exc()
        return jsonify({
            "status": "error", 
            "message": str(e)
        }), 500

@app.route('/api/sensitivity-inputs', methods=['GET', 'POST'])
def sensitivity_inputs():
    print("\n[SENSITIVITY INPUTS] Route: " + request.method + " /api/sensitivity-inputs", flush=True)
    print(f"  → Executing: sensitivity_inputs() function in app.py", flush=True)
    
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
        collection = db['SENSITIVITY_Config']
        
        if request.method == 'GET':
            # Get sensitivity config from MongoDB, fallback to file
            # Accept both unique_id and portfolio for backward compatibility
            # If neither provided, load general/default config
            unique_id = request.args.get('unique_id')
            portfolio_name = request.args.get('portfolio')
            
            query = {}
            if unique_id:
                # Query by unique_id (preferred)
                query['unique_id'] = unique_id
                print(f"  → Querying by unique_id: {unique_id}", flush=True)
            elif portfolio_name:
                # Fallback to portfolio_name for backward compatibility
                query['portfolio_name'] = portfolio_name
                print(f"  → Querying by portfolio_name: {portfolio_name}", flush=True)
            else:
                # No identifier provided - load general/default config
                query['unique_id'] = 'default'
                print(f"  → Querying for general/default sensitivity config", flush=True)
            
            # Query without sort when we have a specific identifier (much faster)
            config_doc = collection.find_one(query)
            
            if config_doc:
                # Remove MongoDB _id for JSON serialization
                if '_id' in config_doc:
                    config_doc['_id'] = str(config_doc['_id'])
                identifier = unique_id if unique_id else (portfolio_name if portfolio_name else 'default')
                print(f"  ✅ Found sensitivity config in MongoDB for: {identifier}", flush=True)
                return jsonify(config_doc), 200
            else:
                # Fallback to file
                config_file_path = os.path.join(current_dir, 'config', 'sensitivity_config.json')
                if os.path.exists(config_file_path):
                    try:
                        with open(config_file_path, 'r') as f:
                            file_config = json.load(f)
                        identifier = unique_id if unique_id else (portfolio_name if portfolio_name else 'default')
                        print(f"  ✅ Loaded sensitivity config from file (MongoDB not found for: {identifier})", flush=True)
                        return jsonify(file_config), 200
                    except Exception as file_err:
                        print(f"  ⚠️  Error reading config file: {file_err}, returning default config", flush=True)
                        # Fall through to return default config
                
                # If no config found anywhere, return default structure
                identifier = unique_id if unique_id else (portfolio_name if portfolio_name else 'default')
                print(f"  ℹ️  No sensitivity config found for: {identifier}, returning default structure", flush=True)
                default_config = {
                    "base_scenario_file": None,
                    "output_collection_prefix": "sensitivity_results",
                    "sensitivities": {}
                }
                return jsonify(default_config), 200
        
        elif request.method == 'POST':
            # Save sensitivity config to MongoDB
            data = request.get_json() or {}
            portfolio_name = data.get('portfolio_name')
            unique_id = data.get('unique_id')
            
            # If no unique_id or portfolio_name provided, save as general/default config
            if not unique_id and not portfolio_name:
                # Save as general config with unique_id: "default"
                unique_id = 'default'
                print(f"  → No identifier provided, saving as general/default config", flush=True)
            else:
                # Try to get unique_id from portfolio_name if not provided
                if not unique_id and portfolio_name:
                    # Look up unique_id from CONFIG_Inputs - try PortfolioTitle first, then PlatformName
                    config_collection = db['CONFIG_Inputs']
                    config_doc = config_collection.find_one({'PortfolioTitle': portfolio_name})
                    if not config_doc:
                        config_doc = config_collection.find_one({'PlatformName': portfolio_name})
                    if config_doc:
                        unique_id = config_doc.get('unique_id')
                        print(f"  → Found unique_id from CONFIG_Inputs: {unique_id} for portfolio_name: {portfolio_name}", flush=True)
            
            # Store unique_id (always use unique_id for general config)
            data['unique_id'] = unique_id
            # Only store portfolio_name if it was provided (for backward compatibility)
            if portfolio_name:
                data['portfolio_name'] = portfolio_name
            
            # Add timestamp
            data['updated_at'] = pd.Timestamp.now().isoformat()
            
            # Save to MongoDB (upsert based on unique_id)
            update_query = {'unique_id': unique_id}
            result = collection.update_one(
                update_query,
                {'$set': data},
                upsert=True
            )
            
            identifier = unique_id
            print(f"  ✅ Saved sensitivity config to MongoDB for: {identifier}", flush=True)
            print(f"     - Matched: {result.matched_count}, Modified: {result.modified_count}, Upserted: {result.upserted_id is not None}", flush=True)
            
            # Also optionally save to file as backup
            config_file_path = os.path.join(current_dir, 'config', 'sensitivity_config.json')
            try:
                # Remove MongoDB-specific fields before saving to file
                file_data = {k: v for k, v in data.items() if k not in ['_id', 'updated_at', 'portfolio_name']}
                with open(config_file_path, 'w') as f:
                    json.dump(file_data, f, indent=4)
                print(f"  ✅ Also saved sensitivity config to file: {config_file_path}", flush=True)
            except Exception as file_err:
                print(f"  ⚠️  Warning: Could not save to file: {file_err}", flush=True)
            
            return jsonify({
                "status": "success",
                "message": "Sensitivity config saved successfully",
                "unique_id": unique_id,
                "portfolio_name": portfolio_name,
                "result": {
                    "matched": result.matched_count,
                    "modified": result.modified_count,
                    "upserted": result.upserted_id is not None
                }
            }), 200
        
    except Exception as e:
        print(f"Error in sensitivity_inputs: {e}", flush=True)
        traceback.print_exc()
        return jsonify({
            "status": "error", 
            "message": str(e)
        }), 500
    # Note: Do not close the client here - it's managed by DatabaseManager singleton

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
    # Determine debug mode from environment (default to True for local development)
    debug_env = os.environ.get('FLASK_DEBUG', '1')
    debug_mode = not (debug_env in ['0', 'false', 'False'])
    print("\n" + "="*80, flush=True)
    print("🌐 STARTING FLASK SERVER", flush=True)
    print("="*80, flush=True)
    print(f"  📍 Host: 0.0.0.0 (all interfaces)", flush=True)
    print(f"  🔌 Port: {port}", flush=True)
    print(f"  🐛 Debug Mode: {debug_mode}", flush=True)
    print(f"  📡 Server will be available at: http://localhost:{port}", flush=True)
    print("="*80, flush=True)
    print("✅ Server is ready to accept requests!", flush=True)
    print("="*80 + "\n", flush=True)
    app.run(host='0.0.0.0', port=port, debug=debug_mode)

# For production (some platforms expect this)
application = app