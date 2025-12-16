# scripts/run_sensitivity_analysis.py

import json
import os
import sys
from datetime import datetime
import tempfile

# Add the project root and src directory to the Python path for module imports
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)  # Go up one level to project root
src_dir = os.path.join(project_root, 'src')

# Add to Python path
sys.path.insert(0, project_root)
sys.path.insert(0, src_dir)

# Import main function directly to avoid subprocess Unicode issues
from src.main import run_cashflow_model
from src.core.database import database_lifecycle, db_manager, mongo_session, get_data_from_mongodb
from src.core.input_processor import load_price_data
from scripts.enhanced_sensitivity_summary import generate_enhanced_sensitivity_summary

# Use separate collection for sensitivity results
SENSITIVITY_COLLECTION = "SENS_Asset_Outputs"

def cleanup_sensitivity_results(sensitivity_prefix="sensitivity_results", unique_id=None):
    """Clean up existing sensitivity results using optimized connection"""
    print(f"=== CLEANING UP EXISTING SENSITIVITY RESULTS (Prefix: {sensitivity_prefix}, Unique ID: {unique_id}) ===")
    
    if not unique_id:
        print("Error: No unique_id provided for cleanup. Aborting to prevent data loss.")
        return False
        
    try:
        with mongo_session() as db_mgr:
            collection = db_mgr.get_collection(SENSITIVITY_COLLECTION)
            
            # Filter by both prefix AND unique_id
            query = {
                "scenario_id": {"$regex": f"^{sensitivity_prefix}"},
                "unique_id": unique_id
            }
            
            existing_scenarios = collection.distinct("scenario_id", query)
            
            if existing_scenarios:
                print(f"Found {len(existing_scenarios)} existing sensitivity scenarios for this portfolio")
                total_records = collection.count_documents(query)
                print(f"Deleting {total_records} sensitivity records...")
                result = collection.delete_many(query)
                print(f"Deleted {result.deleted_count} sensitivity records")
            else:
                print("No existing sensitivity results found for this portfolio")
                
            return True
            
    except Exception as e:
        print(f"Error cleaning up: {e}")
        return False

def move_to_sensitivity_collection(scenario_id, unique_id):
    """Move scenario results from main collection to sensitivity collection using optimized connection"""
    if not unique_id:
        print(f"Error: No unique_id provided for moving results for scenario {scenario_id}")
        return

    try:
        with mongo_session() as db_mgr:
            main_collection = db_mgr.get_collection("ASSET_cash_flows")  # Your main collection
            sens_collection = db_mgr.get_collection(SENSITIVITY_COLLECTION)
            
            # Find records for this scenario AND unique_id
            query = {"scenario_id": scenario_id, "unique_id": unique_id}
            scenario_records = list(main_collection.find(query))
            
            if scenario_records:
                # Insert into sensitivity collection
                sens_collection.insert_many(scenario_records)
                print(f"    Moved {len(scenario_records)} records to {SENSITIVITY_COLLECTION}")
                
                # Remove from main collection
                delete_result = main_collection.delete_many(query)
                print(f"    Removed {delete_result.deleted_count} records from main collection")
            else:
                print(f"    No records found for scenario {scenario_id} and unique_id {unique_id}")
    
    except Exception as e:
        print(f"    Error moving records: {e}")

def run_single_scenario_direct(scenario_content, scenario_id, assets, monthly_prices, yearly_spreads, portfolio_name, portfolio_unique_id, progress_callback=None):
    """Run a single scenario using direct function call
    
    Args:
        progress_callback: Optional function to call with progress updates (message, type='info')
    
    Returns:
        tuple: (success: bool, error_message: str or None)
    """
    def log_progress(message, progress_type='info'):
        """Helper to log progress via callback or print"""
        if progress_callback:
            progress_callback(message, progress_type)
        else:
            print(message, flush=True)
    
    try:
        # Create temporary file for scenario
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            json.dump(scenario_content, temp_file, indent=4)
            temp_file_path = temp_file.name
        
        print(f"  Running scenario: {scenario_id}")
        
        # Call the function directly instead of subprocess to avoid Unicode issues
        # NOTE: Database connection is already established by the parent context
        result = run_cashflow_model(
            assets=assets,
            monthly_prices=monthly_prices,
            yearly_spreads=yearly_spreads,
            portfolio_name=portfolio_name,
            scenario_file=temp_file_path,
            scenario_id=scenario_id,
            replace_data=True,
            portfolio_unique_id=portfolio_unique_id
        )
        
        # Clean up temp file
        os.unlink(temp_file_path)
        
        # Move results to sensitivity collection
        move_to_sensitivity_collection(scenario_id, portfolio_unique_id)
        
        return True, None
        
    except Exception as e:
        error_msg = f"ERROR in scenario {scenario_id}: {e}"
        print(f"  {error_msg}")
        log_progress(error_msg, 'error')
        # Clean up temp file if it exists
        if 'temp_file_path' in locals() and os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
        return False, str(e)

def generate_sensitivity_values(base_value, min_val, max_val, steps):
    """Generate sensitivity values, excluding the base case"""
    if steps <= 1:
        return []
    
    # Generate the full range including base case
    if steps == 2:
        full_values = [base_value + min_val, base_value + max_val]
    else:
        full_values = [base_value + min_val + i * (max_val - min_val) / (steps - 1) for i in range(steps)]
    
    # Filter out the base case (allowing for small floating point differences)
    tolerance = 1e-8
    sensitivity_values = [v for v in full_values if abs(v - base_value) > tolerance]
    
    return sensitivity_values

def run_sensitivity_analysis_optimized(config_file=None, sensitivity_prefix="sensitivity_results", config=None, portfolio_name=None, progress_callback=None):
    """Run sensitivity analysis with optimized database connection
    
    Args:
        config_file: Path to config file (relative to project root)
        sensitivity_prefix: Prefix for sensitivity results
        config: Config dictionary object (takes precedence over config_file)
        portfolio_name: Optional portfolio name to filter assets
        progress_callback: Optional function to call with progress updates (message, type='info')
    """
    def log_progress(message, progress_type='info'):
        """Helper to log progress via callback or print"""
        if progress_callback:
            progress_callback(message, progress_type)
        else:
            print(message, flush=True)
    log_progress("Starting sensitivity analysis...", 'info')
    log_progress("=== OPTIMIZED SENSITIVITY ANALYSIS ===", 'info')
    log_progress(f"Results will be stored in: {SENSITIVITY_COLLECTION}", 'info')
    
    # Step 1: Clean up existing results
    # Step 1: Clean up existing results will be done AFTER loading config to get unique_id
    # log_progress("Cleaning up existing sensitivity results...", 'info')
    # if not cleanup_sensitivity_results(sensitivity_prefix):
    #     log_progress("Failed to clean up existing results. Aborting.", 'error')
    #     return
    # log_progress("Cleanup complete", 'success')
    
    # Step 2: Load config - use config object if provided, otherwise load from file
    log_progress("Loading sensitivity configuration...", 'info')
    if config is not None:
        # Use provided config object
        log_progress("Using provided config object", 'info')
    elif config_file:
        # Load from file
        config_path = os.path.join(project_root, config_file)
        
        if not os.path.exists(config_path):
            print(f"Configuration file not found: {config_path}")
            return
        
        with open(config_path, 'r') as f:
            config = json.load(f)
    else:
        # Default to config file
        config_file = "config/sensitivity_config.json"
        config_path = os.path.join(project_root, config_file)
        
        if not os.path.exists(config_path):
            print(f"Configuration file not found: {config_path}")
            return
        
        with open(config_path, 'r') as f:
            config = json.load(f)

    sensitivities = config.get("sensitivities", {})
    output_collection_prefix = config.get("output_collection_prefix", sensitivity_prefix)

    # Load assets and prices ONCE
    log_progress("Loading assets and price data from MongoDB...", 'info')
    query = {}
    if portfolio_name:
        query['unique_id'] = portfolio_name
        log_progress(f"Filtering by unique_id: {portfolio_name}", 'info')
    
    config_data = get_data_from_mongodb('CONFIG_Inputs', query=query)
    if not config_data:
        error_msg = f"Could not load config data from MongoDB for portfolio unique_id: {portfolio_name}" if portfolio_name else "Could not load config data from MongoDB"
        log_progress(f"Error: {error_msg}", 'error')
        return
    assets = config_data[0].get('asset_inputs', [])
    log_progress(f"Loaded {len(assets)} assets from MongoDB", 'success')
    # Extract portfolio unique_id from config (portfolio_name parameter is actually unique_id)
    portfolio_unique_id = portfolio_name if portfolio_name else config_data[0].get('unique_id')
    # Get PlatformName for display purposes only (not used for lookups)
    platform_name_display = config_data[0].get('PlatformName', portfolio_unique_id)
    
    if not portfolio_unique_id:
        log_progress("Error: Could not find unique_id in config data from MongoDB", 'error')
        return
    
    log_progress(f"Using portfolio unique_id: {portfolio_unique_id} (display name: {platform_name_display})", 'info')

    # Step 1 (Delayed): Clean up existing results now that we have unique_id
    log_progress("Cleaning up existing sensitivity results...", 'info')
    if not cleanup_sensitivity_results(output_collection_prefix, portfolio_unique_id):
        log_progress("Failed to clean up existing results. Aborting.", 'error')
        return
    log_progress("Cleanup complete", 'success')
    
    log_progress("Loading price data files...", 'info')
    monthly_price_path = os.path.join(project_root, 'data', 'raw_inputs', 'merchant_price_monthly.csv')
    yearly_spread_path = os.path.join(project_root, 'data', 'raw_inputs', 'merchant_yearly_spreads.csv')
    
    if not os.path.exists(monthly_price_path) or not os.path.exists(yearly_spread_path):
        log_progress(f"Error: Price files not found at {monthly_price_path} or {yearly_spread_path}", 'error')
        return
        
    monthly_prices, yearly_spreads = load_price_data(monthly_price_path, yearly_spread_path)
    log_progress("Price data loaded successfully", 'success')

    log_progress(f"\nStarting sensitivity analysis with {len(sensitivities)} parameters", 'info')
    log_progress("SENSITIVITY CONFIGURATION:", 'info')
    for param, details in sensitivities.items():
        log_progress(f"  Parameter: {param}", 'info')
        for key, value in details.items():
            log_progress(f"    {key}: {value}", 'info')

    # Calculate total scenarios
    total_scenarios = 0
    for param, details in sensitivities.items():
        base_value = details["base"]
        min_val, max_val = details["range"]
        steps = details["steps"]
        
        sensitivity_values = generate_sensitivity_values(base_value, min_val, max_val, steps)
        scenarios_for_param = len(sensitivity_values)
        total_scenarios += scenarios_for_param
        
        print(f"  {param}: {scenarios_for_param} scenarios (base case {base_value} skipped)")

    print(f"\nTotal sensitivity scenarios to run: {total_scenarios}")
    
    if total_scenarios == 0:
        print("No sensitivity scenarios to run.")
        return

    current_scenario = 0
    successful_scenarios = 0
    failed_scenarios = 0

    for param, details in sensitivities.items():
        log_progress(f"Running sensitivity for {param.upper()}...", 'info')
        print(f"\n=== Running Sensitivity for {param.upper()} ===")
        base_value = details["base"]
        min_val, max_val = details["range"]
        steps = details["steps"]

        # Generate values for sensitivity (excluding base case)
        sensitivity_values = generate_sensitivity_values(base_value, min_val, max_val, steps)
        
        if not sensitivity_values:
            print(f"  No sensitivity values to test for {param} (only base case)")
            continue

        print(f"  Base case: {base_value:.4f} (SKIPPED)")
        print(f"  Testing {len(sensitivity_values)} values: {[f'{v:.4f}' for v in sensitivity_values]}")

        for value in sensitivity_values:
            current_scenario += 1
            scenario_name = f"{param}_{value:.4f}"
            scenario_id = f"{output_collection_prefix}_{param}_{value:.4f}"

            # Create overrides
            overrides = {}
            if details["type"] == "multiplier":
                overrides[f"global_{param}_multiplier"] = value
            elif details["type"] == "absolute_adjustment":
                overrides[f"global_{param}_adjustment_per_mwh"] = value
            elif details["type"] == "basis_points_adjustment":
                overrides[f"global_debt_interest_rate_adjustment_bps"] = int(value)
            else:
                print(f"Warning: Unknown sensitivity type '{details['type']}' for parameter {param}")
                continue
            
            # Generate scenario content
            scenario_content = {
                "scenario_name": scenario_name,
                "overrides": overrides
            }
            
            log_progress(f"Running scenario: {scenario_name} ({current_scenario}/{total_scenarios})", 'info')
            print(f"  [{current_scenario}/{total_scenarios}] {scenario_name}")
            
            # Run scenario using direct call - pass progress callback for error reporting
            success, error_msg = run_single_scenario_direct(scenario_content, scenario_id, assets, monthly_prices, yearly_spreads, platform_name_display, portfolio_unique_id, progress_callback=log_progress)
            
            if success:
                successful_scenarios += 1
                log_progress(f"Scenario {scenario_name} completed", 'success')
                print(f"    SUCCESS")
            else:
                failed_scenarios += 1
                # Error details were already logged by run_single_scenario_direct
                log_progress(f"Scenario {scenario_name} failed: {error_msg}", 'error')
                print(f"    FAILED: {error_msg}")

    log_progress("Sensitivity analysis complete!", 'success')
    print(f"\n=== SENSITIVITY ANALYSIS COMPLETE ===")
    print(f"Total scenarios: {total_scenarios}")
    print(f"Successful: {successful_scenarios}")
    print(f"Failed: {failed_scenarios}")
    print(f"Success rate: {successful_scenarios/total_scenarios*100:.1f}%" if total_scenarios > 0 else "N/A")
    print(f"Results stored in MongoDB collection: {SENSITIVITY_COLLECTION}")
    
    # Verify results
    verify_sensitivity_results(output_collection_prefix, portfolio_unique_id)

    # Generate enhanced sensitivity summary after analysis is complete
    print("\n=== GENERATING ENHANCED SENSITIVITY SUMMARY ===")
    generate_enhanced_sensitivity_summary(sensitivity_prefix=output_collection_prefix)

def verify_sensitivity_results(sensitivity_prefix, unique_id=None):
    """Verify sensitivity results in the dedicated collection using optimized connection"""
    try:
        with mongo_session() as db_mgr:
            collection = db_mgr.get_collection(SENSITIVITY_COLLECTION)
            
            # Check what we have
            query = {"scenario_id": {"$regex": f"^{sensitivity_prefix}"}}
            if unique_id:
                query["unique_id"] = unique_id
                
            scenario_data = list(collection.find(query))
            
            if scenario_data:
                unique_scenarios = len(set(record['scenario_id'] for record in scenario_data))
                print(f"\n=== VERIFICATION ===")
                print(f"Total sensitivity records: {len(scenario_data)}")
                print(f"Unique scenarios: {unique_scenarios}")
                
                # Sample scenarios
                scenarios = sorted(set(record['scenario_id'] for record in scenario_data))
                print(f"Sample scenarios:")
                for scenario in scenarios[:5]:
                    count = sum(1 for r in scenario_data if r['scenario_id'] == scenario)
                    print(f"  {scenario}: {count} records")
                
                if len(scenarios) > 5:
                    print(f"  ... and {len(scenarios) - 5} more scenarios")
            else:
                print(f"\nWARNING: No sensitivity data found in {SENSITIVITY_COLLECTION}")
        
    except Exception as e:
        print(f"Error verifying: {e}")


# Alias for backward compatibility
run_sensitivity_analysis_improved = run_sensitivity_analysis_optimized

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description="Run optimized sensitivity analysis")
    parser.add_argument('--config', type=str, default='config/sensitivity_config.json',
                       help='Path to sensitivity config file')
    parser.add_argument('--prefix', type=str, default='sensitivity_results',
                       help='Sensitivity results prefix')
    
    args = parser.parse_args()
    
    # Use the database lifecycle context manager for the entire sensitivity analysis
    with database_lifecycle():
        run_sensitivity_analysis_optimized(args.config, args.prefix)
