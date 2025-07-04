# scripts/run_sensitivity_unicode_safe.py

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
from src.core.database import get_mongo_client

# Use separate collection for sensitivity results
SENSITIVITY_COLLECTION = "SENS_Asset_Outputs"

def cleanup_sensitivity_results(sensitivity_prefix="sensitivity_results"):
    """Clean up existing sensitivity results"""
    print("=== CLEANING UP EXISTING SENSITIVITY RESULTS ===")
    
    client = None
    try:
        client = get_mongo_client()
        db = client.get_database()
        collection = db[SENSITIVITY_COLLECTION]
        
        existing_scenarios = collection.distinct("scenario_id", {
            "scenario_id": {"$regex": f"^{sensitivity_prefix}"}
        })
        
        if existing_scenarios:
            print(f"Found {len(existing_scenarios)} existing sensitivity scenarios")
            total_records = collection.count_documents({
                "scenario_id": {"$regex": f"^{sensitivity_prefix}"}
            })
            print(f"Deleting {total_records} sensitivity records...")
            result = collection.delete_many({
                "scenario_id": {"$regex": f"^{sensitivity_prefix}"}
            })
            print(f"Deleted {result.deleted_count} sensitivity records")
        else:
            print("No existing sensitivity results found")
            
        return True
        
    except Exception as e:
        print(f"Error cleaning up: {e}")
        return False
    finally:
        if client:
            client.close()

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

def move_to_sensitivity_collection(scenario_id):
    """Move scenario results from main collection to sensitivity collection"""
    client = None
    try:
        client = get_mongo_client()
        db = client.get_database()
        
        main_collection = db["ASSET_cash_flows"]  # Your main collection
        sens_collection = db[SENSITIVITY_COLLECTION]
        
        # Find records for this scenario
        scenario_records = list(main_collection.find({"scenario_id": scenario_id}))
        
        if scenario_records:
            # Insert into sensitivity collection
            sens_collection.insert_many(scenario_records)
            print(f"    Moved {len(scenario_records)} records to {SENSITIVITY_COLLECTION}")
            
            # Remove from main collection
            delete_result = main_collection.delete_many({"scenario_id": scenario_id})
            print(f"    Removed {delete_result.deleted_count} records from main collection")
        else:
            print(f"    No records found for scenario {scenario_id}")
    
    except Exception as e:
        print(f"    Error moving records: {e}")
    finally:
        if client:
            client.close()

def run_single_scenario_direct(scenario_content, scenario_id):
    """Run a single scenario using direct function call"""
    try:
        # Create temporary file for scenario
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            json.dump(scenario_content, temp_file, indent=4)
            temp_file_path = temp_file.name
        
        print(f"  Running scenario: {scenario_id}")
        
        # Call the function directly instead of subprocess to avoid Unicode issues
        result = run_cashflow_model(
            scenario_file=temp_file_path,
            scenario_id=scenario_id,
            replace_data=True
        )
        
        # Clean up temp file
        os.unlink(temp_file_path)
        
        # Move results to sensitivity collection
        move_to_sensitivity_collection(scenario_id)
        
        return True
        
    except Exception as e:
        print(f"  ERROR in scenario {scenario_id}: {e}")
        # Clean up temp file if it exists
        if 'temp_file_path' in locals() and os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
        return False

def run_sensitivity_analysis_unicode_safe(config_file="config/sensitivity_config.json", 
                                        sensitivity_prefix="sensitivity_results"):
    """Run sensitivity analysis without Unicode issues"""
    print("=== UNICODE-SAFE SENSITIVITY ANALYSIS ===")
    print(f"Results will be stored in: {SENSITIVITY_COLLECTION}")
    
    # Step 1: Clean up existing results
    if not cleanup_sensitivity_results(sensitivity_prefix):
        print("Failed to clean up existing results. Aborting.")
        return
    
    # Step 2: Load config
    config_path = os.path.join(project_root, config_file)
    
    if not os.path.exists(config_path):
        print(f"Configuration file not found: {config_path}")
        return
    
    with open(config_path, 'r') as f:
        config = json.load(f)

    sensitivities = config.get("sensitivities", {})
    output_collection_prefix = config.get("output_collection_prefix", sensitivity_prefix)

    print(f"\nStarting sensitivity analysis with {len(sensitivities)} parameters")

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
            
            print(f"  [{current_scenario}/{total_scenarios}] {scenario_name}")
            
            # Run scenario using direct call
            success = run_single_scenario_direct(scenario_content, scenario_id)
            
            if success:
                successful_scenarios += 1
                print(f"    SUCCESS")
            else:
                failed_scenarios += 1
                print(f"    FAILED")

    print(f"\n=== SENSITIVITY ANALYSIS COMPLETE ===")
    print(f"Total scenarios: {total_scenarios}")
    print(f"Successful: {successful_scenarios}")
    print(f"Failed: {failed_scenarios}")
    print(f"Success rate: {successful_scenarios/total_scenarios*100:.1f}%" if total_scenarios > 0 else "N/A")
    print(f"Results stored in MongoDB collection: {SENSITIVITY_COLLECTION}")
    
    # Verify results
    verify_sensitivity_results(output_collection_prefix)

def verify_sensitivity_results(sensitivity_prefix):
    """Verify sensitivity results in the dedicated collection"""
    client = None
    try:
        client = get_mongo_client()
        db = client.get_database()
        collection = db[SENSITIVITY_COLLECTION]
        
        # Check what we have
        scenario_data = list(collection.find({"scenario_id": {"$regex": f"^{sensitivity_prefix}"}}))
        
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
    finally:
        if client:
            client.close()

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description="Run Unicode-safe sensitivity analysis")
    parser.add_argument('--config', type=str, default='config/sensitivity_config.json',
                       help='Path to sensitivity config file')
    parser.add_argument('--prefix', type=str, default='sensitivity_results',
                       help='Sensitivity results prefix')
    
    args = parser.parse_args()
    
    run_sensitivity_analysis_unicode_safe(args.config, args.prefix)