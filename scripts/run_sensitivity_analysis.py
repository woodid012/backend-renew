# scripts/run_sensitivity_analysis.py

import json
import os
import subprocess
import sys
from datetime import datetime
import tempfile

# Add the project root and src directory to the Python path for module imports
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)  # Go up one level to project root
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, project_root)
sys.path.insert(0, src_dir)

from src.core.database import get_mongo_client

# Use separate collection for sensitivity results
SENSITIVITY_COLLECTION = "SENS_Asset_Outputs"

def cleanup_sensitivity_results(sensitivity_prefix="sensitivity_results"):
    """
    Clean up existing sensitivity results from the dedicated sensitivity collection
    """
    print(f"=== CLEANING UP EXISTING SENSITIVITY RESULTS ===")
    
    client = None
    try:
        client = get_mongo_client()
        db = client.get_database()
        collection = db[SENSITIVITY_COLLECTION]
        
        # Find all scenario_ids that start with our prefix
        existing_scenarios = collection.distinct("scenario_id", {
            "scenario_id": {"$regex": f"^{sensitivity_prefix}"}
        })
        
        if existing_scenarios:
            print(f"Found {len(existing_scenarios)} existing sensitivity scenarios:")
            for scenario in existing_scenarios:
                print(f"  - {scenario}")
            
            # Count total records to be deleted
            total_records = collection.count_documents({
                "scenario_id": {"$regex": f"^{sensitivity_prefix}"}
            })
            
            print(f"\nDeleting {total_records} sensitivity records...")
            
            # Delete all existing sensitivity results
            result = collection.delete_many({
                "scenario_id": {"$regex": f"^{sensitivity_prefix}"}
            })
            
            print(f"✓ Deleted {result.deleted_count} sensitivity records")
            
        else:
            print("No existing sensitivity results found")
            
        return True
        
    except Exception as e:
        print(f"Error cleaning up: {e}")
        return False
    
    finally:
        if client:
            client.close()

def generate_scenario_content(scenario_name, overrides):
    """
    Generate scenario content as a dictionary (no file creation needed)
    """
    return {
        "scenario_name": scenario_name,
        "overrides": overrides
    }

def run_main_model_with_sensitivity_storage(scenario_content=None, scenario_id=None):
    """
    Run the main model but store results in sensitivity collection
    """
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)  # Go up one level from scripts/
    main_py_path = os.path.join(project_root, "src", "main.py")
    
    # Create temporary scenario file only if needed
    temp_file = None
    try:
        if scenario_content:
            # Create temporary file that gets auto-cleaned
            temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
            json.dump(scenario_content, temp_file, indent=4)
            temp_file.close()
            scenario_file_path = temp_file.name
        else:
            scenario_file_path = None
        
        # Use the same Python executable that's running this script
        command = [sys.executable, main_py_path]
        if scenario_file_path:
            command.extend(["--scenario", scenario_file_path])
        if scenario_id:
            command.extend(["--scenario_id", scenario_id])
        
        print(f"Running: {' '.join(command)}")
        
        # Set working directory to project root for consistent path resolution
        result = subprocess.run(command, capture_output=True, text=True, cwd=project_root)
        
        print(result.stdout)
        if result.stderr:
            print(f"Error: {result.stderr}")
        
        success = result.returncode == 0
        
        if success and scenario_id:
            # Move results from main collection to sensitivity collection
            move_to_sensitivity_collection(scenario_id)
        
        return success
        
    finally:
        # Clean up temporary file
        if temp_file and os.path.exists(temp_file.name):
            os.unlink(temp_file.name)

def move_to_sensitivity_collection(scenario_id):
    """
    Move scenario results from main collection to sensitivity collection
    """
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
            print(f"  Moved {len(scenario_records)} records to {SENSITIVITY_COLLECTION}")
            
            # Remove from main collection
            delete_result = main_collection.delete_many({"scenario_id": scenario_id})
            print(f"  Removed {delete_result.deleted_count} records from main collection")
        else:
            print(f"  No records found for scenario {scenario_id}")
    
    except Exception as e:
        print(f"  Error moving records: {e}")
    
    finally:
        if client:
            client.close()

def generate_sensitivity_values(base_value, min_val, max_val, steps):
    """
    Generate sensitivity values, excluding the base case.
    
    Args:
        base_value (float): Base case value (will be excluded)
        min_val (float): Minimum adjustment from base
        max_val (float): Maximum adjustment from base  
        steps (int): Total number of steps including base case
    
    Returns:
        list: Values to test, excluding base case
    """
    if steps <= 1:
        return []  # No variations to test
    
    # Generate the full range including base case
    if steps == 2:
        # Only test one extreme if steps=2
        full_values = [base_value + min_val, base_value + max_val]
    else:
        # Generate evenly spaced values
        full_values = [base_value + min_val + i * (max_val - min_val) / (steps - 1) for i in range(steps)]
    
    # Filter out the base case (allowing for small floating point differences)
    tolerance = 1e-8
    sensitivity_values = [v for v in full_values if abs(v - base_value) > tolerance]
    
    return sensitivity_values

def run_sensitivity_analysis_improved(config_file="config/sensitivity_config.json", 
                                    sensitivity_prefix="sensitivity_results"):
    """
    Run sensitivity analysis storing results in dedicated sensitivity collection.
    OPTIMIZED: Skips base case since it already exists.
    """
    print(f"=== OPTIMIZED SENSITIVITY ANALYSIS ===")
    print(f"Results will be stored in: {SENSITIVITY_COLLECTION}")
    print(f"OPTIMIZATION: Skipping base case values (assumes base case already exists)")
    
    # Step 1: Clean up existing results
    if not cleanup_sensitivity_results(sensitivity_prefix):
        print("Failed to clean up existing results. Aborting.")
        return
    
    # Step 2: Load config
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    config_path = os.path.join(project_root, config_file)
    
    if not os.path.exists(config_path):
        print(f"Configuration file not found: {config_path}")
        return
    
    with open(config_path, 'r') as f:
        config = json.load(f)

    base_scenario_file = config.get("base_scenario_file")
    output_collection_prefix = config.get("output_collection_prefix", sensitivity_prefix)
    sensitivities = config.get("sensitivities", {})

    print(f"\nStarting optimized sensitivity analysis with {len(sensitivities)} parameters")

    # Track scenarios - calculate total excluding base cases
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
        print("No sensitivity scenarios to run. All parameters only have base case values.")
        return

    current_scenario = 0

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

        print(f"  Base case: {base_value:.4f} (SKIPPED - already exists)")
        print(f"  Testing {len(sensitivity_values)} sensitivity values: {[f'{v:.4f}' for v in sensitivity_values]}")

        for value in sensitivity_values:
            current_scenario += 1
            overrides = {}
            scenario_name = f"{param}_{value:.4f}"
            scenario_id = f"{output_collection_prefix}_{param}_{value:.4f}"

            if details["type"] == "multiplier":
                overrides[f"global_{param}_multiplier"] = value
            elif details["type"] == "absolute_adjustment":
                overrides[f"global_{param}_adjustment_per_mwh"] = value
            elif details["type"] == "basis_points_adjustment":
                overrides[f"global_debt_interest_rate_adjustment_bps"] = int(value)
            else:
                print(f"Warning: Unknown sensitivity type '{details['type']}' for parameter {param}")
                continue
            
            # Generate scenario content (no file creation)
            scenario_content = generate_scenario_content(scenario_name, overrides)
            
            print(f"  [{current_scenario}/{total_scenarios}] Running scenario: {scenario_name}")
            
            success = run_main_model_with_sensitivity_storage(
                scenario_content=scenario_content, 
                scenario_id=scenario_id
            )
            
            if success:
                print(f"  SUCCESS: Scenario {scenario_name} completed")
            else:
                print(f"  FAILED: Scenario {scenario_name} failed")

    print(f"\n=== Optimized Sensitivity Analysis Complete ===")
    print(f"Completed {current_scenario} sensitivity scenarios")
    print(f"Base case scenarios skipped (assumed to exist already)")
    print(f"Results stored in MongoDB collection: {SENSITIVITY_COLLECTION}")
    
    # Verify results
    verify_sensitivity_results(output_collection_prefix)

def verify_sensitivity_results(sensitivity_prefix):
    """
    Verify sensitivity results in the dedicated collection
    """
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
                
            # Check for any base case scenarios that might have been created
            base_scenarios = [s for s in scenarios if 'base' in s.lower() or '_1.0000' in s]
            if base_scenarios:
                print(f"\nNote: Found {len(base_scenarios)} base case scenarios:")
                for base_scenario in base_scenarios:
                    print(f"  {base_scenario}")
                print("Consider removing these if base case already exists elsewhere")
        else:
            print(f"\n⚠ WARNING: No sensitivity data found in {SENSITIVITY_COLLECTION}")
        
    except Exception as e:
        print(f"Error verifying: {e}")
    finally:
        if client:
            client.close()

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description="Run optimized sensitivity analysis (skips base cases)")
    parser.add_argument('--config', type=str, default='config/sensitivity_config.json',
                       help='Path to sensitivity config file')
    parser.add_argument('--prefix', type=str, default='sensitivity_results',
                       help='Sensitivity results prefix')
    
    args = parser.parse_args()
    
    run_sensitivity_analysis_improved(args.config, args.prefix)