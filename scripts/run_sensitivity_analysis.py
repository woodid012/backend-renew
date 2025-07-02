# scripts/run_sensitivity_analysis_improved.py

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

def run_sensitivity_analysis_improved(config_file="config/sensitivity_config.json", 
                                    sensitivity_prefix="sensitivity_results"):
    """
    Run sensitivity analysis storing results in dedicated sensitivity collection
    """
    print(f"=== IMPROVED SENSITIVITY ANALYSIS ===")
    print(f"Results will be stored in: {SENSITIVITY_COLLECTION}")
    
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

    print(f"\nStarting sensitivity analysis with {len(sensitivities)} parameters")

    # Run base case first
    print("\n=== Running Base Case ===")
    base_scenario_content = None
    if base_scenario_file and os.path.exists(os.path.join(project_root, base_scenario_file)):
        with open(os.path.join(project_root, base_scenario_file), 'r') as f:
            base_scenario_content = json.load(f)
    
    base_success = run_main_model_with_sensitivity_storage(
        scenario_content=base_scenario_content, 
        scenario_id=f"{output_collection_prefix}_base"
    )
    
    if not base_success:
        print("Base case failed. Aborting sensitivity analysis.")
        return

    # Track scenarios
    total_scenarios = sum(details.get("steps", 3) for details in sensitivities.values())
    current_scenario = 0

    for param, details in sensitivities.items():
        print(f"\n=== Running Sensitivity for {param.upper()} ===")
        base_value = details["base"]
        min_val, max_val = details["range"]
        steps = details["steps"]

        # Generate values for sensitivity
        if steps == 1:
            values = [base_value]
        else:
            values = [base_value + min_val + i * (max_val - min_val) / (steps - 1) for i in range(steps)]

        print(f"Testing {len(values)} values for {param}: {[f'{v:.4f}' for v in values]}")

        for value in values:
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

    print(f"\n=== Sensitivity Analysis Complete ===")
    print(f"Completed {current_scenario} scenarios")
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
        else:
            print(f"\n⚠ WARNING: No sensitivity data found in {SENSITIVITY_COLLECTION}")
        
    except Exception as e:
        print(f"Error verifying: {e}")
    finally:
        if client:
            client.close()

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description="Run improved sensitivity analysis")
    parser.add_argument('--config', type=str, default='config/sensitivity_config.json',
                       help='Path to sensitivity config file')
    parser.add_argument('--prefix', type=str, default='sensitivity_results',
                       help='Sensitivity results prefix')
    
    args = parser.parse_args()
    
    run_sensitivity_analysis_improved(args.config, args.prefix)