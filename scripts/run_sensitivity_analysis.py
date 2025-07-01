# scripts/run_sensitivity_analysis.py

import json
import os
import subprocess
import sys
from datetime import datetime

def generate_scenario_file(scenario_name, overrides, output_dir="temp_scenarios"):
    """
    Generates a temporary JSON scenario file.
    """
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)  # Go up one level from scripts/
    
    # Create absolute path for scenario file
    full_output_dir = os.path.join(project_root, output_dir)
    os.makedirs(full_output_dir, exist_ok=True)
    
    scenario_data = {
        "scenario_name": scenario_name,
        "overrides": overrides
    }
    file_path = os.path.join(full_output_dir, f"{scenario_name}.json")
    with open(file_path, 'w') as f:
        json.dump(scenario_data, f, indent=4)
    return file_path

def run_main_model(scenario_file=None, scenario_id=None):
    """
    Runs the main cash flow model with an optional scenario file and ID.
    """
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)  # Go up one level from scripts/
    main_py_path = os.path.join(project_root, "src", "main.py")
    
    # Use the same Python executable that's running this script
    command = [sys.executable, main_py_path]
    if scenario_file:
        command.extend(["--scenario", scenario_file])
    if scenario_id:
        command.extend(["--scenario_id", scenario_id])
    
    print(f"Running: {' '.join(command)}")
    
    # Set working directory to project root for consistent path resolution
    result = subprocess.run(command, capture_output=True, text=True, cwd=project_root)
    
    print(result.stdout)
    if result.stderr:
        print(f"Error: {result.stderr}")
    
    return result.returncode == 0

def run_sensitivity_analysis(config_file="config/sensitivity_config.json"):
    """
    Runs the sensitivity analysis based on the configuration file.
    """
    # Ensure we're working from the project root
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    config_path = os.path.join(project_root, config_file)
    
    if not os.path.exists(config_path):
        print(f"Configuration file not found: {config_path}")
        return
    
    with open(config_path, 'r') as f:
        config = json.load(f)

    base_scenario_file = config.get("base_scenario_file")
    output_collection_prefix = config.get("output_collection_prefix", "sensitivity_results")
    sensitivities = config.get("sensitivities", {})

    print(f"Starting sensitivity analysis with {len(sensitivities)} parameters")
    print(f"Output collection prefix: {output_collection_prefix}")

    # Run base case first (if no base_scenario_file, it's the default model run)
    print("\n=== Running Base Case ===")
    base_success = run_main_model(scenario_file=base_scenario_file, scenario_id=f"{output_collection_prefix}_base")
    if not base_success:
        print("Base case failed. Aborting sensitivity analysis.")
        return

    # Track total scenarios to run
    total_scenarios = sum(details.get("steps", 3) for details in sensitivities.values())
    current_scenario = 0

    for param, details in sensitivities.items():
        print(f"\n=== Running Sensitivity for {param.upper()} ===")
        base_value = details["base"]
        min_val, max_val = details["range"]
        steps = details["steps"]

        # Generate values for sensitivity
        if steps == 1:
            values = [base_value]  # Only base value
        else:
            values = [base_value + min_val + i * (max_val - min_val) / (steps - 1) for i in range(steps)]

        print(f"Testing {len(values)} values for {param}: {[f'{v:.4f}' for v in values]}")

        for value in values:
            current_scenario += 1
            overrides = {}
            scenario_name = f"{param}_{value:.4f}"
            scenario_id = f"{output_collection_prefix}_{param}_{value:.4f}"

            if details["type"] == "multiplier":
                # Multiplier is applied to the base value
                overrides[f"global_{param}_multiplier"] = value
            elif details["type"] == "absolute_adjustment":
                # Absolute adjustment is added to the base value
                overrides[f"global_{param}_adjustment_per_mwh"] = value
            elif details["type"] == "basis_points_adjustment":
                # Basis points adjustment is added to the base value
                overrides[f"global_debt_interest_rate_adjustment_bps"] = int(value)
            else:
                print(f"Warning: Unknown sensitivity type '{details['type']}' for parameter {param}")
                continue
            
            # Generate and run scenario
            try:
                scenario_file_path = generate_scenario_file(scenario_name, overrides)
                print(f"  [{current_scenario}/{total_scenarios}] Running scenario: {scenario_name}")
                print(f"  Created scenario file: {scenario_file_path}")
                
                success = run_main_model(scenario_file=scenario_file_path, scenario_id=scenario_id)
                
                if success:
                    print(f"  SUCCESS: Scenario {scenario_name} completed successfully")
                else:
                    print(f"  FAILED: Scenario {scenario_name} failed")
                
                # Clean up temporary file
                if os.path.exists(scenario_file_path):
                    os.remove(scenario_file_path)
                    print(f"  Cleaned up: {scenario_file_path}")
                    
            except Exception as e:
                print(f"  ERROR: Error running scenario {scenario_name}: {e}")
                # Still try to clean up if file was created
                try:
                    scenario_file_path = os.path.join(
                        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "temp_scenarios",
                        f"{scenario_name}.json"
                    )
                    if os.path.exists(scenario_file_path):
                        os.remove(scenario_file_path)
                except:
                    pass

    print(f"\n=== Sensitivity Analysis Complete ===")
    print(f"Completed {current_scenario} scenarios")

if __name__ == '__main__':
    run_sensitivity_analysis()