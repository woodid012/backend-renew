import json
import os
import subprocess
from datetime import datetime

def generate_scenario_file(scenario_name, overrides, output_dir="temp_scenarios"):
    """
    Generates a temporary JSON scenario file.
    """
    os.makedirs(output_dir, exist_ok=True)
    scenario_data = {
        "scenario_name": scenario_name,
        "overrides": overrides
    }
    file_path = os.path.join(output_dir, f"{scenario_name}.json")
    with open(file_path, 'w') as f:
        json.dump(scenario_data, f, indent=4)
    return file_path

def run_main_model(scenario_file=None, scenario_id=None):
    """
    Runs the main cash flow model with an optional scenario file and ID.
    """
    command = ["python", "src/main.py"]
    if scenario_file:
        command.extend(["--scenario", scenario_file])
    if scenario_id:
        command.extend(["--scenario_id", scenario_id])
    
    print(f"Running: {' '.join(command)}")
    result = subprocess.run(command, capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print(f"Error: {result.stderr}")
    return result.returncode == 0

def run_sensitivity_analysis(config_file="config/sensitivity_config.json"):
    """
    Runs the sensitivity analysis based on the configuration file.
    """
    with open(config_file, 'r') as f:
        config = json.load(f)

    base_scenario_file = config.get("base_scenario_file")
    output_collection_prefix = config.get("output_collection_prefix", "sensitivity_results")
    sensitivities = config.get("sensitivities", {})

    # Run base case first (if no base_scenario_file, it's the default model run)
    print("\n=== Running Base Case ===")
    base_success = run_main_model(scenario_file=base_scenario_file, scenario_id=f"{output_collection_prefix}_base")
    if not base_success:
        print("Base case failed. Aborting sensitivity analysis.")
        return

    for param, details in sensitivities.items():
        print(f"\n=== Running Sensitivity for {param.upper()} ===")
        base_value = details["base"]
        min_val, max_val = details["range"]
        steps = details["steps"]

        # Generate values for sensitivity
        if steps == 1:
            values = [base_value] # Only base value
        else:
            values = [base_value + min_val + i * (max_val - min_val) / (steps - 1) for i in range(steps)]

        for value in values:
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
            
            # Generate and run scenario
            scenario_file_path = generate_scenario_file(scenario_name, overrides)
            print(f"  Running scenario: {scenario_name}")
            run_main_model(scenario_file=scenario_file_path, scenario_id=scenario_id)
            os.remove(scenario_file_path) # Clean up temporary file

    print("\n=== Sensitivity Analysis Complete ===")

if __name__ == '__main__':
    run_sensitivity_analysis()
