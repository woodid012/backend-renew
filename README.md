### Current Setup Flow:

1.  **Input Data (`inputs/` folder):**
    *   This is where your raw data resides (e.g., `zebre_2025-01-13.json` for asset data, `merchant_price_monthly.csv` for prices).
    *   `core/input_processor.py` is responsible for loading and parsing these files into Python data structures (Pandas DataFrames, lists of dictionaries).

2.  **Core Model (`main.py`):**
    *   This is the heart of the financial model.
    *   It orchestrates the entire calculation process:
        *   Loads initial data using `core/input_processor.py`.
        *   **Scenario Application (`core/scenario_manager.py`):** If you provide a `--scenario` file, `main.py` uses `core/scenario_manager.py` to apply overrides (e.g., adjust prices, CAPEX multipliers) to the loaded input data *before* calculations begin.
        *   Performs various financial calculations using modules in the `calculations/` folder:
            *   `calculations/revenue.py`: Calculates revenue.
            *   `calculations/expenses.py`: Calculates operating and capital expenses.
            *   `calculations/depreciation.py`: Calculates straight-line depreciation.
            *   `calculations/debt.py`: Sizes and schedules debt.
            *   `calculations/cashflow.py`: Aggregates all components into a final cash flow.
        *   Calculates Equity IRR (`core/equity_irr.py`).
        *   Generates summary data (`core/summary_generator.py`).
        *   **Output Saving:**
            *   Saves detailed outputs to local JSON files (`results/` folder) using `core/output_generator.py`.
            *   Saves key results (final cash flow, revenue, asset inputs summary) to your MongoDB database using `core/database.py`. Each entry in MongoDB can now be tagged with a `scenario_id` if provided.

3.  **Sensitivity Analysis Orchestrator (`run_sensitivity_analysis.py`):**
    *   This script is designed to automate multiple runs of `main.py`.
    *   It reads `sensitivity_config.json` to understand which parameters to vary and by how much.
    *   For each variation, it dynamically creates a temporary scenario JSON file.
    *   It then calls `main.py` for each of these scenarios, passing the temporary scenario file and a unique `scenario_id`. This allows you to run a full sensitivity analysis and have all results tagged in MongoDB.

4.  **3-Way Financials (`run_three_way_model.py`):**
    *   This is a separate script that takes the *output* of `main.py` (specifically, the `final_cash_flow` data stored in MongoDB).
    *   It uses `calculations/three_way_financials.py` to generate the P&L, Cash Flow Statement, and Balance Sheet.
    *   These generated financial statements are then saved to separate collections in your MongoDB database.

### How to Run a Single Case:

If you just want to run a single case (not a full sensitivity analysis), you have two main options:

1.  **Run the Default Base Case:**
    *   Simply execute `main.py` without any arguments:
        ```bash
        python main.py
        ```
    *   This will run the model using the input data as-is from your `inputs/` directory, without any scenario overrides. The results will be saved to MongoDB without a specific `scenario_id` tag (or with a `None` tag, depending on how you query).

2.  **Run a Specific Single Scenario:**
    *   If you want to run `main.py` with specific overrides (e.g., a 10% increase in electricity price, or a specific CAPEX adjustment), you first need to create a JSON file defining that scenario.
    *   **Example `my_single_scenario.json`:**
        ```json
        {
            "scenario_name": "My Custom Single Run",
            "overrides": {
                "global_electricity_price_adjustment_per_mwh": 5.0,
                "global_capex_multiplier": 1.10
            }
        }
        ```
    *   Then, run `main.py` and pass this scenario file using the `--scenario` argument. You can also provide a `scenario_id` to tag the results in MongoDB:
        ```bash
        python main.py --scenario C:/Projects/backend-renew/my_single_scenario.json --scenario_id "my_custom_run_v1"
        ```
    *   This will run `main.py` once, applying only the overrides specified in `my_single_scenario.json`, and tag the results in MongoDB with "my_custom_run_v1".

This setup provides flexibility for both single runs and automated sensitivity analyses.