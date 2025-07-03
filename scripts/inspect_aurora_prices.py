import pandas as pd
import os

def inspect_aurora_layout():
    """
    Reads and prints the first 10 rows of the Aurora_May.xlsx file to inspect its layout.
    """
    # Define file paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    raw_inputs_dir = os.path.join(script_dir, '..', 'data', 'raw_inputs')
    aurora_path = os.path.join(raw_inputs_dir, 'Aurora_May.xlsx')

    print(f"Inspecting Aurora data from: {aurora_path}")

    try:
        # Read the first 10 rows of the Excel file
        df_inspect = pd.read_excel(aurora_path, header=None, nrows=10)
        print(df_inspect.to_string())

    except FileNotFoundError:
        print(f"Error: Input file not found at {aurora_path}")

if __name__ == '__main__':
    inspect_aurora_layout()