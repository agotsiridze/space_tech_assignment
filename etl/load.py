import pandas as pd
import os


def load_data(df: pd.DataFrame, output_file_path: str):
    """Loads the data into a CSV file."""
    try:
        file_path = os.path.normpath(output_file_path)
        df.to_csv(file_path, index=False)
        print(f"Data successfully written to {file_path}")
    except Exception as e:
        print(f"Error writing to file: {e}")
