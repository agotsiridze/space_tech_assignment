import pandas as pd


# import utils
import os


def extract_data(file_path: str) -> pd.DataFrame:
    """Extracts data from a CSV file."""
    try:
        norm_path = os.path.normpath(file_path)
        return pd.read_csv(norm_path)
    # Handle exception if any of the files are missing
    except FileNotFoundError as e:
        print(f"Error: {e}")
    # Handle any other exceptions
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    pass
