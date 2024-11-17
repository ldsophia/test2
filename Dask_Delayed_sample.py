import os
import glob
import pandas as pd
import dask
from dask import delayed
import dask.dataframe as dd

def extract_csv_data(root_path, domain, model_name, model_version_folder_filter, required_columns):
    """
    Extracts data from CSV files in parallel using Dask Delayed.

    Parameters:
    - root_path (str): The root directory path where data is stored.
    - domain (str): The domain value (e.g., 'Domain1').
    - model_name (str): The model name (e.g., 'model_a').
    - model_version_folder_filter (str): 'all' or a specific model version folder name (e.g., 'model_version1').
    - required_columns (dict): A mapping of column names to their data types.

    Returns:
    - pandas.DataFrame: The concatenated data from the CSV files.
    """
    # Construct the path pattern to search for files
    version_pattern = '*' if model_version_folder_filter == 'all' else model_version_folder_filter

    search_pattern = os.path.join(
        root_path,
        domain,
        model_name,
        version_pattern,
        '*',  # Date folders
        'raw_data*.csv'
    )

    # Get list of files matching the pattern
    files = glob.glob(search_pattern, recursive=True)

    if not files:
        return pd.DataFrame(columns=required_columns.keys())

    # Define a function to read and process a single file
    def read_and_process(file):
        try:
            df = pd.read_csv(file, dtype=str)
            # Ensure required columns are present
            for col, dtype in required_columns.items():
                if col not in df.columns:
                    default_value = {
                        'int': -9999,
                        'float': -9999.0,
                        'date': pd.Timestamp('1900-01-01'),
                        'string': ''
                    }.get(dtype, '')
                    df[col] = default_value
                else:
                    # Convert column to the specified data type
                    if dtype == 'int':
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(-9999).astype('Int64')
                    elif dtype == 'float':
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(-9999.0)
                    elif dtype == 'date':
                        df[col] = pd.to_datetime(df[col], errors='coerce').fillna(pd.Timestamp('1900-01-01'))
                    else:
                        df[col] = df[col].astype(str)
            # Reorder columns
            df = df[list(required_columns.keys())]
            return df
        except Exception as e:
            print(f"Error reading {file}: {e}")
            return pd.DataFrame(columns=required_columns.keys())

    # Create delayed tasks for each file
    delayed_dfs = [delayed(read_and_process)(file) for file in files]

    # Compute all tasks in parallel and concatenate results
    dfs = dask.compute(*delayed_dfs)
    final_df = pd.concat(dfs, ignore_index=True)

    return final_df

# Example usage:
root_path = '/path/to/data'
domain = 'Domain1'
model_name = 'model_a'
model_version_folder_filter = 'all'  # or 'model_version1'
required_columns = {
    'col1': 'int',
    'col2': 'float',
    'col3': 'date',
    'col4': 'string'
}

df = extract_csv_data(root_path, domain, model_name, model_version_folder_filter, required_columns)
print(df)
