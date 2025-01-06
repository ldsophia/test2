import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import glob

# List CSV files
csv_files = glob.glob("data/*.csv")

# Function to read a single CSV
def read_csv(file):
    return pd.read_csv(file)

# Use multithreading to read files in parallel
with ThreadPoolExecutor() as executor:
    dfs = list(executor.map(read_csv, csv_files))

# Concatenate dataframes
df = pd.concat(dfs, ignore_index=True)

print(df.head())
