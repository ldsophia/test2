import os
import sys
import json
import logging
import traceback
from datetime import datetime
from typing import List, Dict

import pandas as pd
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.compute as pc
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.exceptions import SnowparkException

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_snowflake_session() -> Session:
    """Establishes a Snowpark session with Snowflake."""
    try:
        connection_parameters = {
            'account': os.environ.get('SNOWFLAKE_ACCOUNT'),
            'user': os.environ.get('SNOWFLAKE_USER'),
            'password': os.environ.get('SNOWFLAKE_PASSWORD'),
            'role': os.environ.get('SNOWFLAKE_ROLE'),
            'warehouse': os.environ.get('SNOWFLAKE_WAREHOUSE'),
            'database': os.environ.get('SNOWFLAKE_DATABASE'),
            'schema': os.environ.get('SNOWFLAKE_SCHEMA')
        }
        session = Session.builder.configs(connection_parameters).create()
        logger.info("Snowflake session established.")
        return session
    except Exception as e:
        logger.error("Failed to create Snowflake session.")
        raise e

def query_feedback_data(session: Session, client_id: int, start_date: str, end_date: str, region: str = None) -> pd.DataFrame:
    """Queries the feedback table in Snowflake using Snowpark."""
    try:
        feedback_df = session.table('feedback').filter(
            (col('client_id') == client_id) &
            (col('product_date').between(start_date, end_date))
        )

        if client_id == 2 and region:
            feedback_df = feedback_df.filter(col('region') == region)

        feedback_pandas_df = feedback_df.to_pandas()
        logger.info(f"Queried feedback data: {len(feedback_pandas_df)} records.")
        return feedback_pandas_df
    except SnowparkException as e:
        logger.error("Error querying feedback data from Snowflake.")
        raise e

def get_relevant_date_folders(parent_folder: str, product_dates: List[str]) -> List[str]:
    """Gets the list of relevant date folders matching the product_dates."""
    try:
        if not os.path.exists(parent_folder):
            logger.warning("Parent folder does not exist.")
            return []

        subfolders = [f.name for f in os.scandir(parent_folder) if f.is_dir()]
        if not subfolders:
            logger.info("No subfolders found under the parent folder.")
            return []

        relevant_folders = [os.path.join(parent_folder, date) for date in product_dates if date in subfolders]
        logger.info(f"Found {len(relevant_folders)} relevant date folders.")
        return relevant_folders
    except Exception as e:
        logger.error("Error getting relevant date folders.")
        raise e

def read_history_data(folders: List[str]) -> pa.Table:
    """Reads history data CSV files from the specified folders using PyArrow."""
    try:
        tables = []
        for folder in folders:
            file_path = os.path.join(folder, 'history_data.csv')
            if os.path.exists(file_path):
                table = pv.read_csv(file_path)
                tables.append(table)
                logger.info(f"Read history data from {file_path}")
            else:
                logger.warning(f"File not found: {file_path}")

        if tables:
            combined_table = pa.concat_tables(tables)
            logger.info(f"Combined history data records: {combined_table.num_rows}")
            return combined_table
        else:
            logger.info("No history data files found.")
            return pa.Table.from_pandas(pd.DataFrame())
    except Exception as e:
        logger.error("Error reading history data files.")
        raise e

def left_join_data(history_table: pa.Table, feedback_df: pd.DataFrame, feedback_join_key: Dict[str, str]) -> pd.DataFrame:
    """Performs left join between history data and feedback data using specified join keys."""
    try:
        history_df = history_table.to_pandas()
        feedback_keys = feedback_join_key.get('ALL', None)

        if 'data_level' in history_df.columns:
            # Expand the feedback_join_key for each data_level
            data_levels = history_df['data_level'].unique()
            merged_dfs = []
            for level in data_levels:
                level_keys = feedback_join_key.get(level, feedback_keys)
                if level_keys:
                    level_history_df = history_df[history_df['data_level'] == level]
                    merged_df = perform_join(level_history_df, feedback_df, level_keys.split(','), level)
                    merged_dfs.append(merged_df)
            result_df = pd.concat(merged_dfs, ignore_index=True)
        else:
            if feedback_keys:
                result_df = perform_join(history_df, feedback_df, feedback_keys.split(','), 'ALL')
            else:
                logger.error("No join keys provided for 'ALL' level.")
                return pd.DataFrame()
        logger.info(f"Left join completed. Result records: {len(result_df)}")
        return result_df
    except Exception as e:
        logger.error("Error during left join of data.")
        raise e

def perform_join(history_df: pd.DataFrame, feedback_df: pd.DataFrame, join_keys: List[str], level: str) -> pd.DataFrame:
    """Performs the actual join operation."""
    try:
        join_keys = [key.strip() for key in join_keys]
        # Adding 'anomaly_field' to the join keys
        join_keys.append('anomaly_field')
        merged_df = pd.merge(
            history_df,
            feedback_df,
            on=join_keys,
            how='left',
            suffixes=('', '_fb')
        )

        # Determine data_source_type
        merged_df['data_source_type'] = merged_df.apply(
            lambda row: 'label_data' if not pd.isnull(row['feedback']) else 'history_raw_data',
            axis=1
        )

        # Handling additional_label_data
        additional_feedback = feedback_df[~feedback_df[join_keys].isin(history_df[join_keys])]
        if not additional_feedback.empty:
            additional_feedback['data_source_type'] = 'additional_label_data'
            merged_df = pd.concat([merged_df, additional_feedback], ignore_index=True)

        return merged_df
    except Exception as e:
        logger.error(f"Error performing join for level {level}.")
        raise e

def apply_label_logic(df: pd.DataFrame) -> pd.DataFrame:
    """Applies logic to compute the 'label' column based on conditions."""
    try:
        def compute_label(row):
            if row['data_source_type'] == 'label_data':
                if row.get('anomaly_field') == 1 and row.get('feedback') == 'accept':
                    return 1
                elif row.get('feedback') == 'reject':
                    is_anomaly = row.get('is_anomaly', 0)
                    return 1 - is_anomaly
            return None

        df['label'] = df.apply(compute_label, axis=1)
        return df
    except Exception as e:
        logger.error("Error applying label logic.")
        raise e

def main():
    try:
        # Example input parameters
        client_id = int(os.environ.get('CLIENT_ID', 1))
        start_date = os.environ.get('START_DATE', '2021-01-01')
        end_date = os.environ.get('END_DATE', '2021-12-31')
        region = os.environ.get('REGION', None)
        parent_folder = os.environ.get('PARENT_FOLDER', '/data/history')
        feedback_join_key_json = os.environ.get('FEEDBACK_JOIN_KEY', '{"ALL": "security_id"}')
        feedback_join_key = json.loads(feedback_join_key_json)

        # Step 1: Query feedback data
        session = get_snowflake_session()
        feedback_df = query_feedback_data(session, client_id, start_date, end_date, region)

        # Step 2: Extract distinct product_date values
        product_dates = feedback_df['product_date'].dropna().unique().tolist()

        # Step 3: Get relevant date folders
        relevant_folders = get_relevant_date_folders(parent_folder, product_dates)
        if not relevant_folders:
            logger.info("No relevant date folders found. Exiting.")
            return

        # Step 4: Read history data
        history_table = read_history_data(relevant_folders)
        if history_table.num_rows == 0:
            logger.info("No history data found. Exiting.")
            return

        # Step 5: Left join data
        result_df = left_join_data(history_table, feedback_df, feedback_join_key)

        # Step 6: Apply label logic
        result_df = apply_label_logic(result_df)

        # Step 7: Convert to pandas DataFrame if necessary
        output_df = result_df  # Already in pandas DataFrame

        # Output the result or save to file/database as needed
        logger.info("Processing completed successfully.")
        # For example, save to CSV
        output_df.to_csv('output_data.csv', index=False)

    except Exception as e:
        logger.error("An error occurred during processing.")
        logger.error(traceback.format_exc())

if __name__ == '__main__':
    main()
