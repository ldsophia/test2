import asyncio
import csv
import jaydebeapi
import pandas as pd
from datetime import datetime, timedelta
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database connection parameters
username = 'username'
password = 'password'
hostname = 'hostname'
port = 'port'
database = 'database'
jarfile = '/path/to/ojdbc8.jar'
driver = 'oracle.jdbc.driver.OracleDriver'
url = f'jdbc:oracle:thin:@{hostname}:{port}:{database}'

# Async function to extract data
async def extract_data(table_name, query_sql, start_date, end_date):
    conn = jaydebeapi.connect(driver, url, [username, password], jarfile)
    curs = conn.cursor()

    try:
        # Adjust the query to include the date range
        query_sql = f"{query_sql} WHERE DATE_COLUMN BETWEEN '{start_date}' AND '{end_date}'"

        # Execute the query
        curs.execute(query_sql)

        # Fetch the results
        results = curs.fetchall()

        # Fetch the column names
        column_names = [desc[0] for desc in curs.description]

        # Write the results to a CSV file
        with open(f'{table_name}_{start_date}_{end_date}.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(column_names)
            writer.writerows(results)

        logging.info(f"Data extraction for {table_name} from {start_date} to {end_date} completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        # Close the cursor and connection
        curs.close()
        conn.close()

# Async function to run tasks on a weekly basis
async def run_weekly_tasks(table_name, query_sql, start_date, end_date):
    current_date = start_date

    while current_date <= end_date:
        next_date = current_date + timedelta(days=7)

        # Run the extraction task
        await extract_data(table_name, query_sql, current_date, next_date)

        current_date = next_date

# Main function
def main():
    table_name = 'TABLE_NAME'
    query_sql = 'SELECT * FROM TABLE_NAME'
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 12, 31)

    # Run the weekly tasks
    asyncio.run(run_weekly_tasks(table_name, query_sql, start_date, end_date))

if __name__ == "__main__":
    main()
