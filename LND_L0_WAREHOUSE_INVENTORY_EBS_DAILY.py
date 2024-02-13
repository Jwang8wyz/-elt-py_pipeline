import os
from datetime import datetime, timedelta
import logging

# Create a directory for logs based on the current date
current_date = datetime.now().strftime("%Y-%m-%d")
log_directory = f'/elt/logs/jwang_app_log/{current_date}' 
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

# Configure logging to create a dynamic file name including the current date
log_file_name = f'{log_directory}/LND_L0_WAREHOUSE_INVENTORY_EBS_DAILY_{current_date}.log'
logging.basicConfig(level=logging.ERROR,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(log_file_name),
                              logging.StreamHandler()])



import time
import pandas as pd
import sys
from snowflake.connector.pandas_tools import write_pandas
# custom package is in '/elt/PyPkg/'
sys.path.append('/elt/PyPkg/')
# Import the custom utility functions
from INT_SCO_DB_Connection import get_snowflake_conn
from Merge_def_tool import fetch_table_columns, generate_merge_sql

#def snowflake_operations(**kwargs):
try:
    ctx = get_snowflake_conn()
    cs = ctx.cursor()
    logging.info('SF connected')  # This won't be logged due to logging level
    
    # Load CSV data into DataFrame
    file_path = '/mnt/Reports/FreedomSFTP/GW_Inv/csv_inventory_availability.csv'
    df = pd.read_csv(file_path, delimiter="|", encoding='unicode_escape')
    df['INSERT_DATETIME'] = str(datetime.fromtimestamp(os.path.getmtime(file_path)))
    
    # Write DataFrame to a staging table
    staging_table = 'STAGE_L0_WAREHOUSE_INVENTORY_EBS_DAILY'
    target_table = 'L0_WAREHOUSE_INVENTORY_EBS_DAILY'
    schema_name = 'LANDING'
    database_name = 'INT_SCO_DB'

    success, nchunks, nrows, _ = write_pandas(ctx, df, f"{staging_table}")
    logging.info(f'DataFrame written to staging table with {nrows} rows in {nchunks} chunks.')  # This won't be logged
    
    # Fetch column names dynamically
    columns = fetch_table_columns(cs, target_table, schema_name, database_name)
    
    # Hardcode primay key
    key_columns = ['ORG', 'SKU', 'INSERT_DATETIME']
    
    # genertae merge sql
    merge_sql = generate_merge_sql(f"{schema_name}.{staging_table}", f"{schema_name}.{target_table}", key_columns, columns)
    
    # execute merge sql
    cs.execute(merge_sql)
    logging.info('MERGE operation successful.')  # This won't be logged
     
    # execute turncate stage table
    cs.execute(f"TRUNCATE TABLE {database_name}.{schema_name}.{staging_table};")
    logging.info('Staging table truncated successfully.')  # This won't be logged

except Exception as e:
    logging.error(f'An error occurred: {e}', exc_info=True)

finally:
    logging.info('Cleaning up resources...')  # This won't be logged
    if ctx is not None:
        ctx.close()
            
#if __name__ == "__main__":
#    snowflake_operations()