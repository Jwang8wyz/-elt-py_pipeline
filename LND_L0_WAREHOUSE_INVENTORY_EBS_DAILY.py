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
from sf_conn_cls import VLFMSnowflakeConnection
from Merge_def_tool import fetch_table_columns, generate_merge_sql



try:
    # Initialize the connection
    user = os.environ.get('SF_Connection_proc_acct')
    schema_name = 'LANDING'
    database_name = 'INT_SCO_DB'
    LND_conn_details = {
        "user": user,
        "account": 'videotron-freedommobile',
        "private_key_path_env_var": 'SF_Connection_PRIVATE_KEY_PATH',
        "private_key_passphrase_env_var": 'SF_Connection_PRIVATE_KEY_PASSPHRASE',
        "database": database_name,
        "schema": schema_name
    }

    # Create a SnowflakeConnection instance and use it
    LND_snowflake_conn = VLFMSnowflakeConnection(**LND_conn_details)
    ctx = LND_snowflake_conn.connect()
    cs = ctx.cursor()
    logging.info('SF connected')
    print('SF connected')
    

    # Use `ctx` as needed...
    #LND_cs = LND_ctx.cursor()
    #LND_cs.execute("SELECT count(1) FROM INT_SCO_DB.LANDING.L0_WAREHOUSE_INVENTORY_EBS_DAILY;")
    #result = LND_cs.fetch_pandas_all()
    #print(result)
    
    # Load CSV data into DataFrame
    file_path = '/mnt/Reports/FreedomSFTP/GW_Inv/csv_inventory_availability.csv'
    df = pd.read_csv(file_path, delimiter="|", encoding='unicode_escape')
    df['INSERT_DATETIME'] = str(datetime.fromtimestamp(os.path.getmtime(file_path)))
    
    # Write DataFrame to a staging table
    staging_table = 'STAGE_L0_WAREHOUSE_INVENTORY_EBS_DAILY'
    target_table = 'L0_WAREHOUSE_INVENTORY_EBS_DAILY'

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
    
    # Close the connection when done
    LND_snowflake_conn.close()
    logging.info('SF disconnected')
    print('SF disconnected')
    
except Exception as e:
    logging.error(f'An error occurred: {e}', exc_info=True)
