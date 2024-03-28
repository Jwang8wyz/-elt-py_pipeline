import os
from datetime import datetime, timedelta, date
import logging
import time
import pandas as pd
import sys
from snowflake.connector.pandas_tools import write_pandas
import shutil

# custom package is in '/elt/PyPkg/'
sys.path.append('/elt/PyPkg/')
# Import the custom utility functions
from sf_sco_db_conn import get_snowflake_conn
from merge_def_tool import fetch_table_columns, generate_merge_sql
from utf8_converter import File_utf8_Preprocessor
from logger_setup import LoggerSetup

# logger setup
Logger_LND_L0_WAREHOUSE_INVENTORY_EBS_DAILY = LoggerSetup(app_name='LND_L0_EBS_WH_INV_DUMMY',log_level=logging.ERROR)
Logger_LND_L0_WAREHOUSE_INVENTORY_EBS_DAILY.configure_logging()


# datetime object containing current date and time
Date = (date.today() - timedelta(days=0)).strftime('%Y%m%d')
# Get the current date and time
now = datetime.now()
# Convert the datetime object to a string in the format 'YYYY-MM-DD HH:MM:SS'
now_string = now.strftime('%Y-%m-%d %H:%M:%S')

# Move Shaw Files to 3PL folder
src_path = '/mnt1/JaysonReports/GW_Inv/GW_INV_Dummy_Authentic-'+ Date +'.csv'
dst_path = '/elt/data_bucket/GW_Inv_Dummy/GW_INV_Dummy_Authentic-'+ Date +'.csv'
shutil.copy(src_path, dst_path)

logging.info('File copy to Local /elt/data_bucket/GW_Inv_Dummy successful.')  # This won't be logged
print('File copy to Local /elt/data_bucket/GW_Inv_Dummy successful')

db_name = 'INT_SCO_DB'
db_lnd_schema_name = 'LANDING'

try:
    #connect to snowflake
    ctx = get_snowflake_conn(db_name,db_lnd_schema_name)
    cs = ctx.cursor()
    logging.info('SF connected')  # This won't be logged due to logging level
    print('SF_Connected')
    
    # load CSV data into DataFrame, convert it to utf-8
    file_path = src_path
    csv_inventory_file=File_utf8_Preprocessor(file_path,'|')
    csv_inventory_file.read_and_convert()
    df=csv_inventory_file.get_dataframe()
    # df = pd.read_csv(file_path, delimiter="|", encoding='unicode_escape') # regular way
    df['INSERT_DATETIME'] = str(datetime.fromtimestamp(os.path.getmtime(file_path)))
    
    # write DataFrame to a staging table
    staging_table = 'STAGE_L0_EBS_WH_INV_DUMMY'
    target_table = 'L0_EBS_WH_INV_DUMMY'
    schema_name = db_lnd_schema_name
    database_name = db_name

    success, nchunks, nrows, _ = write_pandas(ctx, df, f"{staging_table}")
    logging.info(f'DataFrame written to staging table with {nrows} rows in {nchunks} chunks.')  # This won't be logged
    
    # fetch column names dynamically
    columns = fetch_table_columns(cs, target_table, schema_name, database_name)
    logging.info('Fetch column names operation successful.')  # This won't be logged
    print('Fetch column names operation successful.')
    
    # hardcode primay key
    key_columns = ['SKU', 'ORG', 'REPORT_DATE']
    
    # genertae merge sql
    merge_sql = generate_merge_sql(f"{schema_name}.{staging_table}", f"{schema_name}.{target_table}", key_columns, columns)
    
    # execute merge sql
    cs.execute(merge_sql)
    logging.info('MERGE operation successful.')  # This won't be logged
    print('MERGE operation successful.')
     
    # execute turncate stage table
    cs.execute(f"TRUNCATE TABLE {database_name}.{schema_name}.{staging_table};")
    logging.info('Staging table truncated successfully.')  # This won't be logged
    print('Staging table truncated successfully.')
  
    # execute run task to load to intergation
    cs.execute(f"EXECUTE TASK {database_name}.INTEGRATION.LOAD_INT_L0_EBS_WH_INV_DUMMY;")
    logging.info('run task to load to intergation successfully.')  # This won't be logged
    print('run task to load to intergation successfully.')    
    
    # execute turncate stage table
    # cs.execute(f"TRUNCATE TABLE {database_name}.INTEGRATION.L1_EBS_WH_INV_DUMMY;")
    #logging.info('Staging table truncated successfully.')  # This won't be logged
    #print('Staging table truncated successfully.')
    
    # execute run task to load to intergation L1 Table
    #cs.execute(f"EXECUTE TASK {database_name}.INTEGRATION.LOAD_INT_L1_EBS_WH_INV_DUMMY;")
    #logging.info('run task to Truncate and load to intergation L1 Table successfully.')  # This won't be logged
    #print('run task to Truncate and load to intergation L1 Table successfully.')  
    
except Exception as e:
    logging.error(f'An error occurred: {e}', exc_info=True)

finally:
    logging.info('Cleaning up resources...')  # This won't be logged
    if ctx is not None:
        ctx.close()
            
#if __name__ == "__main__":
#    snowflake_operations()