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
from uft8_converter_noheader import File_utf8_Preprocessor
from logger_setup import LoggerSetup

# logger setup
Logger_LND_L0_WAREHOUSE_INVENTORY_EBS_DAILY = LoggerSetup(app_name='LND_L0_CP_STATUS_APT',log_level=logging.ERROR)
Logger_LND_L0_WAREHOUSE_INVENTORY_EBS_DAILY.configure_logging()


# datetime object containing current date and time
Date = (date.today() - timedelta(days=0)).strftime('%Y%m%d')
# Get the current date and time
now = datetime.now()
# Convert the datetime object to a string in the format 'YYYY-MM-DD HH:MM:SS'
now_string = now.strftime('%Y-%m-%d %H:%M:%S')


src_path = '/elt/data_bucket/Landing/CP_Status'
data_bucket_path = '/elt/data_bucket/CP_Status'
db_name = 'INT_SCO_DB'
db_lnd_schema_name = 'LANDING'


    
for filename in os.listdir(src_path):
    file_path =  os.path.join(src_path,filename)
    data_bucket_file_path =  os.path.join(data_bucket_path,filename)
    print(filename)
    try:
        #connect to snowflake
        ctx = get_snowflake_conn(db_name,db_lnd_schema_name)
        cs = ctx.cursor()
        logging.info('SF connected')  # This won't be logged due to logging level
        print('SF_Connected')
        # load CSV data into DataFrame, convert it to utf-8
        txt_CP_Status=File_utf8_Preprocessor(file_path,'|')
        txt_CP_Status.read_and_convert()
        df=txt_CP_Status.get_dataframe()
        df['INSERT_DATETIME'] = str(datetime.fromtimestamp(os.path.getmtime(file_path)))
        df['INSERT_FN'] = filename
        df.astype(str)
        df.columns = ['PIN_NUMBER',
                    'DELIVERY_DATE',
                    'DELIVERY_ITEM_SIGNATORY',
                    'CPC_ORGANIZATION_NUMBER',
                    'CPC_ORGANIZATION_NAME',
                    'CPC_ORGANIZATION_ADDRESS_1',
                    'CPC_ORGANIZATION_ADDRESS_2',
                    'CPC_ORGANIZATION_CITY',
                    'CPC_ORGANIZATION_PROVINCE',
                    'CPC_ORGANIZATION_COUNTRY',
                    'CPC_ORGANIZATION_POSTAL_CODE',
                    'EVENT_NUMBER',
                    'EVENT_DATE',
                    'EVENT_TIME',
                    'EVENT_TIME_ZONE',
                    'REFERENCE_1',
                    'REFERENCE_2',
                    'MANIFEST_NUMBER',
                    'PRODUCT',
                    'WEIGHT',
                    'SHIP_TO_NAME',
                    'SHIP_TO_POSTAL_CODE_',
                    'MSS_EVENT_DESCRIPTION_FRENCH',
                    'MSS_EVENT_DESCRIPTION_ENGLISH',
                    'COD_PIN_NUMBER',
                    'COD_REMITTANCE',
                    'ORIGINAL_PIN',
                    'ANTICIPATED_RETURN_PIN',
                    'DIRECT_TO_RETAIL_FLAG',
                    'WORK_CENTRE',
                    'DNC_NUMBER',
                    'RETURN_POLICY_ID',
                    'ADDITIONAL_ORDER_INFO',
                    'RECEIVER_ADDRESS_LINE_1',
                    'RECEIVER_ADDRESS_LINE_2',
                    'RECEIVER_CITY',
                    'RECEIVER_PROVINCE',
                    'RECEIVER_COUNTRY',
                    'RECEIVER_EMAIL',
                    'EXPECTED_DELIVERY',
                    'RETURN_FLAG',
                    'DECLARED_HEIGHT',
                    'DECLARED_WIDTH',
                    'DECLARED_LENGTH',
                    'ACTUAL_HEIGHT',
                    'ACTUAL_WIDTH',
                    'ACTUAL_LENGTH',
                    'SIGNATURE_REQUIRED',
                    'INSURED_VALUE',
                    'COD_AMOUNT',
                    'PROOF_OF_AGE',
                    'CARD_FOR_PICKUP',
                    'DO_NOT_SAFE_DROP',
                    'LEAVE_AT_DOOR',
                    'DELIVER_TO_POST_OFFICE',
                    'LABEL_CORRECTION_REASON_ENGLISH',
                    'LABEL_CORRECTION_REASON_FRENCH',
                    'DECLARED_DESTINATION_POSTAL_CODE',
                    'CORRECTED_POSTAL_CODE',
                    'CUSTOMS_AMOUNT',
                    'CUSTOMER_NAME_',
                    'FUNDS_COLLECTED',
                    'PHOTO_CONFIRMATION',
                    'INSERT_DATETIME',
                    'INSERT_FN'
                    ]

        
        # write DataFrame to a staging table
        staging_table = 'STAGE_L0_CP_STATUS_APT'
        target_table = 'L0_CP_STATUS_APT'
        schema_name = db_lnd_schema_name
        database_name = db_name

        success, nchunks, nrows, _ = write_pandas(ctx, df, f"{staging_table}")
        logging.info(f'DataFrame written to staging table with {nrows} rows in {nchunks} chunks.')  # This won't be logged
        
        # fetch column names dynamically
        columns = fetch_table_columns(cs, target_table, schema_name, database_name)
        logging.info('Fetch column names operation successful.')  # This won't be logged
        print('Fetch column names operation successful.')
        
        # hardcode primay key
        key_columns = ['INSERT_FN', 'PIN_NUMBER', 'EVENT_DATE', 'EVENT_TIME', 'REFERENCE_1' ]
        
        # genertae merge sql
        merge_sql = generate_merge_sql(f"{schema_name}.{staging_table}", f"{schema_name}.{target_table}", key_columns, columns)
        
        # Start a Transcation
        cs.execute("BEGIN;")
        
        # execute merge sql
        cs.execute(merge_sql)
        cs.execute("COMMIT;")
        logging.info('MERGE operation successful.')  # This won't be logged
        print('MERGE operation successful.')
        
        # Start a Transcation
        cs.execute("BEGIN;")        
         
        # execute turncate stage table
        cs.execute(f"TRUNCATE TABLE {database_name}.{schema_name}.{staging_table};")
        cs.execute("COMMIT;")
        logging.info('Staging table truncated successfully.')  # This won't be logged
        print('Staging table truncated successfully.')
      
        # execute run task to load to intergation
        #cs.execute(f"EXECUTE TASK {database_name}.INTEGRATION.LOAD_IINT_L0_CP_STATUS_APT;")
        #logging.info('run task to load to intergation successfully.')  # This won't be logged
        #print('run task to load to intergation successfully.')    
        
        # execute turncate stage table
        # cs.execute(f"TRUNCATE TABLE {database_name}.INTEGRATION.L1_EBS_WH_INV_DUMMY;")
        #logging.info('Staging table truncated successfully.')  # This won't be logged
        #print('Staging table truncated successfully.')
        
        # execute run task to load to intergation L1 Table
        #cs.execute(f"EXECUTE TASK {database_name}.INTEGRATION.LOAD_INT_L1_EBS_WH_INV_DUMMY;")
        #logging.info('run task to Truncate and load to intergation L1 Table successfully.')  # This won't be logged
        #print('run task to Truncate and load to intergation L1 Table successfully.')  
        
        shutil.move(file_path,data_bucket_file_path)
        #logging.info('file moves from landning to databucket')  # This won't be logged
        print('file moves from landning to databucket')     
        
        
    except Exception as e:
        logging.error(f'An error occurred: {e}', exc_info=True)

    finally:
        logging.info('Cleaning up resources...')  # This won't be logged
        if ctx is not None:
            ctx.close()
                    
#if __name__ == "__main__":
#    snowflake_operations()

#connect to snowflake
ctx = get_snowflake_conn(db_name,db_lnd_schema_name)
cs = ctx.cursor()
logging.info('SF connected')  # This won't be logged due to logging level
print('SF_Connected')

# execute run task to load to intergation
cs.execute(f"EXECUTE TASK {database_name}.INTEGRATION.LOAD_IINT_L0_CP_STATUS_APT;")
logging.info('run task to load to intergation successfully.')  # This won't be logged
print('run task to load to intergation successfully.')    
