from google.cloud import storage,bigquery
from google.cloud.exceptions import NotFound
import logging,sys,gzip,os,threading,re
from datetime import datetime
from config import *
import pandas as pd
from Common.LOG_FILE_CHECKER import LogFileCheck
from Common.BucketConnection import *
from Common.Archive_Reject import *



def create_temp_table(bq_client, gcs_file_path: str):
    
    table_schema = infer_schema_from_csv(gcs_file_path)
    print('-'*80)
    print(f'schema_inferred : \n {table_schema}')
    print('-'*80)
    table_id = f"{PROJECT_ID}.{STG_DATASET_ID}.{CP_TEMP_TABLE}"
    query = f'DROP TABLE IF EXISTS {table_id}'
    query_job = bq_client.query(query)
    query_job.result()
    table_ref = bigquery.Table(table_id, schema=table_schema)
    print('CREATING_TEMP_TABLE')
    bq_client.create_table(table_ref)
    print("TEMP_TABLE_CREATED")
    logging.info(f"Temporary table created successfully with schema: {table_schema}")
    # except Exception as e:
    #     error_message = f"Error creating temporary table: {e}"
    #     log_checker.log_error('CUSTOMER_PRODUCT',error_message)

def infer_schema_from_csv(gcs_file_path: str):
    print('inside_infer_schema')
    
    df = pd.read_csv(gcs_file_path)
    df.columns = [column.replace('crea edon', 'createdon') if column == 'crea edon' else column for column in df.columns]

    schema = []
    for column_name, data_type in zip(df.columns, df.dtypes):
        bq_data_type = convert_pandas_dtype_to_bq(data_type)
        schema.append(bigquery.SchemaField(column_name, bq_data_type))
    print('INFERRED_schema')
    return schema

def convert_pandas_dtype_to_bq(pandas_dtype):
    # Convert pandas data types to BigQuery data types
    if pandas_dtype == 'int64':
        return 'INTEGER'
    elif pandas_dtype == 'float64':
        return 'FLOAT'
    elif pandas_dtype == 'object':
        return 'STRING'
    elif pandas_dtype == 'bool':
        return 'BOOLEAN'
    else:
        return 'STRING'


def temp_to_final_table(bq_client):
    # try:
    print('temp_to_final')
    query = f'''CALL {PROC_DB}.{TEMP_TO_FINAL}()'''
    
    query_job = bq_client.query(query)
    query_job.result()
    print("FINAL_LOAD DONE")
    # except Exception as e:
    #     error_message = f"Error in TEMP_TO_FINAL: {e}"
    #     log_checker.log_error('CUSTOMER_PRODUCT',error_message)

def drop_temp_table(bq_client,dataset_id,table_name):
    # try:
        
    query = f'''DROP TABLE {dataset_id}.{table_name}'''
    
    query_job = bq_client.query(query)
    query_job.result()
    print("TEMP_TABLE DROPPED")
    # except Exception as e:
    #     error_message = f"Error DROPPING TEMP TABLE: {e}"
    #     log_checker.log_error('CUSTOMER_PRODUCT',error_message)


def truncate_final_table(bq_client,dataset_id,table_name):
    # try:
        
    query = f'''TRUNCATE TABLE {dataset_id}.{table_name}'''
    
    query_job = bq_client.query(query)
    query_job.result()
    print("FINAL_TABLE TRUNCATED")
    # except Exception as e:
    #     error_message = f"Error TRUNCATING FINAL TABLE: {e}"
    #     log_checker.log_error('CUSTOMER_PRODUCT',error_message)


def mm_proc_call(bq_client):
    # try:
    print('MM_PROC_CALL')
    query = f'''CALL {MM_PROC_DB}.{MM_PROC_CP}()'''
    
    query_job = bq_client.query(query)
    query_job.result()
    print("MM_PROC_CP DONE")
    # except Exception as e:
    #     error_message = f"Error calling the MM_PROC_CALL: {e}"
    #     log_checker.log_error('CUSTOMER_PRODUCT',error_message)


def upc_proc_call(bq_client):
    # try:
    print('UPC_PROC_CALL')
    query = f'''CALL {UPC_PROC_DB}.{UPC_PROC_CP}()'''
    
    query_job = bq_client.query(query)
    query_job.result()
    print("UPC_PROC_CP DONE")
    # except Exception as e:
    #     error_message = f"Error calling the MM_PROC_CALL: {e}"
    #     log_checker.log_error('CUSTOMER_PRODUCT',error_message)
    
def load_each_csv(logging,bq_client,gcs_file_path:str,table_id:str,header_rows:int):
    logging.info(f"LOADING THE CSV INTO THE BQ funct.: load_each_csv")
    logging.info(f"csv_path : {gcs_file_path}")
    logging.info(f"table_id : {table_id}")

    rows_before_ins = bq_client.get_table(table_id)
    logging.info(f"rows_before_ins : {rows_before_ins.num_rows}") 
    job_config = bigquery.LoadJobConfig(skip_leading_rows=header_rows,source_format=bigquery.SourceFormat.CSV,)
    load_job = bq_client.load_table_from_uri(gcs_file_path, table_id, job_config=job_config)  
    load_job.result() 
    rows_after_ins = bq_client.get_table(table_id)
    logging.info(f"rows_after_ins : {rows_after_ins.num_rows}") 
    rows_inserted = int(rows_after_ins.num_rows)-int(rows_before_ins.num_rows)
    
    print(f"Loaded {rows_inserted} rows into BQ.") 
    logging.info(f"Loaded {rows_inserted} rows into table : {table_id}") 
    logging.info(f"\n----------------------------------------------------------------------------------------------\n") 


def csv_to_BigQuery(logging,project_id:str,dataset_id:str,table_name:str,file_list:list, path:str,header_rows:int,log_checker,bucket_name):
    sucess_list,excpt_lis= [],{} 
    bq_client = bigquery.Client()
    table_id = f'{project_id}.{dataset_id}.{table_name}'
    final_table_name = CP_FINAL_TABLE
    csv_path_regex = CP_CSV_PATH_REGEX
    task_name = 'CUSTOMER_PRODUCT'
    archive_path = ARCHIVE_PATH_REGEX
    rejected_path = REJECTED_PATH_REGEX

 
    
    for file_name in file_list:
        logging.info(f"\n\n----------------------------------------------------------------------------------------------\n\n") 
        error_flg = False
        
        try:     
            if file_name != '':
                print('!file_name:',file_name)
                logging.info(f"Loading file : {file_name} in the Table {table_id}")
                gcs_file_path = os.path.join(path,file_name)
                print(f"gcs_file_path : {gcs_file_path}")
                create_temp_table(bq_client,gcs_file_path)
                load_each_csv(logging,bq_client,gcs_file_path,table_id,header_rows)
                temp_to_final_table(bq_client)
                mm_proc_call(bq_client)
                upc_proc_call(bq_client)
                drop_temp_table(bq_client,dataset_id,table_name)
                status = 'success'
                copy_blob(status,bucket_name,file_name,bucket_name,csv_path_regex,archive_path,rejected_path,task_name)
                # truncate_final_table(bq_client,dataset_id,final_table_name,log_checker)
                sucess_list.append(file_name)
                


        except Exception as e:
            error_message = f'Exception in csv_to_BigQuery function : {e}'
            excpt_lis[file_name] = e
            log_checker.log_error('CUSTOMER_PRODUCT',error_message)
            status = 'Failure'
            copy_blob(status,bucket_name,file_name,bucket_name,csv_path_regex,archive_path,rejected_path,task_name)
            logging.info(f"Exception : {e}")


    print('The list of files which are loaded : ' , sucess_list)
    logging.info(f'The list of files which are loaded successfully: : {sucess_list}')
    print('The list of files which are not loaded : ', excpt_lis)
    logging.info(f'The list of files which are failed : {excpt_lis}')

      
def main():
    try: 
        
        project_id=PROJECT_ID
        print('project_id:'+project_id)
        
        dataset_id=STG_DATASET_ID
       
        path = CP_CSV_PATH
        print('load_file_csv_path : '+ path)
       
    

        logging.basicConfig(filename=f'BQ_Load_Utility_{datetime.now().strftime("%Y_%m_%d_%H_%M_%S")}.log', level=logging.INFO)
        bucket_name = CP_BUCKET_NAME
        print('bucket_name : '+ bucket_name)

        gcs_object = gcs_connection(logging,project_id ,bucket_name)
        log_checker = LogFileCheck(gcs_object, logs_folder_name='custom_logs')
        

        file_sizes = {os.path.basename(file.name): file.size for file in gcs_object.list_blobs(prefix=CP_CSV_PATH_REGEX) if file.name.endswith('.csv')}
        print(f'file_size_dict: {file_sizes}')
         
        
            
        thread = []
        table_dict = {}     
        
        gcs_file_list = list(gcs_object.list_blobs(prefix=CP_CSV_PATH_REGEX))
        
        gcs_file_list = [os.path.basename(file.name) for file in gcs_file_list]
        
        gcs_file_list = [file for file in gcs_file_list if file.endswith('.csv')]
        
        if len(gcs_file_list) > 0:

            if all(file_sizes.get(file, 0) > 0 for file in gcs_file_list):
                
                log_checker.log_file_exist('CUSTOMER_PRODUCT',gcs_file_list)

                print(f"gcs_file_list : {gcs_file_list}")
                logging.info(f'gcs_file_list : {gcs_file_list}')


                table_dict[CP_TEMP_TABLE] = gcs_file_list
                print(f'table_dict: {table_dict}')
                for table_name,file_list in table_dict.items():
                    thread.append(threading.Thread(target=csv_to_BigQuery, args=(logging,project_id,dataset_id,table_name,file_list,path,1,log_checker,bucket_name)))
                    thread[-1].start()
                    thread[-1].name = table_name
                    logging.info(f'Starting the Thread for the table : {thread[-1].name}')
                    print(f'Starting the Thread for the table : {thread[-1].name}')
                    print(f'Starting the process for the table : {table_name}')
                    err_message = 'STEP1 and STEP2 Executed Successfully'
                    log_checker.log_error('CUSTOMER_PRODUCT',err_message)
            else:
                
                log_checker.log_file_zero_size('CUSTOMER_PRODUCT')


        else:
            log_checker.log_file_doesnot_exist('CUSTOMER_PRODUCT')
            
           
    except Exception as ex:
        error_message = f"An error occurred: {str(ex)}"
        log_checker.log_error('CUSTOMER_PRODUCT',error_message)
                      
if __name__ == "__main__":
    main()


