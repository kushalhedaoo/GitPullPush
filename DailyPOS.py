from google.cloud import bigquery
import pandas as pd
from google.cloud import storage
from config import *
import os
import logging,sys,gzip,os,threading,re
import numpy as np
from datetime import datetime
from Common.LOG_FILE_CHECKER import LogFileCheck
from Common.BucketConnection import *
from Common.Archive_Reject import *


project_id = PROJECT_ID
# Set your GCP bucket name
bucket_name = POS_BUCKET_NAME
# Set your BigQuery dataset and table names
dataset_id = STG_DATASET_ID
table_id = POS_TABLE_ID
temp_table_id = POS_TEMP_TABLE_ID
df_column_names = DF_COLUMN_NAMES
bq_table_schema = BQ_TABLE_SCHEMA
prefix = POS_PREFIX
csv_path_regex = prefix
archive_path = ARCHIVE_PATH_REGEX
rejected_path = REJECTED_PATH_REGEX
task_name = 'DAILYPOS'

gcs_object = gcs_connection(logging,project_id ,bucket_name)

log_checker = LogFileCheck(gcs_object,logs_folder_name='pos_logs')
# storage_client=storage.Client(project=project_id)
# bucket=storage_client.bucket(bucket_name=bucket_name)
# bucket = storage_client.get_bucket(bucket_name)
bq_client = bigquery.Client(project=project_id)

# Define temporary and target table names
temp_table_name = f'{project_id}.{dataset_id}.{temp_table_id}'
target_table_name = f'{project_id}.{dataset_id}.{table_id}'

def final_row_count(project_id, dataset_id, table_id):
    # Initialize a BigQuery client
    client = bigquery.Client(project=project_id)

    # Construct a reference to the table
    table_ref = client.dataset(dataset_id).table(table_id)

    # Fetch the top 10 rows of table data
    query = f"SELECT count(*) from `{project_id}.{dataset_id}.{table_id}`"
    query_job = client.query(query)
    rows = query_job.result()
    row_count=0
    print("\nData:")
    for row in rows:
        row_data = [cell for cell in row]
        row_count= row_data[0]

    return row_count


def Create_Target_Table_Schema(bq_table_schema):
    column_expressions = []

    for col in bq_table_schema:
        col_name = col["name"]
        col_type = col["type"]

        if col_type.upper() == "DATE":
            # print(f"Handling {col_name} as DATE")
            column_expressions.append(f'DATE(`{col_name}`) AS `{col_name}`')
        elif col_type.upper() == "INTEGER":
            # print(f"Handling {col_name} as INTEGER")
            column_expressions.append(f'CAST(`{col_name}` AS INT64) AS `{col_name}`')
        elif col_type.upper() == "NUMERIC":
            # print(f"Handling {col_name} as NUMERIC")
            column_expressions.append(f'CAST(`{col_name}` AS NUMERIC) AS `{col_name}`')
        elif col_type.upper() == "STRING":
            # print(f"Handling {col_name} as STRING")
            column_expressions.append(f'CAST(`{col_name}` AS STRING) AS `{col_name}`')
        else:
            print(f"Unhandled data type for {col_name}: {col_type}")
    target_table_schema = ', '.join(column_expressions)
    return target_table_schema


def pos_proc_call(filename,FileLocation,bq_client,AppendReplaceFlag):
    try:
        print(f'filename: {filename}')
        start_time = datetime.now()  
        CustomerNumber=filename.split('_')[0]

        print(f'{POS_PROC_DB}.{POS_PROC_NAME} CALL')
        query = f'''CALL {POS_PROC_DB}.{POS_PROC_NAME}()'''

        query_job = bq_client.query(query)
        query_job.result()
        end_time = datetime.now()
        ErrorMessage = 'Procedure Executed Successfully'
        
        batch_proc_call(bq_client,CustomerNumber, filename, FileLocation, ErrorMessage, 
        AppendReplaceFlag, start_time, end_time)
        print(f'{POS_PROC_DB}.{POS_PROC_NAME} DONE')
        print(f"SQL Query: {query}")

        
    except Exception as e:
        ErrorMessage = f'Error in {POS_PROC_DB}.{POS_PROC_NAME} : {e}'
        batch_proc_call(bq_client,CustomerNumber, filename, FileLocation, ErrorMessage, 
        AppendReplaceFlag, start_time, end_time)

   

def rem_proc_call(filename,FileLocation,bq_client,AppendReplaceFlag):
    try:
        print(f'filename: {filename}')
        start_time = datetime.now()  
        end_time = datetime.now()
        print("Endtime after start_time",end_time)
        CustomerNumber=filename.split('_')[0]

        print(f'{POS_PROC_DB}.{REM_PROC_NAME} CALL')
        query = f'''CALL {POS_PROC_DB}.{REM_PROC_NAME}("{CustomerNumber}")'''
        
        query_job = bq_client.query(query)
        query_job.result()
        end_time = datetime.now()
        ErrorMessage = 'Procedure Executed Successfully'
        print("End time after procedure calling  :",end_time)
        batch_proc_call(bq_client,CustomerNumber, filename, FileLocation, ErrorMessage, 
        AppendReplaceFlag, start_time, end_time)
        print(f'{POS_PROC_DB}.{REM_PROC_NAME} DONE')
    
    except Exception as ex:
        ErrorMessage = f'Error in {POS_PROC_DB}.{REM_PROC_NAME} : {ex}'
        batch_proc_call(bq_client,CustomerNumber, filename, FileLocation, ErrorMessage, 
        AppendReplaceFlag, start_time, end_time)
        

def batch_proc_call(bq_client,CustomerNumber, filename, FileLocation, ErrorMessage, 
        AppendReplaceFlag, start_time, end_time):
    
    print(f'{BATCH_PROC_DATASET}.{BATCH_PROC} CALL')
    query = f'''CALL {BATCH_PROC_DATASET}.{BATCH_PROC}(
        "{CustomerNumber}", "{filename}", "{FileLocation}", "{ErrorMessage}",
        "{AppendReplaceFlag}", "{start_time}", "{end_time}")''' 
    print(query)
    query_job = bq_client.query(query)
    query_job.result()
    print(f'{BATCH_PROC_DATASET}.{BATCH_PROC} DONE')


def check_and_call_procedure(filename,FileLocation,bq_client):
    # Check if the filename contains '_replace'
    print("The filename is: ",filename)
    if '_replace' not in filename:       
        AppendReplaceFlag = 'False' 
        pos_proc_call(filename,FileLocation,bq_client,AppendReplaceFlag)
    else:
        AppendReplaceFlag = 'True'
        rem_proc_call(filename,FileLocation,bq_client,AppendReplaceFlag)
        pos_proc_call(filename,FileLocation,bq_client,AppendReplaceFlag)
        
# try:
if __name__ == "__main__":
    # Truncate table before inserting data into table
    sql_query = f"""
        TRUNCATE TABLE `{target_table_name}`
    """


    file_sizes = {os.path.basename(file.name): file.size for file in gcs_object.list_blobs(prefix=prefix) if file.name.endswith('.csv')}
    print(f'file_size_dict: {file_sizes}')
         

    # blobs = gcs_object.list_blobs(prefix=prefix)
    blobs = gcs_object.list_blobs(prefix=prefix)
    gcs_file_list = [os.path.basename(file.name) for file in blobs if file.name.endswith('.csv')]
    print("list 1: ",blobs,"\nList2:",gcs_file_list,"\n")
    # gcs_file_list3 = [file for file in gcs_file_list2]

    # # file_names = [blob.name for blob in blobs if blob.name.lower().endswith('.csv')]
    # print(file_names)

    print("Len of :",len(gcs_file_list))
    if len(gcs_file_list) > 0:
        
        for filename in gcs_file_list:
            try:
                print("Blob is : ",filename)

                if filename.lower().endswith('.csv') and file_sizes.get(filename) > 0:
                    
                    log_checker.log_file_exist('CUSTOMER_PRODUCT',gcs_file_list)

                    FileLocation = f'gs://{bucket_name}/{prefix}{filename}'
                    print("Filelocation :",FileLocation)
                    # filename=blob.name.split('/')[-1]
                    df = pd.read_csv(FileLocation)
                    df_to_bq_mapping = {df_column_names[i]: bq_table_schema[i]['name'] for i in range(len(df_column_names))}
                    df=df[df_column_names]
                    df = df.rename(columns=df_to_bq_mapping)
                    new_dict = {}
                    for item in bq_table_schema:
                        name = item['name']
                        data_type = item['type']
                        new_dict[name] = data_type
                    missing_columns = set(new_dict.keys()) - set(df.columns)
                    for column in missing_columns:
                        df[column] = np.nan

                    csv_count=df.shape[0]
                    # Create a temporary table with all columns as STRING
                    df.to_gbq(temp_table_name, project_id=project_id, if_exists='replace')
                    print(f'Temporary table created name : {temp_table_name} for file name : {filename}')
                    # print_table_data(project_id, dataset_id, temp_table_id)

                    # Truncate DailyPOS
                    query_job = bq_client.query(sql_query)
                    query_job.result()


                    print(f'Table name {table_id} truncated. Before Inserting data')

                    #Inserting data from temp table to target table.
                    target_table_schema = Create_Target_Table_Schema(bq_table_schema)
                    # print(target_table_schema)
                    target_table_sql = f"""
                        INSERT INTO {target_table_name} 
                        SELECT
                            {target_table_schema}
                        FROM
                            {temp_table_name}
                    """
                    bq_client.query(target_table_sql).result()
                    print(f'Data loaded into Stage table name : {target_table_name} from Temporary table name : {temp_table_name} for file name : {filename}')
                    row_count=final_row_count(project_id, dataset_id, table_id)

                    if row_count == csv_count:
                        print("Row count for csv",csv_count,"Row count for bq",row_count,"  Matched")
                    else:
                        print("Row count for csv",csv_count,"Row count for bq",row_count,"  Not Matched")
                    # Delete the temporary table
                    bq_client.delete_table(temp_table_name, not_found_ok=True)
                    print(f'Temporary table name : {temp_table_name} deleted for file name : {filename}')

                    check_and_call_procedure(filename, FileLocation,bq_client)
                    status = 'success'
                    copy_blob(status,bucket_name,filename,bucket_name,csv_path_regex,archive_path,rejected_path,task_name)

                else:
                    log_checker.log_file_zero_size('DailyPOS')
            except Exception as e:
                print("Error in Dailypos: ",e)
                status='failure'
                copy_blob(status,bucket_name,filename,bucket_name,csv_path_regex,archive_path,rejected_path,task_name)

            

            
    else:
        log_checker.log_file_doesnot_exist('DailyPOS')

