
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import multiprocessing
import os
import configparser
import logging
from google.cloud import bigquery
import smtplib
from datetime import date, datetime, timedelta
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago
import pandas as pd
from airflow.operators.dagrun_operator import TriggerDagRunOperator





# Variable declaration


python_scripts_path_CUSTOMER_PROD = '/home/airflow/gcs/data/File_check_on_bucket/'
python_scripts_path_DAILYPOS = '/home/airflow/gcs/data/File_check_on_bucket/' 
CUSTOMER_PROD_PYTHON = 'customer_prod.py' # This file will convert Excel file into a csv format.
DAILYPOS_PYTHON='DailyPOS.py'
GROUPING_PYTHON_FILE = 'Customer_Grouping.py'
NEXT_DAG_ID = 'HELLO_WORLD_DAG'


default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),#datetime(2023,4,19),
    'retries': 0,
    'retry_delay': timedelta(seconds=5), #timedelta(minutes=1),
    #'email': email_receiver,

    'email_on_failure': False
}

with DAG('POS_DAG',
        schedule_interval = None,
        # start_date=datetime(2022,3,8),
        catchup = False,
        max_active_runs  = 1,
        default_args=default_args,
        ) as dag:
       
    config = configparser.ConfigParser()


START_TASK = DummyOperator(
    task_id = "START",
    dag = dag)

# parallel_tasks = [
    # BashOperator(
    #     task_id='CUSTOMER_PRODUCT',
    #     bash_command = 'python {}{} '.format(python_scripts_path,CUSTOMER_PROD_PYTHON),
    #     dag=dag,
    # ),
    # BashOperator(
    #     task_id='CUSTOMER_GROUPINGS',
    #     bash_command = 'python {}{} '.format(python_scripts_path,GROUPING_PYTHON_FILE),
    #     dag=dag,
    # )
# ]
CUSTOMER_PROD= BashOperator(
        task_id='CUSTOMER_PRODUCT',
        bash_command = 'python {}{} '.format(python_scripts_path_CUSTOMER_PROD,CUSTOMER_PROD_PYTHON),
        dag=dag,
    )

    


# TRIGGER_ANOTHER_DAG = TriggerDagRunOperator(
#     task_id='TRIGGER_ANOTHER_DAG',
#     trigger_dag_id=NEXT_DAG_ID,
#     # conf={'key': 'value'},  # Optional: Pass any configuration parameters to the triggered DAG
#     dag=dag
# )

source_folder= 'gs://pos_unified_schema/NewFiles/'
destination_folder= 'gs://pos_unified_schema/NewFileProcessing/'
gsutil_command = f"gsutil -m mv {source_folder}* {destination_folder}"

# Define the task to execute the gsutil command
MOVE_FILES = BashOperator(
    task_id='move_files_task',
    bash_command=gsutil_command,
    dag=dag,
)

LOAD_TO_DAILYPOS=BashOperator(
        task_id='DAILYPOS',
        bash_command = 'python {}{} '.format(python_scripts_path_DAILYPOS,DAILYPOS_PYTHON),
        dag=dag,
    )

END_TASK = DummyOperator(
    task_id = "END",
    trigger_rule = TriggerRule.ALL_DONE,
    dag = dag)


START_TASK >> CUSTOMER_PROD >> MOVE_FILES  >> LOAD_TO_DAILYPOS >> END_TASK
