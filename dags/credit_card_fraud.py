import os
from datetime import timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from helper import *
import boto3

data_folder = "/opt/airflow/data"
url = 'https://raw.githubusercontent.com/mohamedmagdy841/credit-card-fraud/main/data/credit_card_fraud.csv'
raw_data = os.path.join(data_folder,'credit_card_fraud.csv')
clean_data = os.path.join(data_folder,'transformed_credit_card_fraud.csv')
s3 = boto3.client('s3', aws_access_key_id='',
                        aws_secret_access_key='')

# PostgreSQL
hostname = 'db'
user='postgres'  
pwd =''      
port ="5432"               
database ="credit_card"           

default_arg = {
    'owner': 'Mohamed Magdy',
    'start_date': days_ago(0),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'credit_card_fraud',
    schedule_interval = timedelta(days=1),
    default_args = default_arg
)

extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract,
    op_args=[url],
    dag=dag
)

upload_to_staging = PythonOperator(
    task_id='upload_to_staging',
    python_callable=s3_upload,
    op_args=[raw_data, s3],
    dag=dag
)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform,
    op_args=[clean_data, s3],
    dag=dag    
)

upload_to_transformed = PythonOperator(
    task_id='upload_to_transformed',
    python_callable=move_to_transformed,
    op_args=[clean_data, s3],
    dag=dag
)

load_data = PythonOperator(
    task_id='load_data',
    python_callable=load,
    op_args=[s3, hostname, user, pwd, port, database],
    dag=dag
)


extract_data >> upload_to_staging >> transform_data >> upload_to_transformed >> load_data
