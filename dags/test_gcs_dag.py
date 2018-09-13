"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/incubator-airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('gcs_upload_gzip', default_args=default_args)

t1 = FileToGoogleCloudStorageOperator(task_id='gcs_upload',src='/app/2008.csv',
dst='2008.csv1', bucket='gcsnotes-neil', gzip=False, dag=dag)

#export AIRFLOW_HOME=/airflow
#airflow initdb
#airflow webserver -p 8080
#pip install -e ".[hdfs,hive,druid,devel,gcs]"
#airflow test gcs_upload_gzip gcs_upload 2015-01-01
#https://www.googleapis.com/auth/cloud-platform
#pip install --upgrade google-api-python-client
