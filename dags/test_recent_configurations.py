"""
Test DAG for Recent Configurations Feature
This DAG is used to test the "Select Recent Configurations" functionality.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# Default arguments
default_args = {
    'owner': 'test_user',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'test_recent_configurations',
    default_args=default_args,
    description='Test DAG for Recent Configurations Feature',
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['test', 'recent-configurations'],
)

def process_config(**context):
    """Process the configuration passed to the DAG run."""
    conf = context.get('dag_run').conf or {}
    print(f"Processing configuration: {conf}")
    
    # Extract parameters from configuration
    environment = conf.get('environment', 'default')
    version = conf.get('version', '1.0.0')
    debug = conf.get('debug', False)
    
    print(f"Environment: {environment}")
    print(f"Version: {version}")
    print(f"Debug mode: {debug}")
    
    return f"Processed with environment={environment}, version={version}, debug={debug}"

# Define tasks
start_task = EmptyOperator(
    task_id='start',
    dag=dag,
)

process_task = PythonOperator(
    task_id='process_configuration',
    python_callable=process_config,
    dag=dag,
)

end_task = EmptyOperator(
    task_id='end',
    dag=dag,
)

# Set task dependencies
start_task >> process_task >> end_task
