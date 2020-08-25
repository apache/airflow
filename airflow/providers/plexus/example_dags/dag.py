import airflow
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.plexus.operators.job import PlexusJobOperator

HOME = '/home/acc'
T3_PRERUN_SCRIPT = 'cp {home}/imdb/run_scripts/mlflow.sh {home}/ && chmod +x mlflow.sh'.format(home=HOME)

args = {
    'owner': 'core scientific',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 1,
}

dag = DAG(
    'test',
    default_args=args,
    description='testing plexus operator',
    schedule_interval='@once',
    catchup=False
)

t1 = PlexusJobOperator(
    task_id='test',
    job_params={'name': 'test', 'app': 'MLFlow Pipeline 01', 'queue': 'DGX-2 (gpu:Tesla V100-SXM3-32GB)', 'num_nodes': 1, 'num_cores': 1, 'prerun_script': T3_PRERUN_SCRIPT},
    dag=dag,
)

t1



