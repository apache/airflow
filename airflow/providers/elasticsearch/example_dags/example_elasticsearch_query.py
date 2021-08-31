from airflow import DAG

from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def show_tables():
    """
    show_tables queries elasticsearch to list available tables
    """
    es = ElasticsearchHook(elasticsearch_conn_id='production-es')

    # Handle ES conn with context manager
    with es.get_conn() as es_conn:
        tables = es_conn.execute('SHOW TABLES')
        for table, *_ in tables:
            print(f"table: {table}")
    return True


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG('elasticsearch_dag',
         start_date=datetime(2021, 8, 30),
         max_active_runs=1,
         schedule_interval=timedelta(days=1),
         default_args=default_args,
         catchup=False
         ) as dag:

    es_tables = PythonOperator(
        task_id='es_print_tables',
        python_callable=show_tables
    )
