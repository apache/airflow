from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def extract_data(**kwargs):
    print("Extracting data from legacy ERP system")


def validate_data(**kwargs):
    print("Validating data (completeness, accuracy checks)")


def transform_data(**kwargs):
    print("Transforming data to target ERP format")


def load_data(**kwargs):
    print("Loading data into target system")


def reconcile_data(**kwargs):
    print("Reconciling source and target data")


with DAG(
    dag_id="example_erp_migration_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "erp", "migration"],
) as dag:

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    load = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    reconcile = PythonOperator(
        task_id="reconcile_data",
        python_callable=reconcile_data,
    )

    extract >> validate >> transform >> load >> reconcile
