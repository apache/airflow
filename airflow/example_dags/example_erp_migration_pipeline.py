"""
Example DAG: ERP Data Migration Pipeline

This example demonstrates a structured ETL workflow including:
extract, validate, transform, load, and reconcile steps.

It is a simplified illustration of how Apache Airflow can be used
to orchestrate enterprise ERP data migration processes.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def extract_data(**kwargs):
    """Simulates extraction from a legacy ERP system."""
    print("Extracting data from legacy ERP system")


def validate_data(**kwargs):
    """Simulates validation checks for completeness and accuracy."""
    print("Validating data (completeness, accuracy checks)")


def transform_data(**kwargs):
    """Simulates transforming source data into the target ERP format."""
    print("Transforming data to target ERP format")


def load_data(**kwargs):
    """Simulates loading transformed data into the target system."""
    print("Loading data into target system")


def reconcile_data(**kwargs):
    """Simulates source-to-target reconciliation after loading."""
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
