"""
ML Training Pipeline DAG
Author: Harshad Khetpal
Description: End-to-end ML pipeline orchestrating data ingestion,
             validation, feature engineering, model training, and registration.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'harshad.khetpal',
    'depends_on_past': False,
    'email': ['harshad@mlops-platform.internal'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def validate_data(**context):
    """Validate input data quality before training."""
    import great_expectations as ge
    print("Running data validation checks...")
    # Placeholder: integrate with GE checkpoint
    return {"rows": 100000, "validation_passed": True}


def run_feature_engineering(**context):
    """Generate features for model training."""
    ti = context['task_instance']
    validation_result = ti.xcom_pull(task_ids='validate_data')
    print(f"Processing {validation_result['rows']} rows for feature engineering")
    return {"feature_store_version": "v20260320", "num_features": 45}


def train_model(**context):
    """Train model and log to MLflow."""
    ti = context['task_instance']
    features = ti.xcom_pull(task_ids='feature_engineering')
    print(f"Training model with {features['num_features']} features")
    # MLflow integration would go here
    return {"model_version": "1.0.0", "accuracy": 0.94}


def register_model(**context):
    """Register model in MLflow Model Registry if metrics pass threshold."""
    ti = context['task_instance']
    model_result = ti.xcom_pull(task_ids='train_model')
    if model_result['accuracy'] >= 0.90:
        print(f"Registering model v{model_result['model_version']} to production")
    else:
        raise ValueError(f"Model accuracy {model_result['accuracy']} below threshold 0.90")


with DAG(
    'ml_training_pipeline',
    default_args=default_args,
    description='End-to-end ML training pipeline',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['mlops', 'training', 'harshad'],
) as dag:

    validate = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
    )

    feature_eng = PythonOperator(
        task_id='feature_engineering',
        python_callable=run_feature_engineering,
    )

    train = PythonOperator(
        task_id='train_model',
        python_callable=train_model,
    )

    register = PythonOperator(
        task_id='register_model',
        python_callable=register_model,
    )

    notify = BashOperator(
        task_id='notify_team',
        bash_command='echo "ML pipeline completed successfully at $(date)"',
    )

    validate >> feature_eng >> train >> register >> notify
