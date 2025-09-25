"""
Test DAG for SparkKubernetesOperator with XCom functionality.

This DAG demonstrates the xcom push feature in SparkKubernetesOperator.
It creates a simple Spark job that returns data via xcom.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    "test_spark_kubernetes_xcom",
    default_args=default_args,
    description="Test SparkKubernetesOperator with XCom functionality",
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=["test", "spark", "kubernetes", "xcom"],
)

# Spark application template that will return data via xcom
spark_template = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {"name": "test-spark-xcom-job", "namespace": "default"},
    "spec": {
        "type": "Python",
        "pythonVersion": "3",
        "mode": "cluster",
        "image": "apache/spark:3.5.0-python3",
        "imagePullPolicy": "Always",
        "mainApplicationFile": "local:///opt/spark/examples/src/main/python/pi.py",
        "sparkVersion": "3.5.0",
        "restartPolicy": {"type": "Never"},
        "driver": {
            "cores": 1,
            "coreLimit": "1200m",
            "memory": "512m",
            "labels": {"version": "3.5.0"},
            "serviceAccount": "spark",
        },
        "executor": {"cores": 1, "instances": 1, "memory": "512m", "labels": {"version": "3.5.0"}},
    },
}

# Create a simple Python script that will be executed and return data
python_script = """
import json
import sys
import os

# Create some test data to return via xcom
test_data = {
    "job_id": "test-spark-xcom-job",
    "status": "completed",
    "result": 3.14159,
    "message": "Hello from Spark Kubernetes Operator!",
    "timestamp": "2024-01-01T00:00:00Z"
}

# Write the result to the xcom file
xcom_file = "/airflow/xcom/return.json"
os.makedirs(os.path.dirname(xcom_file), exist_ok=True)

with open(xcom_file, 'w') as f:
    json.dump(test_data, f)

print("XCom data written successfully!")
print(f"Data: {test_data}")
"""

# Create a custom Spark application that runs our Python script
custom_spark_template = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {"name": "test-spark-xcom-custom-job", "namespace": "default"},
    "spec": {
        "type": "Python",
        "pythonVersion": "3",
        "mode": "cluster",
        "image": "apache/spark:3.5.0-python3",
        "imagePullPolicy": "Always",
        "mainApplicationFile": "local:///tmp/test_script.py",
        "sparkVersion": "3.5.0",
        "restartPolicy": {"type": "Never"},
        "driver": {
            "cores": 1,
            "coreLimit": "1200m",
            "memory": "512m",
            "labels": {"version": "3.5.0"},
            "serviceAccount": "spark",
        },
        "executor": {"cores": 1, "instances": 1, "memory": "512m", "labels": {"version": "3.5.0"}},
    },
}

# Task 1: Test with built-in Spark example (Pi calculation)
test_spark_pi = SparkKubernetesOperator(
    task_id="test_spark_pi_xcom",
    name="test-spark-pi-xcom",
    namespace="default",
    template_spec=spark_template,
    do_xcom_push=True,  # Enable xcom push
    get_logs=True,
    delete_on_termination=True,
    dag=dag,
)

# Task 2: Test with custom Python script that returns structured data
test_spark_custom = SparkKubernetesOperator(
    task_id="test_spark_custom_xcom",
    name="test-spark-custom-xcom",
    namespace="default",
    template_spec=custom_spark_template,
    do_xcom_push=True,  # Enable xcom push
    get_logs=True,
    delete_on_termination=True,
    dag=dag,
)


# Task 3: Print the xcom results
def print_xcom_results(**context):
    """Print the xcom results from the Spark tasks."""
    print("=== XCom Results ===")

    # Get xcom from the Pi task
    pi_result = context["ti"].xcom_pull(task_ids="test_spark_pi_xcom")
    print(f"Pi task xcom result: {pi_result}")

    # Get xcom from the custom task
    custom_result = context["ti"].xcom_pull(task_ids="test_spark_custom_xcom")
    print(f"Custom task xcom result: {custom_result}")

    return {"pi_result": pi_result, "custom_result": custom_result}


from airflow.operators.python import PythonOperator

print_results = PythonOperator(
    task_id="print_xcom_results",
    python_callable=print_xcom_results,
    dag=dag,
)

# Set up task dependencies
test_spark_pi >> print_results
test_spark_custom >> print_results
