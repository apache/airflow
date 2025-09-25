"""
Simple test DAG for SparkKubernetesOperator XCom functionality.

This is a minimal test to verify that xcom push works with SparkKubernetesOperator.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.operators.python import PythonOperator

# Default arguments
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
    "simple_spark_xcom_test",
    default_args=default_args,
    description="Simple test for SparkKubernetesOperator XCom",
    schedule_interval=None,
    catchup=False,
    tags=["test", "spark", "xcom"],
)

# Simple Spark application that just prints and returns data
simple_spark_template = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {"name": "simple-spark-xcom-test", "namespace": "default"},
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

# Test task with xcom push enabled
test_spark_xcom = SparkKubernetesOperator(
    task_id="test_spark_xcom",
    name="simple-spark-xcom-test",
    namespace="default",
    template_spec=simple_spark_template,
    do_xcom_push=True,  # This is the key parameter to test!
    get_logs=True,
    delete_on_termination=True,
    dag=dag,
)


# Task to check the xcom result
def check_xcom_result(**context):
    """Check if xcom was successfully pushed."""
    print("=== Checking XCom Result ===")

    # Try to pull xcom from the Spark task
    xcom_result = context["ti"].xcom_pull(task_ids="test_spark_xcom")

    print(f"XCom result type: {type(xcom_result)}")
    print(f"XCom result: {xcom_result}")

    if xcom_result:
        print("✅ SUCCESS: XCom was pushed successfully!")
        print(f"✅ XCom contains: {xcom_result}")
    else:
        print("❌ FAILURE: No XCom data found")

    return xcom_result


check_result = PythonOperator(
    task_id="check_xcom_result",
    python_callable=check_xcom_result,
    dag=dag,
)

# Set up task dependencies
test_spark_xcom >> check_result
