# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Example Airflow DAG for Google Cloud Dataflow Get Metrics Operator.

This DAG demonstrates how to use DataflowJobMetricsOperator to:
1. Collect metrics from a Dataflow job
2. Pass metrics to a callback function for processing
3. Return metrics directly for XCom consumption when no callback is provided
4. Use deferrable mode for async execution
5. Consume metrics from XCom in downstream tasks
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.sdk import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowJobMetricsOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or "test-project"
DAG_ID = "dataflow_get_metrics"
LOCATION = "us-central1"


def process_metrics_callback(metrics):
    """Callback function that processes metrics returned by the operator."""
    metric_list = metrics if isinstance(metrics, list) else []
    print(f"Metrics count from callback: {len(metric_list)}")
    return {"processed_metrics_count": len(metric_list)}


def consume_metrics_from_xcom(**context):
    """Consume and display metrics count from XCom."""
    task_instance = context["task_instance"]
    metrics = task_instance.xcom_pull(task_ids="collect_metrics_no_callback", key="return_value")
    metric_list = metrics if isinstance(metrics, list) else []
    print(f"Metrics count from XCom: {len(metric_list)}")


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "dataflow", "metrics"],
) as dag:
    start_task = EmptyOperator(task_id="start_task")

    # [START howto_operator_dataflow_get_metrics_with_callback]
    collect_metrics_with_callback = DataflowJobMetricsOperator(
        task_id="collect_metrics_with_callback",
        job_id="{{ dag_run.conf.get('dataflow_job_id', 'test-job-id') }}",
        project_id=PROJECT_ID,
        location=LOCATION,
        callback=process_metrics_callback,
        deferrable=False,
        fail_on_terminal_state=False,
        gcp_conn_id="google_cloud_default",
    )
    # [END howto_operator_dataflow_get_metrics_with_callback]

    # [START howto_operator_dataflow_get_metrics_no_callback]
    collect_metrics_no_callback = DataflowJobMetricsOperator(
        task_id="collect_metrics_no_callback",
        job_id="{{ dag_run.conf.get('dataflow_job_id', 'test-job-id') }}",
        project_id=PROJECT_ID,
        location=LOCATION,
        # callback=None (default) - metrics will be returned directly
        deferrable=False,
        fail_on_terminal_state=False,
        gcp_conn_id="google_cloud_default",
    )
    # [END howto_operator_dataflow_get_metrics_no_callback]

    # [START howto_operator_dataflow_get_metrics_deferrable]
    collect_metrics_deferrable = DataflowJobMetricsOperator(
        task_id="collect_metrics_deferrable",
        job_id="{{ dag_run.conf.get('dataflow_job_id', 'test-job-id') }}",
        project_id=PROJECT_ID,
        location=LOCATION,
        callback=process_metrics_callback,
        deferrable=True,
        poll_interval=10,
        fail_on_terminal_state=False,
        gcp_conn_id="google_cloud_default",
    )
    # [END howto_operator_dataflow_get_metrics_deferrable]

    # [START howto_operator_dataflow_get_metrics_consume_xcom]
    consume_metrics = PythonOperator(
        task_id="consume_metrics_from_xcom",
        python_callable=consume_metrics_from_xcom,
    )
    # [END howto_operator_dataflow_get_metrics_consume_xcom]

    end_task = EmptyOperator(task_id="end_task")

    (
        start_task
        >> [
            collect_metrics_with_callback,
            collect_metrics_no_callback,
            collect_metrics_deferrable,
        ]
        >> consume_metrics
        >> end_task
    )


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
