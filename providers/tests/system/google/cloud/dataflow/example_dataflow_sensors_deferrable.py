#
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

"""Example Airflow DAG for testing Google Dataflow Beam Pipeline Operator with Asynchronous Python in the deferrable mode."""

from __future__ import annotations

import os
from datetime import datetime
from typing import Callable

from airflow.exceptions import AirflowException
from airflow.models.dag import DAG
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.hooks.dataflow import DataflowJobStatus
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.sensors.dataflow import (
    DataflowJobAutoScalingEventsSensor,
    DataflowJobMessagesSensor,
    DataflowJobMetricsSensor,
    DataflowJobStatusSensor,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "dataflow_sensors_deferrable"

RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"

GCS_TMP = f"gs://{BUCKET_NAME}/temp/"
GCS_STAGING = f"gs://{BUCKET_NAME}/staging/"
GCS_OUTPUT = f"gs://{BUCKET_NAME}/output"
GCS_PYTHON_SCRIPT = f"gs://{RESOURCE_DATA_BUCKET}/dataflow/python/wordcount_debugging.py"
LOCATION = "europe-west3"

default_args = {
    "dataflow_default_options": {
        "tempLocation": GCS_TMP,
        "stagingLocation": GCS_STAGING,
    }
}


with DAG(
    DAG_ID,
    default_args=default_args,
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "dataflow"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME
    )

    start_beam_python_pipeline = BeamRunPythonPipelineOperator(
        task_id="start_beam_python_pipeline",
        runner=BeamRunnerType.DataflowRunner,
        py_file=GCS_PYTHON_SCRIPT,
        py_options=[],
        pipeline_options={
            "output": GCS_OUTPUT,
        },
        py_requirements=["apache-beam[gcp]==2.59.0"],
        py_interpreter="python3",
        py_system_site_packages=False,
        dataflow_config={
            "job_name": "start_beam_python_pipeline",
            "location": LOCATION,
            "wait_until_finished": False,
        },
    )

    # [START howto_sensor_wait_for_job_status_deferrable]
    wait_for_beam_python_pipeline_job_status_def = DataflowJobStatusSensor(
        task_id="wait_for_beam_python_pipeline_job_status_def",
        job_id="{{task_instance.xcom_pull('start_beam_python_pipeline')['dataflow_job_id']}}",
        expected_statuses=DataflowJobStatus.JOB_STATE_DONE,
        location=LOCATION,
        deferrable=True,
    )
    # [END howto_sensor_wait_for_job_status_deferrable]

    # [START howto_sensor_wait_for_job_metric_deferrable]
    def check_metric_scalar_gte(metric_name: str, value: int) -> Callable:
        """Check is metric greater than equals to given value."""

        def callback(metrics: list[dict]) -> bool:
            dag.log.info("Looking for '%s' >= %d", metric_name, value)
            for metric in metrics:
                context = metric.get("name", {}).get("context", {})
                original_name = context.get("original_name", "")
                tentative = context.get("tentative", "")
                if original_name == "Service-cpu_num_seconds" and not tentative:
                    return metric["scalar"] >= value
            raise AirflowException(f"Metric '{metric_name}' not found in metrics")

        return callback

    wait_for_beam_python_pipeline_job_metric_def = DataflowJobMetricsSensor(
        task_id="wait_for_beam_python_pipeline_job_metric_def",
        job_id="{{task_instance.xcom_pull('start_beam_python_pipeline')['dataflow_job_id']}}",
        location=LOCATION,
        callback=check_metric_scalar_gte(
            metric_name="Service-cpu_num_seconds", value=100
        ),
        fail_on_terminal_state=False,
        deferrable=True,
    )
    # [END howto_sensor_wait_for_job_metric_deferrable]

    # [START howto_sensor_wait_for_job_message_deferrable]
    def check_job_message(messages: list[dict]) -> bool:
        """Check job message."""
        for message in messages:
            if "Adding workflow start and stop steps." in message.get("messageText", ""):
                return True
        return False

    wait_for_beam_python_pipeline_job_message_def = DataflowJobMessagesSensor(
        task_id="wait_for_beam_python_pipeline_job_message_def",
        job_id="{{task_instance.xcom_pull('start_beam_python_pipeline')['dataflow_job_id']}}",
        location=LOCATION,
        callback=check_job_message,
        fail_on_terminal_state=False,
        deferrable=True,
    )
    # [END howto_sensor_wait_for_job_message_deferrable]

    # [START howto_sensor_wait_for_job_autoscaling_event_deferrable]
    def check_autoscaling_event(autoscaling_events: list[dict]) -> bool:
        """Check autoscaling event."""
        for autoscaling_event in autoscaling_events:
            if "Worker pool started." in autoscaling_event.get("description", {}).get(
                "messageText", ""
            ):
                return True
        return False

    wait_for_beam_python_pipeline_job_autoscaling_event_def = DataflowJobAutoScalingEventsSensor(
        task_id="wait_for_beam_python_pipeline_job_autoscaling_event_def",
        job_id="{{task_instance.xcom_pull('start_beam_python_pipeline')['dataflow_job_id']}}",
        location=LOCATION,
        callback=check_autoscaling_event,
        fail_on_terminal_state=False,
        deferrable=True,
    )
    # [END howto_sensor_wait_for_job_autoscaling_event_deferrable]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        # TEST SETUP
        create_bucket
        >> start_beam_python_pipeline
        # TEST BODY
        >> [
            wait_for_beam_python_pipeline_job_status_def,
            wait_for_beam_python_pipeline_job_metric_def,
            wait_for_beam_python_pipeline_job_message_def,
            wait_for_beam_python_pipeline_job_autoscaling_event_def,
        ]
        # TEST TEARDOWN
        >> delete_bucket
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
