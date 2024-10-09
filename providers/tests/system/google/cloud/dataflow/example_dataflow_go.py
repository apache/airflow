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
"""
Example Airflow DAG for Apache Beam operators

Requirements:
    This test requires the gcloud and go commands to run.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow.models.dag import DAG
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType
from airflow.providers.apache.beam.operators.beam import BeamRunGoPipelineOperator
from airflow.providers.google.cloud.hooks.dataflow import DataflowJobStatus
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.sensors.dataflow import (
    DataflowJobAutoScalingEventsSensor,
    DataflowJobMessagesSensor,
    DataflowJobStatusSensor,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "dataflow_native_go_async"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"

GCS_TMP = f"gs://{BUCKET_NAME}/temp/"
GCS_STAGING = f"gs://{BUCKET_NAME}/staging/"
GCS_OUTPUT = f"gs://{BUCKET_NAME}/output"

GO_FILE_NAME = "wordcount.go"
GO_FILE_LOCAL_PATH = str(Path(__file__).parent / "resources" / GO_FILE_NAME)
GCS_GO = f"gs://{BUCKET_NAME}/{GO_FILE_NAME}"
LOCATION = "europe-west3"

default_args = {
    "dataflow_default_options": {
        "tempLocation": GCS_TMP,
        "stagingLocation": GCS_STAGING,
    }
}

with DAG(
    "example_beam_native_go",
    start_date=datetime(2021, 1, 1),
    schedule="@once",
    catchup=False,
    default_args=default_args,
    tags=["example"],
) as dag:
    create_bucket = GCSCreateBucketOperator(task_id="create_bucket", bucket_name=BUCKET_NAME)

    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file_to_bucket",
        src=GO_FILE_LOCAL_PATH,
        dst=GO_FILE_NAME,
        bucket=BUCKET_NAME,
    )

    start_go_pipeline_dataflow_runner = BeamRunGoPipelineOperator(
        task_id="start_go_pipeline_dataflow_runner",
        runner=BeamRunnerType.DataflowRunner,
        go_file=GCS_GO,
        pipeline_options={
            "tempLocation": GCS_TMP,
            "stagingLocation": GCS_STAGING,
            "output": GCS_OUTPUT,
            "WorkerHarnessContainerImage": "apache/beam_go_sdk:2.46.0",
        },
        dataflow_config=DataflowConfiguration(job_name="start_go_job", location=LOCATION),
    )

    wait_for_go_job_async_done = DataflowJobStatusSensor(
        task_id="wait_for_go_job_async_done",
        job_id="{{task_instance.xcom_pull('start_go_pipeline_dataflow_runner')['dataflow_job_id']}}",
        expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
        location=LOCATION,
    )

    def check_message(messages: list[dict]) -> bool:
        """Check message"""
        for message in messages:
            if "Adding workflow start and stop steps." in message.get("messageText", ""):
                return True
        return False

    wait_for_go_job_async_message = DataflowJobMessagesSensor(
        task_id="wait_for_go_job_async_message",
        job_id="{{task_instance.xcom_pull('start_go_pipeline_dataflow_runner')['dataflow_job_id']}}",
        location=LOCATION,
        callback=check_message,
        fail_on_terminal_state=False,
    )

    def check_autoscaling_event(autoscaling_events: list[dict]) -> bool:
        """Check autoscaling event"""
        for autoscaling_event in autoscaling_events:
            if "Worker pool started." in autoscaling_event.get("description", {}).get("messageText", ""):
                return True
        return False

    wait_for_go_job_async_autoscaling_event = DataflowJobAutoScalingEventsSensor(
        task_id="wait_for_go_job_async_autoscaling_event",
        job_id="{{task_instance.xcom_pull('start_go_pipeline_dataflow_runner')['dataflow_job_id']}}",
        location=LOCATION,
        callback=check_autoscaling_event,
        fail_on_terminal_state=False,
    )

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        # TEST SETUP
        create_bucket
        >> upload_file
        # TEST BODY
        >> start_go_pipeline_dataflow_runner
        >> [
            wait_for_go_job_async_done,
            wait_for_go_job_async_message,
            wait_for_go_job_async_autoscaling_event,
        ]
        # TEST TEARDOWN
        >> delete_bucket
    )


from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
