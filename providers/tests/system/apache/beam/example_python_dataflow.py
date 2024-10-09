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
"""

from __future__ import annotations

from airflow import models
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.hooks.dataflow import DataflowJobStatus
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatusSensor

from providers.tests.system.apache.beam.utils import (
    DEFAULT_ARGS,
    GCP_PROJECT_ID,
    GCS_OUTPUT,
    GCS_PYTHON_DATAFLOW_ASYNC,
    GCS_STAGING,
    GCS_TMP,
    START_DATE,
)

with models.DAG(
    "example_beam_native_python_dataflow_async",
    default_args=DEFAULT_ARGS,
    start_date=START_DATE,
    schedule=None,  # Override to match your needs
    catchup=False,
    tags=["example"],
) as dag:
    # [START howto_operator_start_python_dataflow_runner_pipeline_async_gcs_file]
    start_python_job_dataflow_runner_async = BeamRunPythonPipelineOperator(
        task_id="start_python_job_dataflow_runner_async",
        runner="DataflowRunner",
        py_file=GCS_PYTHON_DATAFLOW_ASYNC,
        pipeline_options={
            "tempLocation": GCS_TMP,
            "stagingLocation": GCS_STAGING,
            "output": GCS_OUTPUT,
        },
        py_options=[],
        py_requirements=["apache-beam[gcp]==2.26.0"],
        py_interpreter="python3",
        py_system_site_packages=False,
        dataflow_config=DataflowConfiguration(
            job_name="{{task.task_id}}",
            project_id=GCP_PROJECT_ID,
            location="us-central1",
            wait_until_finished=False,
        ),
    )

    wait_for_python_job_dataflow_runner_async_done = DataflowJobStatusSensor(
        task_id="wait-for-python-job-async-done",
        job_id="{{task_instance.xcom_pull('start_python_job_dataflow_runner_async')['dataflow_job_id']}}",
        expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
        project_id=GCP_PROJECT_ID,
        location="us-central1",
    )

    start_python_job_dataflow_runner_async >> wait_for_python_job_dataflow_runner_async_done
    # [END howto_operator_start_python_dataflow_runner_pipeline_async_gcs_file]


from dev.tests_common.test_utils.system_tests import get_test_run

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
