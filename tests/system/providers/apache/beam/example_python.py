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
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from tests.system.providers.apache.beam.utils import (
    DEFAULT_ARGS,
    GCP_PROJECT_ID,
    GCS_OUTPUT,
    GCS_PYTHON,
    GCS_STAGING,
    GCS_TMP,
    START_DATE,
)

with models.DAG(
    "example_beam_native_python",
    start_date=START_DATE,
    schedule=None,  # Override to match your needs
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=['example'],
) as dag:

    # [START howto_operator_start_python_direct_runner_pipeline_local_file]
    start_python_pipeline_local_direct_runner = BeamRunPythonPipelineOperator(
        task_id="start_python_pipeline_local_direct_runner",
        py_file='apache_beam.examples.wordcount',
        py_options=['-m'],
        py_requirements=['apache-beam[gcp]==2.26.0'],
        py_interpreter='python3',
        py_system_site_packages=False,
    )
    # [END howto_operator_start_python_direct_runner_pipeline_local_file]

    # [START howto_operator_start_python_direct_runner_pipeline_gcs_file]
    start_python_pipeline_direct_runner = BeamRunPythonPipelineOperator(
        task_id="start_python_pipeline_direct_runner",
        py_file=GCS_PYTHON,
        py_options=[],
        pipeline_options={"output": GCS_OUTPUT},
        py_requirements=['apache-beam[gcp]==2.26.0'],
        py_interpreter='python3',
        py_system_site_packages=False,
    )
    # [END howto_operator_start_python_direct_runner_pipeline_gcs_file]

    # [START howto_operator_start_python_dataflow_runner_pipeline_gcs_file]
    start_python_pipeline_dataflow_runner = BeamRunPythonPipelineOperator(
        task_id="start_python_pipeline_dataflow_runner",
        runner="DataflowRunner",
        py_file=GCS_PYTHON,
        pipeline_options={
            'tempLocation': GCS_TMP,
            'stagingLocation': GCS_STAGING,
            'output': GCS_OUTPUT,
        },
        py_options=[],
        py_requirements=['apache-beam[gcp]==2.26.0'],
        py_interpreter='python3',
        py_system_site_packages=False,
        dataflow_config=DataflowConfiguration(
            job_name='{{task.task_id}}', project_id=GCP_PROJECT_ID, location="us-central1"
        ),
    )
    # [END howto_operator_start_python_dataflow_runner_pipeline_gcs_file]

    start_python_pipeline_local_spark_runner = BeamRunPythonPipelineOperator(
        task_id="start_python_pipeline_local_spark_runner",
        py_file='apache_beam.examples.wordcount',
        runner="SparkRunner",
        py_options=['-m'],
        py_requirements=['apache-beam[gcp]==2.26.0'],
        py_interpreter='python3',
        py_system_site_packages=False,
    )

    start_python_pipeline_local_flink_runner = BeamRunPythonPipelineOperator(
        task_id="start_python_pipeline_local_flink_runner",
        py_file='apache_beam.examples.wordcount',
        runner="FlinkRunner",
        py_options=['-m'],
        pipeline_options={
            'output': '/tmp/start_python_pipeline_local_flink_runner',
        },
        py_requirements=['apache-beam[gcp]==2.26.0'],
        py_interpreter='python3',
        py_system_site_packages=False,
    )

    (
        [
            start_python_pipeline_local_direct_runner,
            start_python_pipeline_direct_runner,
        ]
        >> start_python_pipeline_local_flink_runner
        >> start_python_pipeline_local_spark_runner
    )


from tests.system.utils import get_test_run

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
