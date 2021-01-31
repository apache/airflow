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
import os
from urllib.parse import urlparse

from airflow import models
from airflow.providers.apache.beam.operators.beam import (
    BeamRunJavaPipelineOperator,
    BeamRunPythonPipelineOperator,
)
from airflow.providers.google.cloud.hooks.dataflow import DataflowJobStatus
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatusSensor
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.utils.dates import days_ago

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'example-project')
GCS_INPUT = os.environ.get('APACHE_BEAM_PYTHON', 'gs://apache-beam-samples/shakespeare/kinglear.txt')
GCS_TMP = os.environ.get('APACHE_BEAM_GCS_TMP', 'gs://test-dataflow-example/temp/')
GCS_STAGING = os.environ.get('APACHE_BEAM_GCS_STAGING', 'gs://test-dataflow-example/staging/')
GCS_OUTPUT = os.environ.get('APACHE_BEAM_GCS_OUTPUT', 'gs://test-dataflow-example/output')
GCS_PYTHON = os.environ.get('APACHE_BEAM_PYTHON', 'gs://test-dataflow-example/wordcount_debugging.py')
GCS_PYTHON_DATAFLOW_ASYNC = os.environ.get(
    'APACHE_BEAM_PYTHON_DATAFLOW_ASYNC', 'gs://test-dataflow-example/wordcount_debugging.py'
)

GCS_JAR_DIRECT_RUNNER = os.environ.get(
    'APACHE_BEAM_DIRECT_RUNNER_JAR',
    'gs://test-dataflow-example/tests/dataflow-templates-bundled-java=11-beam-v2.25.0-DirectRunner.jar',
)
GCS_JAR_DATAFLOW_RUNNER = os.environ.get(
    'APACHE_BEAM_DATAFLOW_RUNNER_JAR', 'gs://test-dataflow-example/word-count-beam-bundled-0.1.jar'
)
GCS_JAR_SPARK_RUNNER = os.environ.get(
    'APACHE_BEAM_SPARK_RUNNER_JAR',
    'gs://test-dataflow-example/tests/dataflow-templates-bundled-java=11-beam-v2.25.0-SparkRunner.jar',
)
GCS_JAR_FLINK_RUNNER = os.environ.get(
    'APACHE_BEAM_FLINK_RUNNER_JAR',
    'gs://test-dataflow-example/tests/dataflow-templates-bundled-java=11-beam-v2.25.0-FlinkRunner.jar',
)

GCS_JAR_DIRECT_RUNNER_PARTS = urlparse(GCS_JAR_DIRECT_RUNNER)
GCS_JAR_DIRECT_RUNNER_BUCKET_NAME = GCS_JAR_DIRECT_RUNNER_PARTS.netloc
GCS_JAR_DIRECT_RUNNER_OBJECT_NAME = GCS_JAR_DIRECT_RUNNER_PARTS.path[1:]
GCS_JAR_DATAFLOW_RUNNER_PARTS = urlparse(GCS_JAR_DATAFLOW_RUNNER)
GCS_JAR_DATAFLOW_RUNNER_BUCKET_NAME = GCS_JAR_DATAFLOW_RUNNER_PARTS.netloc
GCS_JAR_DATAFLOW_RUNNER_OBJECT_NAME = GCS_JAR_DATAFLOW_RUNNER_PARTS.path[1:]
GCS_JAR_SPARK_RUNNER_PARTS = urlparse(GCS_JAR_SPARK_RUNNER)
GCS_JAR_SPARK_RUNNER_BUCKET_NAME = GCS_JAR_SPARK_RUNNER_PARTS.netloc
GCS_JAR_SPARK_RUNNER_OBJECT_NAME = GCS_JAR_SPARK_RUNNER_PARTS.path[1:]
GCS_JAR_FLINK_RUNNER_PARTS = urlparse(GCS_JAR_FLINK_RUNNER)
GCS_JAR_FLINK_RUNNER_BUCKET_NAME = GCS_JAR_FLINK_RUNNER_PARTS.netloc
GCS_JAR_FLINK_RUNNER_OBJECT_NAME = GCS_JAR_FLINK_RUNNER_PARTS.path[1:]


default_args = {
    'default_pipeline_options': {
        'output': '/tmp/example_beam',
    },
    "trigger_rule": "all_done",
}


with models.DAG(
    "example_beam_native_java_direct_runner",
    schedule_interval=None,  # Override to match your needs
    start_date=days_ago(1),
    tags=['example'],
) as dag_native_java_direct_runner:

    # [START howto_operator_start_java_direct_runner_pipeline]
    jar_to_local_direct_runner = GCSToLocalFilesystemOperator(
        task_id="jar_to_local_direct_runner",
        bucket=GCS_JAR_DIRECT_RUNNER_BUCKET_NAME,
        object_name=GCS_JAR_DIRECT_RUNNER_OBJECT_NAME,
        filename="/tmp/beam_wordcount_direct_runner_{{ ds_nodash }}.jar",
    )

    start_java_pipeline_direct_runner = BeamRunJavaPipelineOperator(
        task_id="start_java_pipeline_direct_runner",
        jar="/tmp/beam_wordcount_direct_runner_{{ ds_nodash }}.jar",
        pipeline_options={
            'output': '/tmp/start_java_pipeline_direct_runner',
            'inputFile': GCS_INPUT,
        },
        job_class='org.apache.beam.examples.WordCount',
    )

    jar_to_local_direct_runner >> start_java_pipeline_direct_runner
    # [END howto_operator_start_java_direct_runner_pipeline]

with models.DAG(
    "example_beam_native_java_dataflow_runner",
    schedule_interval=None,  # Override to match your needs
    start_date=days_ago(1),
    tags=['example'],
) as dag_native_java_dataflow_runner:
    # [START howto_operator_start_java_dataflow_runner_pipeline]
    jar_to_local_dataflow_runner = GCSToLocalFilesystemOperator(
        task_id="jar_to_local_dataflow_runner",
        bucket=GCS_JAR_DATAFLOW_RUNNER_BUCKET_NAME,
        object_name=GCS_JAR_DATAFLOW_RUNNER_OBJECT_NAME,
        filename="/tmp/beam_wordcount_dataflow_runner_{{ ds_nodash }}.jar",
    )

    start_java_pipeline_dataflow = BeamRunJavaPipelineOperator(
        task_id="start_java_pipeline_dataflow",
        runner="DataflowRunner",
        jar="/tmp/beam_wordcount_dataflow_runner_{{ ds_nodash }}.jar",
        pipeline_options={
            'tempLocation': GCS_TMP,
            'stagingLocation': GCS_STAGING,
            'output': GCS_OUTPUT,
        },
        job_class='org.apache.beam.examples.WordCount',
        dataflow_config={"job_name": "{{task.task_id}}", "location": "us-central1"},
    )

    jar_to_local_dataflow_runner >> start_java_pipeline_dataflow
    # [END howto_operator_start_java_dataflow_runner_pipeline]

with models.DAG(
    "example_beam_native_java_spark_runner",
    schedule_interval=None,  # Override to match your needs
    start_date=days_ago(1),
    tags=['example'],
) as dag_native_java_spark_runner:

    jar_to_local_spark_runner = GCSToLocalFilesystemOperator(
        task_id="jar_to_local_spark_runner",
        bucket=GCS_JAR_SPARK_RUNNER_BUCKET_NAME,
        object_name=GCS_JAR_SPARK_RUNNER_OBJECT_NAME,
        filename="/tmp/beam_wordcount_spark_runner_{{ ds_nodash }}.jar",
    )

    start_java_pipeline_spark_runner = BeamRunJavaPipelineOperator(
        task_id="start_java_pipeline_spark_runner",
        runner="SparkRunner",
        jar="/tmp/beam_wordcount_spark_runner_{{ ds_nodash }}.jar",
        pipeline_options={
            'output': '/tmp/start_java_pipeline_spark_runner',
            'inputFile': GCS_INPUT,
        },
        job_class='org.apache.beam.examples.WordCount',
    )

    jar_to_local_spark_runner >> start_java_pipeline_spark_runner

with models.DAG(
    "example_beam_native_java_flink_runner",
    schedule_interval=None,  # Override to match your needs
    start_date=days_ago(1),
    tags=['example'],
) as dag_native_java_flink_runner:

    jar_to_local_flink_runner = GCSToLocalFilesystemOperator(
        task_id="jar_to_local_flink_runner",
        bucket=GCS_JAR_FLINK_RUNNER_BUCKET_NAME,
        object_name=GCS_JAR_FLINK_RUNNER_OBJECT_NAME,
        filename="/tmp/beam_wordcount_flink_runner_{{ ds_nodash }}.jar",
    )

    start_java_pipeline_flink_runner = BeamRunJavaPipelineOperator(
        task_id="start_java_pipeline_flink_runner",
        runner="FlinkRunner",
        jar="/tmp/beam_wordcount_flink_runner_{{ ds_nodash }}.jar",
        pipeline_options={
            'output': '/tmp/start_java_pipeline_flink_runner',
            'inputFile': GCS_INPUT,
        },
        job_class='org.apache.beam.examples.WordCount',
    )

    jar_to_local_flink_runner >> start_java_pipeline_flink_runner


with models.DAG(
    "example_beam_native_python",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # Override to match your needs
    tags=['example'],
) as dag_native_python:

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

    [
        start_python_pipeline_local_direct_runner,
        start_python_pipeline_direct_runner,
    ] >> start_python_pipeline_local_flink_runner >> start_python_pipeline_local_spark_runner


with models.DAG(
    "example_beam_native_python_dataflow_async",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # Override to match your needs
    tags=['example'],
) as dag_native_python_dataflow_async:
    # [START howto_operator_start_python_dataflow_runner_pipeline_async_gcs_file]
    start_python_job_dataflow_runner_async = BeamRunPythonPipelineOperator(
        task_id="start_python_job_dataflow_runner_async",
        runner="DataflowRunner",
        py_file=GCS_PYTHON_DATAFLOW_ASYNC,
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
            job_name='{{task.task_id}}',
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
        location='us-central1',
    )

    start_python_job_dataflow_runner_async >> wait_for_python_job_dataflow_runner_async_done
    # [END howto_operator_start_python_dataflow_runner_pipeline_async_gcs_file]
