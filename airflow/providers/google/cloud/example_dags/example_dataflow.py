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
Example Airflow DAG for Google Cloud Dataflow service
"""
import os
from urllib.parse import urlparse

from airflow import models
from airflow.providers.google.cloud.operators.dataflow import (
    CheckJobRunning,
    DataflowCreateJavaJobOperator,
    DataflowCreatePythonJobOperator,
    DataflowTemplatedJobStartOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.utils.dates import days_ago

GCS_TMP = os.environ.get('GCP_DATAFLOW_GCS_TMP', 'gs://test-dataflow-example/temp/')
GCS_STAGING = os.environ.get('GCP_DATAFLOW_GCS_STAGING', 'gs://test-dataflow-example/staging/')
GCS_OUTPUT = os.environ.get('GCP_DATAFLOW_GCS_OUTPUT', 'gs://test-dataflow-example/output')
GCS_JAR = os.environ.get('GCP_DATAFLOW_JAR', 'gs://test-dataflow-example/word-count-beam-bundled-0.1.jar')
GCS_PYTHON = os.environ.get('GCP_DATAFLOW_PYTHON', 'gs://test-dataflow-example/wordcount_debugging.py')

GCS_JAR_PARTS = urlparse(GCS_JAR)
GCS_JAR_BUCKET_NAME = GCS_JAR_PARTS.netloc
GCS_JAR_OBJECT_NAME = GCS_JAR_PARTS.path[1:]

default_args = {'dataflow_default_options': {'tempLocation': GCS_TMP, 'stagingLocation': GCS_STAGING,}}

with models.DAG(
    "example_gcp_dataflow_native_java",
    schedule_interval=None,  # Override to match your needs
    start_date=days_ago(1),
    tags=['example'],
) as dag_native_java:

    # [START howto_operator_start_java_job]
    start_java_job = DataflowCreateJavaJobOperator(
        task_id="start-java-job",
        jar=GCS_JAR,
        job_name='{{task.task_id}}',
        options={'output': GCS_OUTPUT,},
        poll_sleep=10,
        job_class='org.apache.beam.examples.WordCount',
        check_if_running=CheckJobRunning.IgnoreJob,
        location='europe-west3',
    )
    # [END howto_operator_start_java_job]

    jar_to_local = GCSToLocalFilesystemOperator(
        task_id="jar-to-local",
        bucket=GCS_JAR_BUCKET_NAME,
        object_name=GCS_JAR_OBJECT_NAME,
        filename="/tmp/dataflow-{{ ds_nodash }}.jar",
    )

    start_java_job_local = DataflowCreateJavaJobOperator(
        task_id="start-java-job-local",
        jar="/tmp/dataflow-{{ ds_nodash }}.jar",
        job_name='{{task.task_id}}',
        options={'output': GCS_OUTPUT,},
        poll_sleep=10,
        job_class='org.apache.beam.examples.WordCount',
        check_if_running=CheckJobRunning.WaitForRun,
    )
    jar_to_local >> start_java_job_local

with models.DAG(
    "example_gcp_dataflow_native_python",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # Override to match your needs
    tags=['example'],
) as dag_native_python:

    # [START howto_operator_start_python_job]
    start_python_job = DataflowCreatePythonJobOperator(
        task_id="start-python-job",
        py_file=GCS_PYTHON,
        py_options=[],
        job_name='{{task.task_id}}',
        options={'output': GCS_OUTPUT,},
        py_requirements=['apache-beam[gcp]==2.21.0'],
        py_interpreter='python3',
        py_system_site_packages=False,
        location='europe-west3',
    )
    # [END howto_operator_start_python_job]

    start_python_job_local = DataflowCreatePythonJobOperator(
        task_id="start-python-job-local",
        py_file='apache_beam.examples.wordcount',
        py_options=['-m'],
        job_name='{{task.task_id}}',
        options={'output': GCS_OUTPUT,},
        py_requirements=['apache-beam[gcp]==2.14.0'],
        py_interpreter='python3',
        py_system_site_packages=False,
    )

with models.DAG(
    "example_gcp_dataflow_template",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # Override to match your needs
    tags=['example'],
) as dag_template:
    start_template_job = DataflowTemplatedJobStartOperator(
        task_id="start-template-job",
        template='gs://dataflow-templates/latest/Word_Count',
        parameters={'inputFile': "gs://dataflow-samples/shakespeare/kinglear.txt", 'output': GCS_OUTPUT},
        location='europe-west3',
    )
