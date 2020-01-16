# -*- coding: utf-8 -*-
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

from airflow import models
from airflow.gcp.operators.dataflow import (
    CheckJobRunning, DataflowCreateJavaJobOperator, DataflowCreatePythonJobOperator,
    DataflowTemplatedJobStartOperator,
)
from airflow.utils.dates import days_ago

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'example-project')
GCS_TMP = os.environ.get('GCP_DATAFLOW_GCS_TMP', 'gs://test-dataflow-example/temp/')
GCS_STAGING = os.environ.get('GCP_DATAFLOW_GCS_STAGING', 'gs://test-dataflow-example/staging/')
GCS_OUTPUT = os.environ.get('GCP_DATAFLOW_GCS_OUTPUT', 'gs://test-dataflow-example/output')
GCS_JAR = os.environ.get('GCP_DATAFLOW_JAR', 'gs://test-dataflow-example/word-count-beam-bundled-0.1.jar')

default_args = {
    "start_date": days_ago(1),
    'dataflow_default_options': {
        'project': GCP_PROJECT_ID,
        'tempLocation': GCS_TMP,
        'stagingLocation': GCS_STAGING,
    }
}

with models.DAG(
    "example_gcp_dataflow",
    default_args=default_args,
    schedule_interval=None,  # Override to match your needs
    tags=['example'],
) as dag:

    # [START howto_operator_start_java_job]
    start_java_job = DataflowCreateJavaJobOperator(
        task_id="start-java-job",
        jar=GCS_JAR,
        job_name='{{task.task_id}}22222255sss{{ macros.uuid.uuid4() }}',
        options={
            'output': GCS_OUTPUT,
        },
        poll_sleep=10,
        job_class='org.apache.beam.examples.WordCount',
        check_if_running=CheckJobRunning.WaitForRun,
    )
    # [END howto_operator_start_java_job]

    # [START howto_operator_start_python_job]
    start_python_job = DataflowCreatePythonJobOperator(
        task_id="start-python-job",
        py_file='apache_beam.examples.wordcount',
        py_options=['-m'],
        job_name='{{task.task_id}}',
        options={
            'output': GCS_OUTPUT,
        },
    )
    # [END howto_operator_start_python_job]

    start_template_job = DataflowTemplatedJobStartOperator(
        task_id="start-template-job",
        template='gs://dataflow-templates/latest/Word_Count',
        parameters={
            'inputFile': "gs://dataflow-samples/shakespeare/kinglear.txt",
            'output': GCS_OUTPUT
        },
    )
