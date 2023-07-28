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
Example Airflow DAG for testing Google Dataflow Beam Pipeline Operator with Java.
Important Note:
    This test downloads Java JAR file from the public bucket. In case the JAR file cannot be downloaded
    or is not compatible with the Java version used in the test, the source code for this test can be
    downloaded from here (https://beam.apache.org/get-started/wordcount-example) and needs to be compiled
    manually in order to work.
    You can follow the instructions on how to pack a self-executing jar here:
    https://beam.apache.org/documentation/runners/dataflow/
Requirements:
    These operators require the gcloud command and Java's JRE to run.
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow import models
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType
from airflow.providers.google.cloud.operators.datapipeline import (
    CreateDataPipelineOperator,
    RunDataPipelineOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "google-datapipeline"

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")
GCP_LOCATION = os.environ.get("location", "us-central1")
PIPELINE_NAME = "defualt-pipeline-name"
PIPELINE_TYPE = "PIPELINE_TYPE_BATCH"

DATAPIPELINES_JOB_NAME = os.environ.get("GCP_DATA_PIPELINES_FLEX_TEMPLATE_JOB_NAME", "default-job-name")

GCS_FLEX_TEMPLATE_TEMPLATE_PATH = os.environ.get(
    "GCP_DATA_PIPELINES_GCS_FLEX_TEMPLATE_TEMPLATE_PATH",
    "gs://INSERT BUCKET/templates/word-count.json",
)

TEMP_LOCATION = os.environ.get("GCP_DATA_PIPELINES_GCS_TEMP_LOCATION", "gs://INSERT BUCKET/temp")
INPUT_FILE = os.environ.get("GCP_DATA_PIPELINES_INPUT_FILE", "gs://INSERT BUCKET/examples/kinglear.txt")
OUTPUT = os.environ.get("GCP_DATA_PIPELINES_OUTPUT", "gs://INSERT BUCKET/results/hello")


with models.DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "datapipeline"],
) as dag:
    # [START howto_operator_create_data_pipeline]
    create_data_pipeline = CreateDataPipelineOperator(
        task_id="create_data_pipeline",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        body={
            "name": "projects/{GCP_PROJECT_ID}/locations/{GCP_LOCATION}/pipelines/{PIPELINE_NAME}",
            "type": PIPELINE_TYPE,
            "workload": {
                "dataflowFlexTemplateRequest": {
                    "launchParameter": {
                        "containerSpecGcsPath": GCS_FLEX_TEMPLATE_TEMPLATE_PATH,
                        "jobName": DATAPIPELINES_JOB_NAME,
                        "environment": {"tempLocation": TEMP_LOCATION},
                        "parameters": {
                            "inputFile": INPUT_FILE,
                            "output": OUTPUT,
                        },
                    },
                    "projectId": GCP_PROJECT_ID,
                    "location": GCP_LOCATION,
                }
            },
        },
    )
    # [END howto_operator_create_data_pipeline]

    create_data_pipeline
