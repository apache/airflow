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
Example Airflow DAG that demonstrates operators for the Google Vertex AI service in the Google
Cloud Platform.

This DAG relies on the following OS environment variables:

* GCP_BUCKET_NAME - Google Cloud Storage bucket where the file exists.
"""
import os
from random import randint
from uuid import uuid4

from airflow import models
from airflow.providers.google.cloud.operators.vertex_ai import (
    VertexAICreateCustomContainerTrainingJobOperator,
    VertexAICreateCustomPythonPackageTrainingJobOperator,
    VertexAICreateCustomTrainingJobOperator,
)
from airflow.utils.dates import days_ago

# from google.cloud import aiplatform


PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "an-id")
REGION = os.environ.get("GCP_LOCATION", "europe-west1")
BUCKET = os.environ.get("GCP_VERTEX_AI_BUCKET", "vertex-ai-system-tests")

STAGING_BUCKET = f"gs://{BUCKET}"
DISPLAY_NAME = str(uuid4())  # Create random display name
DISPLAY_NAME_2 = str(uuid4())
DISPLAY_NAME_3 = str(uuid4())
DISPLAY_NAME_4 = str(uuid4())
ARGS = ["--tfds", "tf_flowers:3.*.*"]
CONTAINER_URI = "gcr.io/cloud-aiplatform/training/tf-cpu.2-2:latest"
RESOURCE_ID = str(randint(10000000, 99999999))  # Create random resource ID
REPLICA_COUNT = 1
MACHINE_TYPE = "n1-standard-4"
ACCELERATOR_TYPE = "ACCELERATOR_TYPE_UNSPECIFIED"
ACCELERATOR_COUNT = 0
TRAINING_FRACTION_SPLIT = 0.7
TEST_FRACTION_SPLIT = 0.15
VALIDATION_FRACTION_SPLIT = 0.15
# This example uses an ImageDataset, but you can use another type
# DATASET =  aiplatform.ImageDataset(RESOURCE_ID) if RESOURCE_ID else None
COMMAND = ['python3', 'run_script.py']
COMMAND_2 = ['echo', 'Hello World']
PYTHON_PACKAGE_GCS_URI = "gs://bucket3/custom-training-python-package/my_app/trainer-0.1.tar.gz"
PYTHON_MODULE_NAME = "trainer.task"


with models.DAG(
    "example_gcp_vertex_ai",
    start_date=days_ago(1),
    schedule_interval="@once",
) as dag:
    # [START how_to_cloud_vertex_ai_create_custom_container_training_job_operator]
    create_custom_container_training_job = VertexAICreateCustomContainerTrainingJobOperator(
        task_id="custom_container_task",
        staging_bucket=STAGING_BUCKET,
        display_name=DISPLAY_NAME,
        args=ARGS,
        container_uri=CONTAINER_URI,
        model_serving_container_image_uri=CONTAINER_URI,
        command=COMMAND_2,
        # dataset=DATASET,
        model_display_name=DISPLAY_NAME_2,
        replica_count=REPLICA_COUNT,
        machine_type=MACHINE_TYPE,
        accelerator_type=ACCELERATOR_TYPE,
        accelerator_count=ACCELERATOR_COUNT,
        training_fraction_split=TRAINING_FRACTION_SPLIT,
        validation_fraction_split=VALIDATION_FRACTION_SPLIT,
        test_fraction_split=TEST_FRACTION_SPLIT,
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_create_custom_container_training_job_operator]

    # [START how_to_cloud_vertex_ai_create_custom_python_package_training_job_operator]
    create_custom_python_package_training_job = VertexAICreateCustomPythonPackageTrainingJobOperator(
        task_id="python_package_task",
        staging_bucket=STAGING_BUCKET,
        display_name=DISPLAY_NAME_3,
        python_package_gcs_uri=PYTHON_PACKAGE_GCS_URI,
        python_module_name=PYTHON_MODULE_NAME,
        container_uri=CONTAINER_URI,
        args=ARGS,
        model_serving_container_image_uri=CONTAINER_URI,
        # dataset=DATASET,
        model_display_name=DISPLAY_NAME_4,
        replica_count=REPLICA_COUNT,
        machine_type=MACHINE_TYPE,
        accelerator_type=ACCELERATOR_TYPE,
        accelerator_count=ACCELERATOR_COUNT,
        training_fraction_split=TRAINING_FRACTION_SPLIT,
        validation_fraction_split=VALIDATION_FRACTION_SPLIT,
        test_fraction_split=TEST_FRACTION_SPLIT,
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_create_custom_python_package_training_job_operator]

    # [START how_to_cloud_vertex_ai_create_custom_training_job_operator]
    create_custom_training_job = VertexAICreateCustomTrainingJobOperator(
        task_id="custom_task",
        # TODO: add parameters from example
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_create_custom_training_job_operator]
