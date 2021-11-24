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
from airflow.providers.google.cloud.operators.vertex_ai.custom_job import (
    CreateCustomContainerTrainingJobOperator,
    CreateCustomPythonPackageTrainingJobOperator,
    CreateCustomTrainingJobOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.dataset import (
    CreateDatasetOperator,
    DeleteDatasetOperator,
    ExportDataOperator,
    ImportDataOperator,
    ListDatasetsOperator,
    UpdateDatasetOperator,
)
from airflow.utils.dates import days_ago

# from google.cloud import aiplatform


PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "an-id")
REGION = os.environ.get("GCP_LOCATION", "us-central1")
BUCKET = os.environ.get("GCP_VERTEX_AI_BUCKET", "vertex-ai-system-tests")

STAGING_BUCKET = f"gs://{BUCKET}"
BASE_OUTPUT_DIR = f"{STAGING_BUCKET}/models"
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
GCS_DESTINATION = f"gs://{BUCKET}/output-dir/"
PYTHON_PACKAGE = "/files/trainer-0.1.tar.gz"
PYTHON_PACKAGE_CMDARGS = f"--model-dir={GCS_DESTINATION}"
PYTHON_PACKAGE_GCS_URI = "gs://test-vertex-ai-bucket/trainer-0.1.tar.gz"
PYTHON_MODULE_NAME = "trainer.task"

IMAGE_DATASET = {
    "display_name": str(uuid4()),
    "metadata_schema_uri": "gs://google-cloud-aiplatform/schema/dataset/metadata/image_1.0.0.yaml",
    "metadata": "test-image-dataset",
}
TABULAR_DATASET = {
    "display_name": str(uuid4()),
    "metadata_schema_uri": "gs://google-cloud-aiplatform/schema/dataset/metadata/tabular_1.0.0.yaml",
    "metadata": "test-tabular-dataset",
}
TEXT_DATASET = {
    "display_name": str(uuid4()),
    "metadata_schema_uri": "gs://google-cloud-aiplatform/schema/dataset/metadata/text_1.0.0.yaml",
    "metadata": "test-text-dataset",
}
VIDEO_DATASET = {
    "display_name": str(uuid4()),
    "metadata_schema_uri": "gs://google-cloud-aiplatform/schema/dataset/metadata/video_1.0.0.yaml",
    "metadata": "test-video-dataset",
}
TIME_SERIES_DATASET = {
    "display_name": str(uuid4()),
    "metadata_schema_uri": "gs://google-cloud-aiplatform/schema/dataset/metadata/time_series_1.0.0.yaml",
    "metadata": "test-video-dataset",
}
DATASET_ID = "3255741890774958080"
TEST_EXPORT_CONFIG = {"gcs_destination": {"output_uri_prefix": "gs://test-vertex-ai-bucket/exports"}}
TEST_IMPORT_CONFIG = [
    {
        "data_item_labels": {
            "test-labels-name": "test-labels-value",
        },
        "import_schema_uri": "gs://google-cloud-aiplatform/schema/dataset/ioformat/image_bounding_box_io_format_1.0.0.yaml",
        "gcs_source": {
            "uris": ["gs://ucaip-test-us-central1/dataset/salads_oid_ml_use_public_unassigned.jsonl"]
        },
    },
]
TEST_UPDATE_MASK = {
    "paths": "display_name",
}

with models.DAG(
    "example_gcp_vertex_ai_custom_jobs",
    start_date=days_ago(1),
    schedule_interval="@once",
) as custom_jobs_dag:
    # [START how_to_cloud_vertex_ai_create_custom_container_training_job_operator]
    create_custom_container_training_job = CreateCustomContainerTrainingJobOperator(
        task_id="custom_container_task",
        staging_bucket=STAGING_BUCKET,
        display_name=DISPLAY_NAME,
        args=ARGS,
        container_uri=CONTAINER_URI,
        model_serving_container_image_uri=CONTAINER_URI,
        command=COMMAND_2,
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
        base_output_dir=BASE_OUTPUT_DIR,
    )
    # [END how_to_cloud_vertex_ai_create_custom_container_training_job_operator]

    # [START how_to_cloud_vertex_ai_create_custom_python_package_training_job_operator]
    create_custom_python_package_training_job = CreateCustomPythonPackageTrainingJobOperator(
        task_id="python_package_task",
        staging_bucket=STAGING_BUCKET,
        display_name=DISPLAY_NAME_3,
        python_package_gcs_uri=PYTHON_PACKAGE_GCS_URI,
        python_module_name=PYTHON_MODULE_NAME,
        container_uri=CONTAINER_URI,
        args=ARGS,
        model_serving_container_image_uri=CONTAINER_URI,
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
    create_custom_training_job = CreateCustomTrainingJobOperator(
        task_id="custom_task",
        staging_bucket=STAGING_BUCKET,
        display_name=DISPLAY_NAME,
        script_path=PYTHON_PACKAGE,
        args=PYTHON_PACKAGE_CMDARGS,
        container_uri=CONTAINER_URI,
        model_serving_container_image_uri=CONTAINER_URI,
        requirements=[],
        replica_count=1,
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_create_custom_training_job_operator]

with models.DAG(
    "example_gcp_vertex_ai_dataset",
    start_date=days_ago(1),
    schedule_interval="@once",
) as dataset_dag:
    # [START how_to_cloud_vertex_ai_create_dataset_operator]
    create_image_dataset_job = CreateDatasetOperator(
        task_id="image_dataset",
        dataset=IMAGE_DATASET,
        region=REGION,
        project_id=PROJECT_ID,
    )
    create_tabular_dataset_job = CreateDatasetOperator(
        task_id="tabular_dataset",
        dataset=TABULAR_DATASET,
        region=REGION,
        project_id=PROJECT_ID,
    )
    create_text_dataset_job = CreateDatasetOperator(
        task_id="text_dataset",
        dataset=TEXT_DATASET,
        region=REGION,
        project_id=PROJECT_ID,
    )
    create_video_dataset_job = CreateDatasetOperator(
        task_id="video_dataset",
        dataset=VIDEO_DATASET,
        region=REGION,
        project_id=PROJECT_ID,
    )
    create_time_series_dataset_job = CreateDatasetOperator(
        task_id="time_series_dataset",
        dataset=TIME_SERIES_DATASET,
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_create_dataset_operator]

    # # [START how_to_cloud_vertex_ai_delete_dataset_operator]
    delete_dataset_job = DeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id=DATASET_ID,
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_delete_dataset_operator]

    # [START how_to_cloud_vertex_ai_export_data_operator]
    export_data_job = ExportDataOperator(
        task_id="export_data",
        dataset_id="7732319920381231104",
        region=REGION,
        project_id=PROJECT_ID,
        export_config=TEST_EXPORT_CONFIG,
    )
    # [END how_to_cloud_vertex_ai_export_datas_operator]

    # [START how_to_cloud_vertex_ai_import_data_operator]
    import_data_job = ImportDataOperator(
        task_id="import_data",
        dataset_id="7732319920381231104",
        region=REGION,
        project_id=PROJECT_ID,
        import_configs=TEST_IMPORT_CONFIG,
    )
    # [END how_to_cloud_vertex_ai_import_data_operator]

    # [START how_to_cloud_vertex_ai_list_dataset_operator]
    list_dataset_job = ListDatasetsOperator(
        task_id="list_dataset",
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_list_dataset_operator]

    # [START how_to_cloud_vertex_ai_update_dataset_operator]
    update_dataset_job = UpdateDatasetOperator(
        task_id="update_dataset",
        region=REGION,
        dataset=TEXT_DATASET,
        update_mask=TEST_UPDATE_MASK,
    )
    # [END how_to_cloud_vertex_ai_update_dataset_operator]
