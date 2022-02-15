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

* GCP_VERTEX_AI_BUCKET - Google Cloud Storage bucket where the model will be saved
after training process was finished.
* CUSTOM_CONTAINER_URI - path to container with model.
* PYTHON_PACKAGE_GSC_URI - path to test model in archive.
* LOCAL_TRAINING_SCRIPT_PATH - path to local training script.
* DATASET_ID - ID of dataset which will be used in training process.
"""
import os
from datetime import datetime
from uuid import uuid4

from google.protobuf.struct_pb2 import Value

from airflow import models
from airflow.providers.google.cloud.operators.vertex_ai.custom_job import (
    CreateCustomContainerTrainingJobOperator,
    CreateCustomPythonPackageTrainingJobOperator,
    CreateCustomTrainingJobOperator,
    DeleteCustomTrainingJobOperator,
    ListCustomTrainingJobOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.dataset import (
    CreateDatasetOperator,
    DeleteDatasetOperator,
    ExportDataOperator,
    GetDatasetOperator,
    ImportDataOperator,
    ListDatasetsOperator,
    UpdateDatasetOperator,
)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "an-id")
REGION = os.environ.get("GCP_LOCATION", "us-central1")
BUCKET = os.environ.get("GCP_VERTEX_AI_BUCKET", "vertex-ai-system-tests")

STAGING_BUCKET = f"gs://{BUCKET}"
DISPLAY_NAME = str(uuid4())  # Create random display name
CONTAINER_URI = "gcr.io/cloud-aiplatform/training/tf-cpu.2-2:latest"
CUSTOM_CONTAINER_URI = os.environ.get("CUSTOM_CONTAINER_URI", "path_to_container_with_model")
MODEL_SERVING_CONTAINER_URI = "gcr.io/cloud-aiplatform/prediction/tf2-cpu.2-2:latest"
REPLICA_COUNT = 1
MACHINE_TYPE = "n1-standard-4"
ACCELERATOR_TYPE = "ACCELERATOR_TYPE_UNSPECIFIED"
ACCELERATOR_COUNT = 0
TRAINING_FRACTION_SPLIT = 0.7
TEST_FRACTION_SPLIT = 0.15
VALIDATION_FRACTION_SPLIT = 0.15

PYTHON_PACKAGE_GCS_URI = os.environ.get("PYTHON_PACKAGE_GSC_URI", "path_to_test_model_in_arch")
PYTHON_MODULE_NAME = "aiplatform_custom_trainer_script.task"

LOCAL_TRAINING_SCRIPT_PATH = os.environ.get("LOCAL_TRAINING_SCRIPT_PATH", "path_to_training_script")

TRAINING_PIPELINE_ID = "test-training-pipeline-id"
CUSTOM_JOB_ID = "test-custom-job-id"

IMAGE_DATASET = {
    "display_name": str(uuid4()),
    "metadata_schema_uri": "gs://google-cloud-aiplatform/schema/dataset/metadata/image_1.0.0.yaml",
    "metadata": Value(string_value="test-image-dataset"),
}
TABULAR_DATASET = {
    "display_name": str(uuid4()),
    "metadata_schema_uri": "gs://google-cloud-aiplatform/schema/dataset/metadata/tabular_1.0.0.yaml",
    "metadata": Value(string_value="test-tabular-dataset"),
}
TEXT_DATASET = {
    "display_name": str(uuid4()),
    "metadata_schema_uri": "gs://google-cloud-aiplatform/schema/dataset/metadata/text_1.0.0.yaml",
    "metadata": Value(string_value="test-text-dataset"),
}
VIDEO_DATASET = {
    "display_name": str(uuid4()),
    "metadata_schema_uri": "gs://google-cloud-aiplatform/schema/dataset/metadata/video_1.0.0.yaml",
    "metadata": Value(string_value="test-video-dataset"),
}
TIME_SERIES_DATASET = {
    "display_name": str(uuid4()),
    "metadata_schema_uri": "gs://google-cloud-aiplatform/schema/dataset/metadata/time_series_1.0.0.yaml",
    "metadata": Value(string_value="test-video-dataset"),
}
DATASET_ID = os.environ.get("DATASET_ID", "test-dataset-id")
TEST_EXPORT_CONFIG = {"gcs_destination": {"output_uri_prefix": "gs://test-vertex-ai-bucket/exports"}}
TEST_IMPORT_CONFIG = [
    {
        "data_item_labels": {
            "test-labels-name": "test-labels-value",
        },
        "import_schema_uri": (
            "gs://google-cloud-aiplatform/schema/dataset/ioformat/image_bounding_box_io_format_1.0.0.yaml"
        ),
        "gcs_source": {
            "uris": ["gs://ucaip-test-us-central1/dataset/salads_oid_ml_use_public_unassigned.jsonl"]
        },
    },
]
DATASET_TO_UPDATE = {"display_name": "test-name"}
TEST_UPDATE_MASK = {"paths": ["displayName"]}

with models.DAG(
    "example_gcp_vertex_ai_custom_jobs",
    schedule_interval="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as custom_jobs_dag:
    # [START how_to_cloud_vertex_ai_create_custom_container_training_job_operator]
    create_custom_container_training_job = CreateCustomContainerTrainingJobOperator(
        task_id="custom_container_task",
        staging_bucket=STAGING_BUCKET,
        display_name=f"train-housing-container-{DISPLAY_NAME}",
        container_uri=CUSTOM_CONTAINER_URI,
        model_serving_container_image_uri=MODEL_SERVING_CONTAINER_URI,
        # run params
        dataset_id=DATASET_ID,
        command=["python3", "task.py"],
        model_display_name=f"container-housing-model-{DISPLAY_NAME}",
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
    create_custom_python_package_training_job = CreateCustomPythonPackageTrainingJobOperator(
        task_id="python_package_task",
        staging_bucket=STAGING_BUCKET,
        display_name=f"train-housing-py-package-{DISPLAY_NAME}",
        python_package_gcs_uri=PYTHON_PACKAGE_GCS_URI,
        python_module_name=PYTHON_MODULE_NAME,
        container_uri=CONTAINER_URI,
        model_serving_container_image_uri=MODEL_SERVING_CONTAINER_URI,
        # run params
        dataset_id=DATASET_ID,
        model_display_name=f"py-package-housing-model-{DISPLAY_NAME}",
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
        display_name=f"train-housing-custom-{DISPLAY_NAME}",
        script_path=LOCAL_TRAINING_SCRIPT_PATH,
        container_uri=CONTAINER_URI,
        requirements=["gcsfs==0.7.1"],
        model_serving_container_image_uri=MODEL_SERVING_CONTAINER_URI,
        # run params
        dataset_id=DATASET_ID,
        replica_count=1,
        model_display_name=f"custom-housing-model-{DISPLAY_NAME}",
        sync=False,
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_create_custom_training_job_operator]

    # [START how_to_cloud_vertex_ai_delete_custom_training_job_operator]
    delete_custom_training_job = DeleteCustomTrainingJobOperator(
        task_id="delete_custom_training_job",
        training_pipeline_id=TRAINING_PIPELINE_ID,
        custom_job_id=CUSTOM_JOB_ID,
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_delete_custom_training_job_operator]

    # [START how_to_cloud_vertex_ai_list_custom_training_job_operator]
    list_custom_training_job = ListCustomTrainingJobOperator(
        task_id="list_custom_training_job",
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_list_custom_training_job_operator]

with models.DAG(
    "example_gcp_vertex_ai_dataset",
    schedule_interval="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
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

    # [START how_to_cloud_vertex_ai_delete_dataset_operator]
    delete_dataset_job = DeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id=create_text_dataset_job.output['dataset_id'],
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_delete_dataset_operator]

    # [START how_to_cloud_vertex_ai_get_dataset_operator]
    get_dataset = GetDatasetOperator(
        task_id="get_dataset",
        project_id=PROJECT_ID,
        region=REGION,
        dataset_id=create_tabular_dataset_job.output['dataset_id'],
    )
    # [END how_to_cloud_vertex_ai_get_dataset_operator]

    # [START how_to_cloud_vertex_ai_export_data_operator]
    export_data_job = ExportDataOperator(
        task_id="export_data",
        dataset_id=create_image_dataset_job.output['dataset_id'],
        region=REGION,
        project_id=PROJECT_ID,
        export_config=TEST_EXPORT_CONFIG,
    )
    # [END how_to_cloud_vertex_ai_export_data_operator]

    # [START how_to_cloud_vertex_ai_import_data_operator]
    import_data_job = ImportDataOperator(
        task_id="import_data",
        dataset_id=create_image_dataset_job.output['dataset_id'],
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
        project_id=PROJECT_ID,
        region=REGION,
        dataset_id=create_video_dataset_job.output['dataset_id'],
        dataset=DATASET_TO_UPDATE,
        update_mask=TEST_UPDATE_MASK,
    )
    # [END how_to_cloud_vertex_ai_update_dataset_operator]

    create_time_series_dataset_job
    create_text_dataset_job >> delete_dataset_job
    create_tabular_dataset_job >> get_dataset
    create_image_dataset_job >> import_data_job >> export_data_job
    create_video_dataset_job >> update_dataset_job
    list_dataset_job
