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

# mypy ignore arg types (for templated fields)
# type: ignore[arg-type]

"""
Example Airflow DAG for Google Vertex AI service testing Custom Jobs operations.
"""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from google.cloud.aiplatform import schema
from google.protobuf.json_format import ParseDict
from google.protobuf.struct_pb2 import Value

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.operators.vertex_ai.custom_job import (
    CreateCustomTrainingJobOperator,
    DeleteCustomTrainingJobOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.dataset import (
    CreateDatasetOperator,
    DeleteDatasetOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "vertex_ai_custom_job_operations"
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
REGION = "us-central1"
CUSTOM_DISPLAY_NAME = f"train-housing-custom-{ENV_ID}"
MODEL_DISPLAY_NAME = f"custom-housing-model-{ENV_ID}"

CUSTOM_GCS_BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"

DATA_SAMPLE_GCS_OBJECT_NAME = "vertex-ai/california_housing_train.csv"
CSV_ZIP_FILE_LOCAL_PATH = str(Path(__file__).parent / "resources" / "California-housing.zip")
CSV_FILE_LOCAL_PATH = "/custom-job/california_housing_train.csv"

TABULAR_DATASET = lambda bucket_name: {
    "display_name": f"tabular-dataset-{ENV_ID}",
    "metadata_schema_uri": schema.dataset.metadata.tabular,
    "metadata": ParseDict(
        {"input_config": {"gcs_source": {"uri": [f"gs://{bucket_name}/{DATA_SAMPLE_GCS_OBJECT_NAME}"]}}},
        Value(),
    ),
}

CONTAINER_URI = "gcr.io/cloud-aiplatform/training/tf-cpu.2-2:latest"
MODEL_SERVING_CONTAINER_URI = "gcr.io/cloud-aiplatform/prediction/tf2-cpu.2-2:latest"
REPLICA_COUNT = 1

LOCAL_TRAINING_SCRIPT_PATH = "/custom-job/california_housing_training_script.py"


with models.DAG(
    f"{DAG_ID}_custom",
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "vertex_ai", "custom_job"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=CUSTOM_GCS_BUCKET_NAME,
        storage_class="REGIONAL",
        location=REGION,
    )
    unzip_file = BashOperator(
        task_id="unzip_csv_data_file",
        bash_command=f"mkdir -p /custom-job && unzip {CSV_ZIP_FILE_LOCAL_PATH} -d /custom-job/",
    )
    upload_files = LocalFilesystemToGCSOperator(
        task_id="upload_file_to_bucket",
        src=CSV_FILE_LOCAL_PATH,
        dst=DATA_SAMPLE_GCS_OBJECT_NAME,
        bucket=CUSTOM_GCS_BUCKET_NAME,
    )
    create_tabular_dataset = CreateDatasetOperator(
        task_id="tabular_dataset",
        dataset=TABULAR_DATASET(CUSTOM_GCS_BUCKET_NAME),
        region=REGION,
        project_id=PROJECT_ID,
    )
    tabular_dataset_id = create_tabular_dataset.output["dataset_id"]

    # [START how_to_cloud_vertex_ai_create_custom_training_job_operator]
    create_custom_training_job = CreateCustomTrainingJobOperator(
        task_id="custom_task",
        staging_bucket=f"gs://{CUSTOM_GCS_BUCKET_NAME}",
        display_name=CUSTOM_DISPLAY_NAME,
        script_path=LOCAL_TRAINING_SCRIPT_PATH,
        container_uri=CONTAINER_URI,
        requirements=["gcsfs==0.7.1"],
        model_serving_container_image_uri=MODEL_SERVING_CONTAINER_URI,
        # run params
        dataset_id=tabular_dataset_id,
        replica_count=REPLICA_COUNT,
        model_display_name=MODEL_DISPLAY_NAME,
        sync=False,
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_create_custom_training_job_operator]

    # [START how_to_cloud_vertex_ai_delete_custom_training_job_operator]
    delete_custom_training_job = DeleteCustomTrainingJobOperator(
        task_id="delete_custom_training_job",
        training_pipeline_id=create_custom_training_job.output["training_id"],
        custom_job_id=create_custom_training_job.output["custom_job_id"],
        region=REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END how_to_cloud_vertex_ai_delete_custom_training_job_operator]

    delete_tabular_dataset = DeleteDatasetOperator(
        task_id="delete_tabular_dataset",
        dataset_id=tabular_dataset_id,
        region=REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=CUSTOM_GCS_BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    clear_folder = BashOperator(
        task_id="clear_folder",
        bash_command="rm -r /custom-job/*",
    )

    (
        # TEST SETUP
        create_bucket
        >> unzip_file
        >> upload_files
        >> create_tabular_dataset
        # TEST BODY
        >> create_custom_training_job
        # TEST TEARDOWN
        >> delete_custom_training_job
        >> delete_tabular_dataset
        >> delete_bucket
        >> clear_folder
    )


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
