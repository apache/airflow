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


"""Example Airflow DAG for Google Vertex AI service testing Custom Jobs operations."""

from __future__ import annotations

import os
from datetime import datetime

from google.cloud.aiplatform import schema
from google.protobuf.json_format import ParseDict
from google.protobuf.struct_pb2 import Value

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
    GCSSynchronizeBucketsOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.custom_job import (
    CreateCustomTrainingJobOperator,
    DeleteCustomTrainingJobOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.dataset import (
    CreateDatasetOperator,
    DeleteDatasetOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
REGION = "us-central1"
CUSTOM_DISPLAY_NAME = f"train-housing-custom-{ENV_ID}"
MODEL_DISPLAY_NAME = f"custom-housing-model-{ENV_ID}"
DAG_ID = "vertex_ai_custom_job_operations"
RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"
CUSTOM_GCS_BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("_", "-")
DATA_SAMPLE_GCS_OBJECT_NAME = "vertex-ai/california_housing_train.csv"


def TABULAR_DATASET(bucket_name):
    return {
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

# LOCAL_TRAINING_SCRIPT_PATH should be set for Airflow which is running on distributed system.
# For example in Composer the correct path is `gcs/data/california_housing_training_script.py`.
# Because `gcs/data/` is shared folder for Airflow's workers.
IS_COMPOSER = bool(os.environ.get("COMPOSER_ENVIRONMENT", ""))
LOCAL_TRAINING_SCRIPT_PATH = "gcs/data/california_housing_training_script.py" if IS_COMPOSER else ""


with DAG(
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

    move_data_files = GCSSynchronizeBucketsOperator(
        task_id="move_files_to_bucket",
        source_bucket=RESOURCE_DATA_BUCKET,
        source_object="vertex-ai/california-housing-data",
        destination_bucket=CUSTOM_GCS_BUCKET_NAME,
        destination_object="vertex-ai",
        recursive=True,
    )

    download_training_script_file = GCSToLocalFilesystemOperator(
        task_id="download_training_script_file",
        object_name="vertex-ai/california_housing_training_script.py",
        bucket=CUSTOM_GCS_BUCKET_NAME,
        filename=LOCAL_TRAINING_SCRIPT_PATH,
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
        region=REGION,
        project_id=PROJECT_ID,
    )

    model_id_v1 = create_custom_training_job.output["model_id"]
    # [END how_to_cloud_vertex_ai_create_custom_training_job_operator]

    # [START how_to_cloud_vertex_ai_create_custom_training_job_operator_deferrable]
    create_custom_training_job_deferrable = CreateCustomTrainingJobOperator(
        task_id="custom_task_deferrable",
        staging_bucket=f"gs://{CUSTOM_GCS_BUCKET_NAME}",
        display_name=f"{CUSTOM_DISPLAY_NAME}-def",
        script_path=LOCAL_TRAINING_SCRIPT_PATH,
        container_uri=CONTAINER_URI,
        requirements=["gcsfs==0.7.1"],
        model_serving_container_image_uri=MODEL_SERVING_CONTAINER_URI,
        # run params
        dataset_id=tabular_dataset_id,
        replica_count=REPLICA_COUNT,
        model_display_name=f"{MODEL_DISPLAY_NAME}-def",
        region=REGION,
        project_id=PROJECT_ID,
        deferrable=True,
    )
    model_id_deferrable_v1 = create_custom_training_job_deferrable.output["model_id"]
    # [END how_to_cloud_vertex_ai_create_custom_training_job_operator_deferrable]

    # [START how_to_cloud_vertex_ai_create_custom_training_job_v2_operator]
    create_custom_training_job_v2 = CreateCustomTrainingJobOperator(
        task_id="custom_task_v2",
        staging_bucket=f"gs://{CUSTOM_GCS_BUCKET_NAME}",
        display_name=CUSTOM_DISPLAY_NAME,
        script_path=LOCAL_TRAINING_SCRIPT_PATH,
        container_uri=CONTAINER_URI,
        requirements=["gcsfs==0.7.1"],
        model_serving_container_image_uri=MODEL_SERVING_CONTAINER_URI,
        parent_model=model_id_v1,
        # run params
        dataset_id=tabular_dataset_id,
        replica_count=REPLICA_COUNT,
        model_display_name=MODEL_DISPLAY_NAME,
        sync=False,
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_create_custom_training_job_v2_operator]

    # [START how_to_cloud_vertex_ai_create_custom_training_job_v2_deferrable_operator]
    create_custom_training_job_deferrable_v2 = CreateCustomTrainingJobOperator(
        task_id="custom_task_deferrable_v2",
        staging_bucket=f"gs://{CUSTOM_GCS_BUCKET_NAME}",
        display_name=f"{CUSTOM_DISPLAY_NAME}-def",
        script_path=LOCAL_TRAINING_SCRIPT_PATH,
        container_uri=CONTAINER_URI,
        requirements=["gcsfs==0.7.1"],
        model_serving_container_image_uri=MODEL_SERVING_CONTAINER_URI,
        parent_model=model_id_deferrable_v1,
        # run params
        dataset_id=tabular_dataset_id,
        replica_count=REPLICA_COUNT,
        model_display_name=f"{MODEL_DISPLAY_NAME}-def",
        sync=False,
        region=REGION,
        project_id=PROJECT_ID,
        deferrable=True,
    )
    # [END how_to_cloud_vertex_ai_create_custom_training_job_v2_deferrable_operator]

    # [START how_to_cloud_vertex_ai_delete_custom_training_job_operator]
    delete_custom_training_job = DeleteCustomTrainingJobOperator(
        task_id="delete_custom_training_job",
        training_pipeline_id="{{ task_instance.xcom_pull(task_ids='custom_task', key='training_id') }}",
        custom_job_id="{{ task_instance.xcom_pull(task_ids='custom_task', key='custom_job_id') }}",
        region=REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END how_to_cloud_vertex_ai_delete_custom_training_job_operator]

    delete_custom_training_job_deferrable = DeleteCustomTrainingJobOperator(
        task_id="delete_custom_training_job_deferrable",
        training_pipeline_id="{{ task_instance.xcom_pull(task_ids='custom_task_deferrable', key='training_id') }}",
        custom_job_id="{{ task_instance.xcom_pull(task_ids='custom_task_deferrable', key='custom_job_id') }}",
        region=REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

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

    (
        chain(
            # TEST SETUP
            create_bucket,
            move_data_files,
            download_training_script_file,
            create_tabular_dataset,
            # TEST BODY
            [create_custom_training_job, create_custom_training_job_deferrable],
            [create_custom_training_job_v2, create_custom_training_job_deferrable_v2],
            # TEST TEARDOWN
            [delete_custom_training_job, delete_custom_training_job_deferrable],
            delete_tabular_dataset,
            delete_bucket,
        )
    )

    # ### Everything below this line is not part of example ###
    # ### Just for system tests purpose ###
    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
