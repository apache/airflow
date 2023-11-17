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
Example Airflow DAG that uses Google AutoML services.
"""
from __future__ import annotations

import os
from datetime import datetime
from typing import cast

from google.cloud import storage  # type: ignore[attr-defined]

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.models.xcom_arg import XComArg
from airflow.providers.google.cloud.hooks.automl import CloudAutoMLHook
from airflow.providers.google.cloud.operators.automl import (
    AutoMLCreateDatasetOperator,
    AutoMLDeleteDatasetOperator,
    AutoMLDeleteModelOperator,
    AutoMLImportDataOperator,
    AutoMLTrainModelOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "example_automl_vision_clss"
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
GCP_PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
GCP_AUTOML_LOCATION = "us-central1"
DATA_SAMPLE_GCS_BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("_", "-")
RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"


DATASET_NAME = f"ds_vision_clss_{ENV_ID}".replace("-", "_")
DATASET = {
    "display_name": DATASET_NAME,
    "image_classification_dataset_metadata": {"classification_type": "MULTILABEL"},
}
AUTOML_DATASET_BUCKET = f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}/automl/vision_classification.csv"
IMPORT_INPUT_CONFIG = {"gcs_source": {"input_uris": [AUTOML_DATASET_BUCKET]}}

MODEL_NAME = "vision_clss_test_model"
MODEL = {
    "display_name": MODEL_NAME,
    "image_classification_model_metadata": {"train_budget": 1},
}

CSV_FILE_NAME = "vision_classification.csv"
GCS_FILE_PATH = f"automl/datasets/vision/{CSV_FILE_NAME}"
DESTINATION_FILE_PATH = f"/tmp/{CSV_FILE_NAME}"

extract_object_id = CloudAutoMLHook.extract_object_id

# Example DAG for AutoML Vision Classification
with DAG(
    DAG_ID,
    schedule="@once",  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    user_defined_macros={"extract_object_id": extract_object_id},
    tags=["example", "automl", "vision-clss"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        storage_class="REGIONAL",
        location=GCP_AUTOML_LOCATION,
    )

    @task
    def upload_csv_file_to_gcs():
        # download file to local storage
        storage_client = storage.Client()
        bucket = storage_client.bucket(RESOURCE_DATA_BUCKET)
        blob = bucket.blob(GCS_FILE_PATH)
        blob.download_to_filename(DESTINATION_FILE_PATH)

        # update file content
        with open(DESTINATION_FILE_PATH) as file:
            lines = file.readlines()

        updated_lines = [line.replace("template-bucket", DATA_SAMPLE_GCS_BUCKET_NAME) for line in lines]

        with open(DESTINATION_FILE_PATH, "w") as file:
            file.writelines(updated_lines)

        # upload updated file to bucket storage
        destination_bucket = storage_client.bucket(DATA_SAMPLE_GCS_BUCKET_NAME)
        destination_blob = destination_bucket.blob(f"automl/{CSV_FILE_NAME}")
        generation_match_precondition = 0
        destination_blob.upload_from_filename(
            DESTINATION_FILE_PATH, if_generation_match=generation_match_precondition
        )

    upload_csv_file_to_gcs_task = upload_csv_file_to_gcs()

    copy_folder_tasks = [
        GCSToGCSOperator(
            task_id=f"copy_dataset_folder_{folder}",
            source_bucket=RESOURCE_DATA_BUCKET,
            source_object=f"automl/datasets/vision/{folder}",
            destination_bucket=DATA_SAMPLE_GCS_BUCKET_NAME,
            destination_object=f"automl/{folder}",
        )
        for folder in ("cirrus", "cumulonimbus", "cumulus")
    ]

    create_dataset_task = AutoMLCreateDatasetOperator(
        task_id="create_dataset_task",
        dataset=DATASET,
        location=GCP_AUTOML_LOCATION,
    )

    dataset_id = cast(str, XComArg(create_dataset_task, key="dataset_id"))
    import_dataset_task = AutoMLImportDataOperator(
        task_id="import_dataset_task",
        dataset_id=dataset_id,
        location=GCP_AUTOML_LOCATION,
        input_config=IMPORT_INPUT_CONFIG,
    )

    MODEL["dataset_id"] = dataset_id

    create_model = AutoMLTrainModelOperator(task_id="create_model", model=MODEL, location=GCP_AUTOML_LOCATION)
    model_id = cast(str, XComArg(create_model, key="model_id"))

    delete_model = AutoMLDeleteModelOperator(
        task_id="delete_model",
        model_id=model_id,
        location=GCP_AUTOML_LOCATION,
        project_id=GCP_PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_dataset = AutoMLDeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id=dataset_id,
        location=GCP_AUTOML_LOCATION,
        project_id=GCP_PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        # TEST SETUP
        create_bucket
        >> upload_csv_file_to_gcs_task
        >> copy_folder_tasks
        # TEST BODY
        >> create_dataset_task
        >> import_dataset_task
        >> create_model
        # TEST TEARDOWN
        >> delete_model
        >> delete_dataset
        >> delete_bucket
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
