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

"""Example Airflow DAG for Google AutoML service testing dataset operations."""

from __future__ import annotations

import os
from datetime import datetime

from google.cloud import storage  # type: ignore[attr-defined]

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.automl import (
    AutoMLCreateDatasetOperator,
    AutoMLDeleteDatasetOperator,
    AutoMLImportDataOperator,
    AutoMLListDatasetOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "automl_dataset"
GCP_PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")

GCP_AUTOML_LOCATION = "us-central1"
RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"
DATA_SAMPLE_GCS_BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("_", "-")

DATASET_NAME = f"ds_{DAG_ID}_{ENV_ID}".replace("-", "_")
DATASET = {
    "display_name": DATASET_NAME,
    "translation_dataset_metadata": {
        "source_language_code": "en",
        "target_language_code": "es",
    },
}

CSV_FILE_NAME = "en-es.csv"
TSV_FILE_NAME = "en-es.tsv"
GCS_FILE_PATH = f"automl/datasets/translate/{CSV_FILE_NAME}"
AUTOML_DATASET_BUCKET = f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}/automl/{CSV_FILE_NAME}"
IMPORT_INPUT_CONFIG = {"gcs_source": {"input_uris": [AUTOML_DATASET_BUCKET]}}


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "automl", "dataset"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        storage_class="REGIONAL",
        location=GCP_AUTOML_LOCATION,
    )

    @task
    def upload_updated_csv_file_to_gcs():
        # download file into memory
        storage_client = storage.Client()
        bucket = storage_client.bucket(RESOURCE_DATA_BUCKET, GCP_PROJECT_ID)
        blob = bucket.blob(GCS_FILE_PATH)
        contents = blob.download_as_string().decode()

        # update file content
        updated_contents = contents.replace("template-bucket", DATA_SAMPLE_GCS_BUCKET_NAME)

        # upload updated content to bucket
        destination_bucket = storage_client.bucket(DATA_SAMPLE_GCS_BUCKET_NAME)
        destination_blob = destination_bucket.blob(f"automl/{CSV_FILE_NAME}")
        destination_blob.upload_from_string(updated_contents)

    # AutoML requires a .csv file with links to .tsv/.tmx files containing translation training data
    upload_csv_dataset_file = upload_updated_csv_file_to_gcs()

    # The .tsv file contains training data with translated language pairs
    copy_tsv_dataset_file = GCSToGCSOperator(
        task_id="copy_dataset_file",
        source_bucket=RESOURCE_DATA_BUCKET,
        source_object=f"automl/datasets/translate/{TSV_FILE_NAME}",
        destination_bucket=DATA_SAMPLE_GCS_BUCKET_NAME,
        destination_object=f"automl/{TSV_FILE_NAME}",
    )

    # [START howto_operator_automl_create_dataset]
    create_dataset = AutoMLCreateDatasetOperator(
        task_id="create_dataset",
        dataset=DATASET,
        location=GCP_AUTOML_LOCATION,
        project_id=GCP_PROJECT_ID,
    )
    dataset_id = create_dataset.output["dataset_id"]
    # [END howto_operator_automl_create_dataset]

    # [START howto_operator_automl_import_data]
    import_dataset = AutoMLImportDataOperator(
        task_id="import_dataset",
        dataset_id=dataset_id,
        location=GCP_AUTOML_LOCATION,
        input_config=IMPORT_INPUT_CONFIG,
    )
    # [END howto_operator_automl_import_data]

    # [START howto_operator_list_dataset]
    list_datasets = AutoMLListDatasetOperator(
        task_id="list_datasets",
        location=GCP_AUTOML_LOCATION,
        project_id=GCP_PROJECT_ID,
    )
    # [END howto_operator_list_dataset]

    # [START howto_operator_delete_dataset]
    delete_dataset = AutoMLDeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id=dataset_id,
        location=GCP_AUTOML_LOCATION,
        project_id=GCP_PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_delete_dataset]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        # TEST SETUP
        [create_bucket >> upload_csv_dataset_file >> copy_tsv_dataset_file]
        # create_bucket
        >> create_dataset
        # TEST BODY
        >> import_dataset
        >> list_datasets
        # TEST TEARDOWN
        >> delete_dataset
        >> delete_bucket
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
