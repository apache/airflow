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
Example Airflow DAG that translates text in Google Cloud Translate using V3 API version
service in the Google Cloud.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.operators.translate import (
    TranslateCreateDatasetOperator,
    TranslateDatasetsListOperator,
    TranslateDeleteDatasetOperator,
    TranslateImportDataOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

DAG_ID = "gcp_translate_automl_native_dataset"
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
REGION = "us-central1"
RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"

DATA_SAMPLE_GCS_BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("_", "-")

DATA_FILE_NAME = "import_en-es.tsv"

RESOURCE_PATH = f"V3_translate/create_ds/import_data/{DATA_FILE_NAME}"
COPY_DATA_PATH = f"gs://{RESOURCE_DATA_BUCKET}/V3_translate/create_ds/import_data/{DATA_FILE_NAME}"

DST_PATH = f"translate/import/{DATA_FILE_NAME}"

DATASET_DATA_PATH = f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}/{DST_PATH}"


DATASET = {
    "display_name": f"op_ds_native{DAG_ID}_{ENV_ID}",
    "source_language_code": "es",
    "target_language_code": "en",
}

with DAG(
    DAG_ID,
    schedule="@once",  # Override to match your needs
    start_date=datetime(2024, 11, 1),
    catchup=False,
    tags=[
        "example",
        "translate_dataset",
    ],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        storage_class="REGIONAL",
        location=REGION,
    )
    copy_dataset_source_tsv = GCSToGCSOperator(
        task_id="copy_dataset_file",
        source_bucket=RESOURCE_DATA_BUCKET,
        source_object=RESOURCE_PATH,
        destination_bucket=DATA_SAMPLE_GCS_BUCKET_NAME,
        destination_object=DST_PATH,
    )

    # [START howto_operator_translate_automl_create_dataset]
    create_dataset_op = TranslateCreateDatasetOperator(
        task_id="translate_v3_ds_create",
        dataset=DATASET,
        project_id=PROJECT_ID,
        location=REGION,
    )
    # [END howto_operator_translate_automl_create_dataset]

    # [START howto_operator_translate_automl_import_data]
    import_ds_data_op = TranslateImportDataOperator(
        task_id="translate_v3_ds_import_data",
        dataset_id=create_dataset_op.output["dataset_id"],
        input_config={
            "input_files": [{"usage": "UNASSIGNED", "gcs_source": {"input_uri": DATASET_DATA_PATH}}]
        },
        project_id=PROJECT_ID,
        location=REGION,
    )
    # [END howto_operator_translate_automl_import_data]

    # [START howto_operator_translate_automl_list_datasets]
    list_datasets_op = TranslateDatasetsListOperator(
        task_id="translate_v3_list_ds",
        project_id=PROJECT_ID,
        location=REGION,
    )
    # [END howto_operator_translate_automl_list_datasets]

    # [START howto_operator_translate_automl_delete_dataset]
    delete_ds_op = TranslateDeleteDatasetOperator(
        task_id="translate_v3_ds_delete",
        dataset_id=create_dataset_op.output["dataset_id"],
        project_id=PROJECT_ID,
        location=REGION,
    )
    # [END howto_operator_translate_automl_delete_dataset]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        # TEST SETUP
        [create_bucket >> copy_dataset_source_tsv]
        # TEST BODY
        >> create_dataset_op
        >> import_ds_data_op
        >> list_datasets_op
        >> delete_ds_op
        # TEST TEARDOWN
        >> delete_bucket
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
