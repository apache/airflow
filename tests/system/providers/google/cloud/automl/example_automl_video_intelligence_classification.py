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
    GCSSynchronizeBucketsOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "example_automl_video_clss"
GCP_PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
GCP_AUTOML_LOCATION = "us-central1"
DATA_SAMPLE_GCS_BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("_", "-")
RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"

MODEL_NAME = "video_clss_test_model"
MODEL = {
    "display_name": MODEL_NAME,
    "video_classification_model_metadata": {},
}

DATASET_NAME = f"ds_video_clss_{ENV_ID}".replace("-", "_")
DATASET = {
    "display_name": DATASET_NAME,
    "video_classification_dataset_metadata": {},
}

AUTOML_DATASET_BUCKET = f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}/automl/video_classification.csv"
IMPORT_INPUT_CONFIG = {"gcs_source": {"input_uris": [AUTOML_DATASET_BUCKET]}}


extract_object_id = CloudAutoMLHook.extract_object_id


# Example DAG for AutoML Video Intelligence Classification
with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    user_defined_macros={"extract_object_id": extract_object_id},
    tags=["example", "automl", "video-clss"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        storage_class="REGIONAL",
        location=GCP_AUTOML_LOCATION,
    )

    move_dataset_file = GCSSynchronizeBucketsOperator(
        task_id="move_dataset_to_bucket",
        source_bucket=RESOURCE_DATA_BUCKET,
        source_object="automl/datasets/video",
        destination_bucket=DATA_SAMPLE_GCS_BUCKET_NAME,
        destination_object="automl",
        recursive=True,
    )

    create_dataset = AutoMLCreateDatasetOperator(
        task_id="create_dataset", dataset=DATASET, location=GCP_AUTOML_LOCATION
    )

    dataset_id = cast(str, XComArg(create_dataset, key="dataset_id"))
    MODEL["dataset_id"] = dataset_id

    import_dataset = AutoMLImportDataOperator(
        task_id="import_dataset",
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
    )

    delete_dataset = AutoMLDeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id=dataset_id,
        location=GCP_AUTOML_LOCATION,
        project_id=GCP_PROJECT_ID,
    )

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        # TEST SETUP
        [create_bucket >> move_dataset_file, create_dataset]
        # TEST BODY
        >> import_dataset
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
