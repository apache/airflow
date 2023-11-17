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

from google.cloud.aiplatform import schema
from google.protobuf.struct_pb2 import Value

from airflow.models.dag import DAG
from airflow.models.xcom_arg import XComArg
from airflow.providers.google.cloud.hooks.automl import CloudAutoMLHook
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
    GCSSynchronizeBucketsOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.auto_ml import (
    CreateAutoMLTextTrainingJobOperator,
    DeleteAutoMLTrainingJobOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.dataset import (
    CreateDatasetOperator,
    DeleteDatasetOperator,
    ImportDataOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
GCP_PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
DAG_ID = "example_automl_text_sent"
GCP_AUTOML_LOCATION = "us-central1"
DATA_SAMPLE_GCS_BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("_", "-")
RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"

TEXT_SENT_DISPLAY_NAME = f"{DAG_ID}-{ENV_ID}".replace("_", "-")
AUTOML_DATASET_BUCKET = f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}/automl/sentiment.csv"

MODEL_NAME = f"{DAG_ID}-{ENV_ID}".replace("_", "-")

DATASET_NAME = f"ds_sent_{ENV_ID}".replace("-", "_")
DATASET = {
    "display_name": DATASET_NAME,
    "metadata_schema_uri": schema.dataset.metadata.text,
    "metadata": Value(string_value="sent-dataset"),
}

DATA_CONFIG = [
    {
        "import_schema_uri": schema.dataset.ioformat.text.sentiment,
        "gcs_source": {"uris": [AUTOML_DATASET_BUCKET]},
    },
]

extract_object_id = CloudAutoMLHook.extract_object_id

# Example DAG for AutoML Natural Language Text Sentiment
with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    user_defined_macros={"extract_object_id": extract_object_id},
    tags=["example", "automl", "text-sentiment"],
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
        source_object="vertex-ai/automl/datasets/text",
        destination_bucket=DATA_SAMPLE_GCS_BUCKET_NAME,
        destination_object="automl",
        recursive=True,
    )

    create_sent_dataset = CreateDatasetOperator(
        task_id="create_sent_dataset",
        dataset=DATASET,
        region=GCP_AUTOML_LOCATION,
        project_id=GCP_PROJECT_ID,
    )
    sent_dataset_id = create_sent_dataset.output["dataset_id"]

    import_sent_dataset = ImportDataOperator(
        task_id="import_sent_data",
        dataset_id=sent_dataset_id,
        region=GCP_AUTOML_LOCATION,
        project_id=GCP_PROJECT_ID,
        import_configs=DATA_CONFIG,
    )

    # [START howto_operator_automl_create_model]
    create_sent_training_job = CreateAutoMLTextTrainingJobOperator(
        task_id="create_sent_training_job",
        display_name=TEXT_SENT_DISPLAY_NAME,
        prediction_type="sentiment",
        multi_label=False,
        dataset_id=sent_dataset_id,
        model_display_name=MODEL_NAME,
        training_fraction_split=0.7,
        validation_fraction_split=0.2,
        test_fraction_split=0.1,
        sentiment_max=5,
        sync=True,
        region=GCP_AUTOML_LOCATION,
        project_id=GCP_PROJECT_ID,
    )
    # [END howto_operator_automl_create_model]
    model_id = cast(str, XComArg(create_sent_training_job, key="model_id"))

    delete_sent_training_job = DeleteAutoMLTrainingJobOperator(
        task_id="delete_sent_training_job",
        training_pipeline_id=create_sent_training_job.output["training_id"],
        region=GCP_AUTOML_LOCATION,
        project_id=GCP_PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_sent_dataset = DeleteDatasetOperator(
        task_id="delete_sent_dataset",
        dataset_id=sent_dataset_id,
        region=GCP_AUTOML_LOCATION,
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
        [create_bucket >> move_dataset_file, create_sent_dataset]
        # TEST BODY
        >> import_sent_dataset
        >> create_sent_training_job
        # TEST TEARDOWN
        >> delete_sent_training_job
        >> delete_sent_dataset
        >> delete_bucket
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
