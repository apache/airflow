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
Example Airflow DAG for Google Vertex AI service testing Auto ML operations.
"""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from google.cloud.aiplatform import schema
from google.protobuf.struct_pb2 import Value

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.operators.vertex_ai.auto_ml import (
    CreateAutoMLTextTrainingJobOperator,
    DeleteAutoMLTrainingJobOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.dataset import (
    CreateDatasetOperator,
    DeleteDatasetOperator,
    ImportDataOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
DAG_ID = "vertex_ai_auto_ml_operations"
REGION = "us-central1"
TEXT_DISPLAY_NAME = f"auto-ml-text-{ENV_ID}"
MODEL_DISPLAY_NAME = f"auto-ml-text-model-{ENV_ID}"

TEXT_GCS_BUCKET_NAME = f"bucket_text_{DAG_ID}_{ENV_ID}"

RESOURCES_PATH = Path(__file__).parent / "resources"
TEXT_ZIP_CSV_FILE_LOCAL_PATH = str(RESOURCES_PATH / "text-dataset.csv.zip")
TEXT_GCS_OBJECT_NAME = "vertex-ai/text-dataset.csv"
TEXT_CSV_FILE_LOCAL_PATH = "/text/text-dataset.csv"

TEXT_DATASET = {
    "display_name": f"text-dataset-{ENV_ID}",
    "metadata_schema_uri": schema.dataset.metadata.text,
    "metadata": Value(string_value="text-dataset"),
}
TEXT_DATA_CONFIG = [
    {
        "import_schema_uri": schema.dataset.ioformat.text.single_label_classification,
        "gcs_source": {"uris": [f"gs://{TEXT_GCS_BUCKET_NAME}/vertex-ai/text-dataset.csv"]},
    },
]

with models.DAG(
    f"{DAG_ID}_text_training_job",
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "vertex_ai", "auto_ml"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=TEXT_GCS_BUCKET_NAME,
        storage_class="REGIONAL",
        location=REGION,
    )

    unzip_file = BashOperator(
        task_id="unzip_csv_data_file",
        bash_command=f"unzip {TEXT_ZIP_CSV_FILE_LOCAL_PATH} -d /text/",
    )

    upload_files = LocalFilesystemToGCSOperator(
        task_id="upload_file_to_bucket",
        src=TEXT_CSV_FILE_LOCAL_PATH,
        dst=TEXT_GCS_OBJECT_NAME,
        bucket=TEXT_GCS_BUCKET_NAME,
    )

    create_text_dataset = CreateDatasetOperator(
        task_id="text_dataset",
        dataset=TEXT_DATASET,
        region=REGION,
        project_id=PROJECT_ID,
    )
    text_dataset_id = create_text_dataset.output["dataset_id"]

    import_text_dataset = ImportDataOperator(
        task_id="import_text_data",
        dataset_id=text_dataset_id,
        region=REGION,
        project_id=PROJECT_ID,
        import_configs=TEXT_DATA_CONFIG,
    )

    # [START how_to_cloud_vertex_ai_create_auto_ml_text_training_job_operator]
    create_auto_ml_text_training_job = CreateAutoMLTextTrainingJobOperator(
        task_id="auto_ml_text_task",
        display_name=TEXT_DISPLAY_NAME,
        prediction_type="classification",
        multi_label=False,
        dataset_id=text_dataset_id,
        model_display_name=MODEL_DISPLAY_NAME,
        training_fraction_split=0.7,
        validation_fraction_split=0.2,
        test_fraction_split=0.1,
        sync=True,
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_create_auto_ml_text_training_job_operator]

    delete_auto_ml_text_training_job = DeleteAutoMLTrainingJobOperator(
        task_id="delete_auto_ml_text_training_job",
        training_pipeline_id=create_auto_ml_text_training_job.output["training_id"],
        region=REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_text_dataset = DeleteDatasetOperator(
        task_id="delete_text_dataset",
        dataset_id=text_dataset_id,
        region=REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=TEXT_GCS_BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    clear_folder = BashOperator(
        task_id="clear_folder",
        bash_command="rm -r /text/*",
    )

    (
        # TEST SETUP
        [
            create_bucket,
            create_text_dataset,
        ]
        >> unzip_file
        >> upload_files
        >> import_text_dataset
        # TEST BODY
        >> create_auto_ml_text_training_job
        # TEST TEARDOWN
        >> delete_auto_ml_text_training_job
        >> delete_text_dataset
        >> delete_bucket
        >> clear_folder
    )


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
