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
Example Airflow DAG for Google Vertex AI Generative Model Tuning Tasks.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import requests

from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

try:
    from airflow.sdk import task
except ImportError:
    # Airflow 2 path
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]
from google.genai.types import TuningDataset

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.gen_ai import (
    GenAISupervisedFineTuningTrainOperator,
)
from airflow.providers.google.common.utils.get_secret import get_secret


def _get_actual_model(key) -> str:
    source_model: str | None = None
    try:
        response = requests.get("https://generativelanguage.googleapis.com/v1/models", {"key": key})
        response.raise_for_status()
        available_models = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching models from API: {e}")
        return ""
    for model in available_models.get("models", []):
        try:
            model_name = model["name"].split("/")[-1]
            splited_model_name = model_name.split("-")
            if not source_model and "flash" in model_name:
                source_model = model_name
            elif (
                source_model
                and "flash" in model_name
                and float(source_model.split("-")[1]) < float(splited_model_name[1])
            ):
                source_model = model_name
            elif (
                source_model
                and "flash" in model_name
                and (
                    float(source_model.split("-")[1]) == float(splited_model_name[1])
                    and int(splited_model_name[-1]) > int(source_model.split("-")[-1])
                )
            ):
                source_model = model_name
        except (ValueError, IndexError) as e:
            print(f"Could not parse model name '{model.get('name')}'. Skipping. Error: {e}")
            continue
    if not source_model:
        raise ValueError("Source model not found")
    return source_model


PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
DAG_ID = "gen_ai_generative_model_tuning_dag"
REGION = "us-central1"
GEMINI_API_KEY = "api_key"
SOURCE_MODEL = "{{ task_instance.xcom_pull('get_actual_model') }}"
TRAIN_DATASET = TuningDataset(
    gcs_uri="gs://cloud-samples-data/ai-platform/generative_ai/gemini-1_5/text/sft_train_data.jsonl",
)
TUNED_MODEL_DISPLAY_NAME = "my_tuned_gemini_model"
TUNING_JOB_CONFIG = {"tuned_model_display_name": TUNED_MODEL_DISPLAY_NAME}
TUNED_VIDEO_MODEL_DISPLAY_NAME = "my_tuned_gemini_video_model"
TUNING_JOB_VIDEO_MODEL_CONFIG = {"tuned_model_display_name": TUNED_VIDEO_MODEL_DISPLAY_NAME}

BUCKET_NAME = f"bucket_tuning_dag_{PROJECT_ID}"
FILE_NAME = "video_tuning_dataset.jsonl"
UPLOAD_FILE_PATH = str(Path(__file__).parent / "resources" / FILE_NAME)
TRAIN_VIDEO_DATASET = TuningDataset(gcs_uri=f"gs://{BUCKET_NAME}/{FILE_NAME}")


with DAG(
    dag_id=DAG_ID,
    description="Sample DAG with generative model tuning tasks.",
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "vertex_ai", "generative_model"],
    render_template_as_native_obj=True,
) as dag:

    @task
    def get_gemini_api_key():
        return get_secret(GEMINI_API_KEY)

    get_gemini_api_key_task = get_gemini_api_key()

    @task
    def get_actual_model(key):
        return _get_actual_model(key)

    get_actual_model_task = get_actual_model(get_gemini_api_key_task)

    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=BUCKET_NAME,
        project_id=PROJECT_ID,
    )

    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=UPLOAD_FILE_PATH,
        dst=FILE_NAME,
        bucket=BUCKET_NAME,
    )

    delete_bucket = GCSDeleteBucketOperator(task_id="delete_bucket", bucket_name=BUCKET_NAME)

    # [START how_to_cloud_gen_ai_supervised_fine_tuning_train_operator]
    sft_train_task = GenAISupervisedFineTuningTrainOperator(
        task_id="sft_train_task",
        project_id=PROJECT_ID,
        location=REGION,
        source_model=SOURCE_MODEL,
        training_dataset=TRAIN_DATASET,
        tuning_job_config=TUNING_JOB_CONFIG,
    )
    # [END how_to_cloud_gen_ai_supervised_fine_tuning_train_operator]

    # [START how_to_cloud_gen_ai_supervised_fine_tuning_train_operator_for_video]
    sft_video_task = GenAISupervisedFineTuningTrainOperator(
        task_id="sft_train_video_task",
        project_id=PROJECT_ID,
        location=REGION,
        source_model=SOURCE_MODEL,
        training_dataset=TRAIN_VIDEO_DATASET,
        tuning_job_config=TUNING_JOB_VIDEO_MODEL_CONFIG,
    )
    # [END how_to_cloud_gen_ai_supervised_fine_tuning_train_operator_for_video]

    delete_bucket.trigger_rule = TriggerRule.ALL_DONE

    (
        get_gemini_api_key_task
        >> get_actual_model_task
        >> create_bucket
        >> upload_file
        >> [sft_train_task, sft_video_task]
        >> delete_bucket
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
