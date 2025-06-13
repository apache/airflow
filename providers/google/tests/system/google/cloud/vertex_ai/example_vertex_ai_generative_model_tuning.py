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

import requests

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.vertex_ai.generative_model import (
    SupervisedFineTuningTrainOperator,
)


def get_actual_model() -> str:
    source_model: str | None = None
    try:
        response = requests.get(
            "https://generativelanguage.googleapis.com/v1/models", {"key": GEMINI_API_KEY}
        )
        response.raise_for_status()
        available_models = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching models from API: {e}")
        return ""
    for model in available_models.get("models", []):
        try:
            model_name = model["name"].split("/")[-1]
            splited_model_name = model_name.split("-")
            if not splited_model_name[-1].isdigit():
                # We are not using model aliases because sometimes it is not guaranteed to work
                continue
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
DAG_ID = "vertex_ai_generative_model_tuning_dag"
REGION = "us-central1"
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
SOURCE_MODEL = get_actual_model()
TRAIN_DATASET = "gs://cloud-samples-data/ai-platform/generative_ai/gemini-2_0/text/sft_train_data.jsonl"
TUNED_MODEL_DISPLAY_NAME = "my_tuned_gemini_model"

with DAG(
    dag_id=DAG_ID,
    description="Sample DAG with generative model tuning tasks.",
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "vertex_ai", "generative_model"],
) as dag:
    # [START how_to_cloud_vertex_ai_supervised_fine_tuning_train_operator]
    sft_train_task = SupervisedFineTuningTrainOperator(
        task_id="sft_train_task",
        project_id=PROJECT_ID,
        location=REGION,
        source_model=SOURCE_MODEL,
        train_dataset=TRAIN_DATASET,
        tuned_model_display_name=TUNED_MODEL_DISPLAY_NAME,
    )
    # [END how_to_cloud_vertex_ai_supervised_fine_tuning_train_operator]

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
