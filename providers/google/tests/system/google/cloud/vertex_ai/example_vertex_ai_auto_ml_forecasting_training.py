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
Example Airflow DAG for Google Vertex AI service testing Auto ML operations.
"""

from __future__ import annotations

import os
from datetime import datetime

from google.cloud.aiplatform import schema
from google.protobuf.json_format import ParseDict
from google.protobuf.struct_pb2 import Value

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.vertex_ai.auto_ml import (
    CreateAutoMLForecastingTrainingJobOperator,
    DeleteAutoMLTrainingJobOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.dataset import (
    CreateDatasetOperator,
    DeleteDatasetOperator,
)

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
DAG_ID = "vertex_ai_auto_ml_operations"
REGION = "us-central1"
FORECASTING_DISPLAY_NAME = f"auto-ml-forecasting-{ENV_ID}"
MODEL_DISPLAY_NAME = f"auto-ml-forecasting-model-{ENV_ID}"

RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"

FORECAST_DATASET = {
    "display_name": f"forecast-dataset-{ENV_ID}",
    "metadata_schema_uri": schema.dataset.metadata.time_series,
    "metadata": ParseDict(
        {
            "input_config": {
                "gcs_source": {
                    "uri": [f"gs://{RESOURCE_DATA_BUCKET}/vertex-ai/datasets/forecast-dataset.csv"]
                }
            }
        },
        Value(),
    ),
}

TEST_TIME_COLUMN = "date"
TEST_TIME_SERIES_IDENTIFIER_COLUMN = "store_name"
TEST_TARGET_COLUMN = "sale_dollars"

COLUMN_SPECS = {
    TEST_TIME_COLUMN: "timestamp",
    TEST_TARGET_COLUMN: "numeric",
    "city": "categorical",
    "zip_code": "categorical",
    "county": "categorical",
}


with DAG(
    f"{DAG_ID}_forecasting_training_job",
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "vertex_ai", "auto_ml"],
) as dag:
    create_forecast_dataset = CreateDatasetOperator(
        task_id="forecast_dataset",
        dataset=FORECAST_DATASET,
        region=REGION,
        project_id=PROJECT_ID,
    )
    forecast_dataset_id = create_forecast_dataset.output["dataset_id"]

    # [START how_to_cloud_vertex_ai_create_auto_ml_forecasting_training_job_operator]
    create_auto_ml_forecasting_training_job = CreateAutoMLForecastingTrainingJobOperator(
        task_id="auto_ml_forecasting_task",
        display_name=FORECASTING_DISPLAY_NAME,
        optimization_objective="minimize-rmse",
        column_specs=COLUMN_SPECS,
        # run params
        dataset_id=forecast_dataset_id,
        target_column=TEST_TARGET_COLUMN,
        time_column=TEST_TIME_COLUMN,
        time_series_identifier_column=TEST_TIME_SERIES_IDENTIFIER_COLUMN,
        available_at_forecast_columns=[TEST_TIME_COLUMN],
        unavailable_at_forecast_columns=[TEST_TARGET_COLUMN],
        time_series_attribute_columns=["city", "zip_code", "county"],
        forecast_horizon=30,
        context_window=30,
        data_granularity_unit="day",
        data_granularity_count=1,
        weight_column=None,
        budget_milli_node_hours=1000,
        model_display_name=MODEL_DISPLAY_NAME,
        predefined_split_column_name=None,
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_create_auto_ml_forecasting_training_job_operator]

    # [START how_to_cloud_vertex_ai_delete_auto_ml_training_job_operator]
    delete_auto_ml_forecasting_training_job = DeleteAutoMLTrainingJobOperator(
        task_id="delete_auto_ml_forecasting_training_job",
        training_pipeline_id="{{ task_instance.xcom_pull(task_ids='auto_ml_forecasting_task', "
        "key='training_id') }}",
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_delete_auto_ml_training_job_operator]

    delete_forecast_dataset = DeleteDatasetOperator(
        task_id="delete_forecast_dataset",
        dataset_id=forecast_dataset_id,
        region=REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        # TEST SETUP
        create_forecast_dataset
        # TEST BODY
        >> create_auto_ml_forecasting_training_job
        # TEST TEARDOWN
        >> delete_auto_ml_forecasting_training_job
        >> delete_forecast_dataset
    )

    # ### Everything below this line is not part of example ###
    # ### Just for system tests purpose ###
    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
