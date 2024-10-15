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
    CreateAutoMLTabularTrainingJobOperator,
    DeleteAutoMLTrainingJobOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.dataset import (
    CreateDatasetOperator,
    DeleteDatasetOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
DAG_ID = "vertex_ai_auto_ml_operations"
REGION = "us-central1"
TABULAR_DISPLAY_NAME = f"auto-ml-tabular-{ENV_ID}"
MODEL_DISPLAY_NAME = "adopted-prediction-model"

RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"

TABULAR_DATASET = {
    "display_name": f"tabular-dataset-{ENV_ID}",
    "metadata_schema_uri": schema.dataset.metadata.tabular,
    "metadata": ParseDict(
        {
            "input_config": {
                "gcs_source": {"uri": [f"gs://{RESOURCE_DATA_BUCKET}/vertex-ai/datasets/tabular-dataset.csv"]}
            }
        },
        Value(),
    ),
}

COLUMN_TRANSFORMATIONS = [
    {"categorical": {"column_name": "Type"}},
    {"numeric": {"column_name": "Age"}},
    {"categorical": {"column_name": "Breed1"}},
    {"categorical": {"column_name": "Color1"}},
    {"categorical": {"column_name": "Color2"}},
    {"categorical": {"column_name": "MaturitySize"}},
    {"categorical": {"column_name": "FurLength"}},
    {"categorical": {"column_name": "Vaccinated"}},
    {"categorical": {"column_name": "Sterilized"}},
    {"categorical": {"column_name": "Health"}},
    {"numeric": {"column_name": "Fee"}},
    {"numeric": {"column_name": "PhotoAmt"}},
]

with DAG(
    f"{DAG_ID}_tabular_training_job",
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "vertex_ai", "auto_ml"],
) as dag:
    create_tabular_dataset = CreateDatasetOperator(
        task_id="tabular_dataset",
        dataset=TABULAR_DATASET,
        region=REGION,
        project_id=PROJECT_ID,
    )
    tabular_dataset_id = create_tabular_dataset.output["dataset_id"]

    # [START how_to_cloud_vertex_ai_create_auto_ml_tabular_training_job_operator]
    create_auto_ml_tabular_training_job = CreateAutoMLTabularTrainingJobOperator(
        task_id="auto_ml_tabular_task",
        display_name=TABULAR_DISPLAY_NAME,
        optimization_prediction_type="classification",
        column_transformations=COLUMN_TRANSFORMATIONS,
        dataset_id=tabular_dataset_id,
        target_column="Adopted",
        training_fraction_split=0.8,
        validation_fraction_split=0.1,
        test_fraction_split=0.1,
        model_display_name=MODEL_DISPLAY_NAME,
        disable_early_stopping=False,
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_create_auto_ml_tabular_training_job_operator]

    delete_auto_ml_tabular_training_job = DeleteAutoMLTrainingJobOperator(
        task_id="delete_auto_ml_training_job",
        training_pipeline_id="{{ task_instance.xcom_pull(task_ids='auto_ml_tabular_task', "
        "key='training_id') }}",
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

    (
        # TEST SETUP
        create_tabular_dataset
        # TEST BODY
        >> create_auto_ml_tabular_training_job
        # TEST TEARDOWN
        >> delete_auto_ml_tabular_training_job
        >> delete_tabular_dataset
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
