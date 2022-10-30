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
from copy import deepcopy
from datetime import datetime
from typing import cast

from airflow import models
from airflow.models.xcom_arg import XComArg
from airflow.providers.google.cloud.hooks.automl import CloudAutoMLHook
from airflow.providers.google.cloud.operators.automl import (
    AutoMLBatchPredictOperator,
    AutoMLCreateDatasetOperator,
    AutoMLDeleteDatasetOperator,
    AutoMLDeleteModelOperator,
    AutoMLDeployModelOperator,
    AutoMLGetModelOperator,
    AutoMLImportDataOperator,
    AutoMLListDatasetOperator,
    AutoMLPredictOperator,
    AutoMLTablesListColumnSpecsOperator,
    AutoMLTablesListTableSpecsOperator,
    AutoMLTablesUpdateDatasetOperator,
    AutoMLTrainModelOperator,
)

START_DATE = datetime(2021, 1, 1)

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-project-id")
GCP_AUTOML_LOCATION = os.environ.get("GCP_AUTOML_LOCATION", "us-central1")
GCP_AUTOML_DATASET_BUCKET = os.environ.get(
    "GCP_AUTOML_DATASET_BUCKET", "gs://INVALID BUCKET NAME/bank-marketing.csv"
)
TARGET = os.environ.get("GCP_AUTOML_TARGET", "Deposit")

# Example values
MODEL_ID = "TBL123456"
DATASET_ID = "TBL123456"

# Example model
MODEL = {
    "display_name": "auto_model_1",
    "dataset_id": DATASET_ID,
    "tables_model_metadata": {"train_budget_milli_node_hours": 1000},
}

# Example dataset
DATASET = {
    "display_name": "test_set",
    "tables_dataset_metadata": {"target_column_spec_id": ""},
}

IMPORT_INPUT_CONFIG = {"gcs_source": {"input_uris": [GCP_AUTOML_DATASET_BUCKET]}}

extract_object_id = CloudAutoMLHook.extract_object_id


def get_target_column_spec(columns_specs: list[dict], column_name: str) -> str:
    """
    Using column name returns spec of the column.
    """
    for column in columns_specs:
        if column["display_name"] == column_name:
            return extract_object_id(column)
    raise Exception(f"Unknown target column: {column_name}")


# Example DAG to create dataset, train model_id and deploy it.
with models.DAG(
    "example_create_and_deploy",
    start_date=START_DATE,
    catchup=False,
    user_defined_macros={
        "get_target_column_spec": get_target_column_spec,
        "target": TARGET,
        "extract_object_id": extract_object_id,
    },
    tags=["example"],
) as create_deploy_dag:
    # [START howto_operator_automl_create_dataset]
    create_dataset_task = AutoMLCreateDatasetOperator(
        task_id="create_dataset_task",
        dataset=DATASET,
        location=GCP_AUTOML_LOCATION,
        project_id=GCP_PROJECT_ID,
    )

    dataset_id = cast(str, XComArg(create_dataset_task, key="dataset_id"))
    # [END howto_operator_automl_create_dataset]

    MODEL["dataset_id"] = dataset_id

    # [START howto_operator_automl_import_data]
    import_dataset_task = AutoMLImportDataOperator(
        task_id="import_dataset_task",
        dataset_id=dataset_id,
        location=GCP_AUTOML_LOCATION,
        input_config=IMPORT_INPUT_CONFIG,
    )
    # [END howto_operator_automl_import_data]

    # [START howto_operator_automl_specs]
    list_tables_spec_task = AutoMLTablesListTableSpecsOperator(
        task_id="list_tables_spec_task",
        dataset_id=dataset_id,
        location=GCP_AUTOML_LOCATION,
        project_id=GCP_PROJECT_ID,
    )
    # [END howto_operator_automl_specs]

    # [START howto_operator_automl_column_specs]
    list_columns_spec_task = AutoMLTablesListColumnSpecsOperator(
        task_id="list_columns_spec_task",
        dataset_id=dataset_id,
        table_spec_id="{{ extract_object_id(task_instance.xcom_pull('list_tables_spec_task')[0]) }}",
        location=GCP_AUTOML_LOCATION,
        project_id=GCP_PROJECT_ID,
    )
    # [END howto_operator_automl_column_specs]

    # [START howto_operator_automl_update_dataset]
    update = deepcopy(DATASET)
    update["name"] = '{{ task_instance.xcom_pull("create_dataset_task")["name"] }}'
    update["tables_dataset_metadata"][  # type: ignore
        "target_column_spec_id"
    ] = "{{ get_target_column_spec(task_instance.xcom_pull('list_columns_spec_task'), target) }}"

    update_dataset_task = AutoMLTablesUpdateDatasetOperator(
        task_id="update_dataset_task",
        dataset=update,
        location=GCP_AUTOML_LOCATION,
    )
    # [END howto_operator_automl_update_dataset]

    # [START howto_operator_automl_create_model]
    create_model_task = AutoMLTrainModelOperator(
        task_id="create_model_task",
        model=MODEL,
        location=GCP_AUTOML_LOCATION,
        project_id=GCP_PROJECT_ID,
    )

    model_id = cast(str, XComArg(create_model_task, key="model_id"))
    # [END howto_operator_automl_create_model]

    # [START howto_operator_automl_delete_model]
    delete_model_task = AutoMLDeleteModelOperator(
        task_id="delete_model_task",
        model_id=model_id,
        location=GCP_AUTOML_LOCATION,
        project_id=GCP_PROJECT_ID,
    )
    # [END howto_operator_automl_delete_model]

    delete_datasets_task = AutoMLDeleteDatasetOperator(
        task_id="delete_datasets_task",
        dataset_id=dataset_id,
        location=GCP_AUTOML_LOCATION,
        project_id=GCP_PROJECT_ID,
    )

    (
        import_dataset_task
        >> list_tables_spec_task
        >> list_columns_spec_task
        >> update_dataset_task
        >> create_model_task
    )
    delete_model_task >> delete_datasets_task

    # Task dependencies created via `XComArgs`:
    #   create_dataset_task >> import_dataset_task
    #   create_dataset_task >> list_tables_spec_task
    #   create_dataset_task >> list_columns_spec_task
    #   create_dataset_task >> create_model_task
    #   create_model_task >> delete_model_task
    #   create_dataset_task >> delete_datasets_task


# Example DAG for AutoML datasets operations
with models.DAG(
    "example_automl_dataset",
    start_date=START_DATE,
    catchup=False,
    user_defined_macros={"extract_object_id": extract_object_id},
) as example_dag:
    create_dataset_task2 = AutoMLCreateDatasetOperator(
        task_id="create_dataset_task",
        dataset=DATASET,
        location=GCP_AUTOML_LOCATION,
        project_id=GCP_PROJECT_ID,
    )

    dataset_id = cast(str, XComArg(create_dataset_task2, key="dataset_id"))

    import_dataset_task = AutoMLImportDataOperator(
        task_id="import_dataset_task",
        dataset_id=dataset_id,
        location=GCP_AUTOML_LOCATION,
        input_config=IMPORT_INPUT_CONFIG,
    )

    list_tables_spec_task = AutoMLTablesListTableSpecsOperator(
        task_id="list_tables_spec_task",
        dataset_id=dataset_id,
        location=GCP_AUTOML_LOCATION,
        project_id=GCP_PROJECT_ID,
    )

    list_columns_spec_task = AutoMLTablesListColumnSpecsOperator(
        task_id="list_columns_spec_task",
        dataset_id=dataset_id,
        table_spec_id="{{ extract_object_id(task_instance.xcom_pull('list_tables_spec_task')[0]) }}",
        location=GCP_AUTOML_LOCATION,
        project_id=GCP_PROJECT_ID,
    )

    # [START howto_operator_list_dataset]
    list_datasets_task = AutoMLListDatasetOperator(
        task_id="list_datasets_task",
        location=GCP_AUTOML_LOCATION,
        project_id=GCP_PROJECT_ID,
    )
    # [END howto_operator_list_dataset]

    # [START howto_operator_delete_dataset]
    delete_datasets_task = AutoMLDeleteDatasetOperator(
        task_id="delete_datasets_task",
        dataset_id="{{ task_instance.xcom_pull('list_datasets_task', key='dataset_id_list') | list }}",
        location=GCP_AUTOML_LOCATION,
        project_id=GCP_PROJECT_ID,
    )
    # [END howto_operator_delete_dataset]

    (
        import_dataset_task
        >> list_tables_spec_task
        >> list_columns_spec_task
        >> list_datasets_task
        >> delete_datasets_task
    )

    # Task dependencies created via `XComArgs`:
    # create_dataset_task >> import_dataset_task
    # create_dataset_task >> list_tables_spec_task
    # create_dataset_task >> list_columns_spec_task


with models.DAG(
    "example_gcp_get_deploy",
    start_date=START_DATE,
    catchup=False,
    tags=["example"],
) as get_deploy_dag:
    # [START howto_operator_get_model]
    get_model_task = AutoMLGetModelOperator(
        task_id="get_model_task",
        model_id=MODEL_ID,
        location=GCP_AUTOML_LOCATION,
        project_id=GCP_PROJECT_ID,
    )
    # [END howto_operator_get_model]

    # [START howto_operator_deploy_model]
    deploy_model_task = AutoMLDeployModelOperator(
        task_id="deploy_model_task",
        model_id=MODEL_ID,
        location=GCP_AUTOML_LOCATION,
        project_id=GCP_PROJECT_ID,
    )
    # [END howto_operator_deploy_model]


with models.DAG(
    "example_gcp_predict",
    start_date=START_DATE,
    catchup=False,
    tags=["example"],
) as predict_dag:
    # [START howto_operator_prediction]
    predict_task = AutoMLPredictOperator(
        task_id="predict_task",
        model_id=MODEL_ID,
        payload={},  # Add your own payload, the used model_id must be deployed
        location=GCP_AUTOML_LOCATION,
        project_id=GCP_PROJECT_ID,
    )
    # [END howto_operator_prediction]

    # [START howto_operator_batch_prediction]
    batch_predict_task = AutoMLBatchPredictOperator(
        task_id="batch_predict_task",
        model_id=MODEL_ID,
        input_config={},  # Add your config
        output_config={},  # Add your config
        location=GCP_AUTOML_LOCATION,
        project_id=GCP_PROJECT_ID,
    )
    # [END howto_operator_batch_prediction]
