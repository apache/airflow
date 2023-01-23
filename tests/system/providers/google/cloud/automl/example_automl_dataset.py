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
Example Airflow DAG for Google AutoML service testing dataset operations.
"""
from __future__ import annotations

import os
from copy import deepcopy
from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.hooks.automl import CloudAutoMLHook
from airflow.providers.google.cloud.operators.automl import (
    AutoMLCreateDatasetOperator,
    AutoMLDeleteDatasetOperator,
    AutoMLImportDataOperator,
    AutoMLListDatasetOperator,
    AutoMLTablesListColumnSpecsOperator,
    AutoMLTablesListTableSpecsOperator,
    AutoMLTablesUpdateDatasetOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
    GCSSynchronizeBucketsOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "automl_dataset"
GCP_PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")

GCP_AUTOML_LOCATION = "us-central1"

DATA_SAMPLE_GCS_BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
RESOURCE_DATA_BUCKET = "system-tests-resources"

DATASET_NAME = f"ds_{DAG_ID}_{ENV_ID}"
DATASET = {
    "display_name": DATASET_NAME,
    "tables_dataset_metadata": {"target_column_spec_id": ""},
}
AUTOML_DATASET_BUCKET = f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}/automl/bank-marketing.csv"
IMPORT_INPUT_CONFIG = {"gcs_source": {"input_uris": [AUTOML_DATASET_BUCKET]}}

extract_object_id = CloudAutoMLHook.extract_object_id


def get_target_column_spec(columns_specs: list[dict], column_name: str) -> str:
    """
    Using column name returns spec of the column.
    """
    for column in columns_specs:
        if column["display_name"] == column_name:
            return extract_object_id(column)
    raise Exception(f"Unknown target column: {column_name}")


with models.DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "automl"],
    user_defined_macros={
        "get_target_column_spec": get_target_column_spec,
        "target": "Class",
        "extract_object_id": extract_object_id,
    },
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
        source_object="automl",
        destination_bucket=DATA_SAMPLE_GCS_BUCKET_NAME,
        destination_object="automl",
        recursive=True,
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
    update["name"] = '{{ task_instance.xcom_pull("create_dataset")["name"] }}'
    update["tables_dataset_metadata"][  # type: ignore
        "target_column_spec_id"
    ] = "{{ get_target_column_spec(task_instance.xcom_pull('list_columns_spec_task'), target) }}"

    update_dataset_task = AutoMLTablesUpdateDatasetOperator(
        task_id="update_dataset_task",
        dataset=update,
        location=GCP_AUTOML_LOCATION,
    )
    # [END howto_operator_automl_update_dataset]

    # [START howto_operator_list_dataset]
    list_datasets_task = AutoMLListDatasetOperator(
        task_id="list_datasets_task",
        location=GCP_AUTOML_LOCATION,
        project_id=GCP_PROJECT_ID,
    )
    # [END howto_operator_list_dataset]

    # [START howto_operator_delete_dataset]
    delete_dataset = AutoMLDeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id="{{ task_instance.xcom_pull('list_datasets_task', key='dataset_id_list') | list }}",
        location=GCP_AUTOML_LOCATION,
        project_id=GCP_PROJECT_ID,
    )
    # [END howto_operator_delete_dataset]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        # TEST SETUP
        [create_bucket >> move_dataset_file, create_dataset]
        # TEST BODY
        >> import_dataset_task
        >> list_tables_spec_task
        >> list_columns_spec_task
        >> update_dataset_task
        >> list_datasets_task
        # TEST TEARDOWN
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
