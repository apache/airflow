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
Example Airflow DAG for Google BigQuery service testing tables.
"""

from __future__ import annotations

import os
import time
from datetime import datetime
from pathlib import Path

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryDeleteTableOperator,
    BigQueryGetDatasetTablesOperator,
    BigQueryUpdateDatasetOperator,
    BigQueryUpdateTableOperator,
    BigQueryUpdateTableSchemaOperator,
    BigQueryUpsertTableOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from airflow.providers.google.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
from airflow.providers.openlineage.tests.system.openlineage.operator import OpenLineageTestOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "bigquery_tables"

BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}"
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
SCHEMA_JSON_LOCAL_SRC = str(Path(__file__).parent / "resources" / "update_table_schema.json")
SCHEMA_JSON_DESTINATION = "update_table_schema.json"
GCS_PATH_TO_SCHEMA_JSON = f"gs://{BUCKET_NAME}/{SCHEMA_JSON_DESTINATION}"


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "bigquery"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )

    upload_schema_json = LocalFilesystemToGCSOperator(
        task_id="upload_schema_json",
        src=SCHEMA_JSON_LOCAL_SRC,
        dst=SCHEMA_JSON_DESTINATION,
        bucket=BUCKET_NAME,
    )

    create_dataset = BigQueryCreateEmptyDatasetOperator(task_id="create_dataset", dataset_id=DATASET_NAME)

    # [START howto_operator_bigquery_create_table]
    create_table = BigQueryCreateTableOperator(
        task_id="create_table",
        dataset_id=DATASET_NAME,
        table_id="test_table",
        table_resource={
            "schema": {
                "fields": [
                    {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
                ],
            },
        },
    )
    # [END howto_operator_bigquery_create_table]

    # [START howto_operator_bigquery_create_view]
    create_view = BigQueryCreateTableOperator(
        task_id="create_view",
        dataset_id=DATASET_NAME,
        table_id="test_view",
        table_resource={
            "view": {
                "query": f"SELECT * FROM `{PROJECT_ID}.{DATASET_NAME}.test_table`",
                "useLegacySql": False,
            },
        },
    )
    # [END howto_operator_bigquery_create_view]

    # [START howto_operator_bigquery_create_materialized_view]
    create_materialized_view = BigQueryCreateTableOperator(
        task_id="create_materialized_view",
        dataset_id=DATASET_NAME,
        table_id="test_materialized_view",
        table_resource={
            "materializedView": {
                "query": f"SELECT SUM(salary) AS sum_salary FROM `{PROJECT_ID}.{DATASET_NAME}.test_table`",
                "enableRefresh": True,
                "refreshIntervalMs": 600000,
            },
        },
    )
    # [END howto_operator_bigquery_create_materialized_view]

    # [START howto_operator_bigquery_delete_view]
    delete_view = BigQueryDeleteTableOperator(
        task_id="delete_view",
        deletion_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.test_view",
    )
    # [END howto_operator_bigquery_delete_view]

    # [START howto_operator_bigquery_update_table]
    update_table = BigQueryUpdateTableOperator(
        task_id="update_table",
        dataset_id=DATASET_NAME,
        table_id="test_table",
        fields=["friendlyName", "description"],
        table_resource={
            "friendlyName": "Updated Table",
            "description": "Updated Table",
        },
    )
    # [END howto_operator_bigquery_update_table]

    # [START howto_operator_bigquery_update_view]
    update_view = BigQueryUpdateTableOperator(
        task_id="update_view",
        dataset_id=DATASET_NAME,
        table_id="test_view",
        fields=["friendlyName", "description"],
        table_resource={
            "friendlyName": "Updated View friendlyName",
            "description": "Updated View description",
        },
    )
    # [END howto_operator_bigquery_update_view]

    # [START howto_operator_bigquery_update_materialized_view]
    update_materialized_view = BigQueryUpdateTableOperator(
        task_id="update_materialized_view",
        dataset_id=DATASET_NAME,
        table_id="test_materialized_view",
        fields=["friendlyName", "description"],
        table_resource={
            "friendlyName": "Updated View friendlyName",
            "description": "Updated View description",
        },
    )
    # [END howto_operator_bigquery_update_materialized_view]

    # [START howto_operator_bigquery_upsert_table]
    upsert_table = BigQueryUpsertTableOperator(
        task_id="upsert_table",
        dataset_id=DATASET_NAME,
        table_resource={
            "tableReference": {"tableId": "test_table_id"},
            "expirationTime": (int(time.time()) + 300) * 1000,
        },
    )
    # [END howto_operator_bigquery_upsert_table]

    # [START howto_operator_bigquery_update_table_schema]
    update_table_schema = BigQueryUpdateTableSchemaOperator(
        task_id="update_table_schema",
        dataset_id=DATASET_NAME,
        table_id="test_table",
        schema_fields_updates=[
            {"name": "emp_name", "description": "Name of employee"},
            {"name": "salary", "description": "Monthly salary in USD"},
        ],
    )
    # [END howto_operator_bigquery_update_table_schema]

    # [START howto_operator_bigquery_create_table_schema_json]
    create_table_schema_json = BigQueryCreateTableOperator(
        task_id="create_table_schema_json",
        dataset_id=DATASET_NAME,
        table_id="test_table",
        gcs_schema_object=GCS_PATH_TO_SCHEMA_JSON,
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": DATASET_NAME,
                "tableId": "test_table",
            },
        },
    )
    # [END howto_operator_bigquery_create_table_schema_json]

    # [START howto_operator_bigquery_delete_materialized_view]
    delete_materialized_view = BigQueryDeleteTableOperator(
        task_id="delete_materialized_view",
        deletion_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.test_materialized_view",
    )
    # [END howto_operator_bigquery_delete_materialized_view]

    # [START howto_operator_bigquery_get_dataset_tables]
    get_dataset_tables = BigQueryGetDatasetTablesOperator(
        task_id="get_dataset_tables", dataset_id=DATASET_NAME
    )
    # [END howto_operator_bigquery_get_dataset_tables]

    update_dataset = BigQueryUpdateDatasetOperator(
        task_id="update_dataset",
        dataset_id=DATASET_NAME,
        dataset_resource={"description": "Updated dataset"},
    )

    # [START howto_operator_bigquery_delete_table]
    delete_table = BigQueryDeleteTableOperator(
        task_id="delete_table",
        deletion_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.test_table",
    )
    # [END howto_operator_bigquery_delete_table]

    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset", dataset_id=DATASET_NAME, delete_contents=True
    )
    delete_dataset.trigger_rule = TriggerRule.ALL_DONE

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    check_openlineage_events = OpenLineageTestOperator(
        task_id="check_openlineage_events",
        file_path=str(Path(__file__).parent / "resources" / "openlineage" / "bigquery_tables.json"),
    )

    (
        # TEST SETUP
        create_bucket
        >> create_dataset
        >> upload_schema_json
        # TEST BODY
        >> update_dataset
        >> create_table
        >> create_view
        >> create_materialized_view
        >> update_view
        >> [
            get_dataset_tables,
            delete_view,
        ]
        >> update_table
        >> update_materialized_view
        >> upsert_table
        >> update_table_schema
        >> create_table_schema_json
        >> delete_materialized_view
        >> delete_table
        # TEST TEARDOWN
        >> delete_bucket
        >> delete_dataset
        >> check_openlineage_events
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
