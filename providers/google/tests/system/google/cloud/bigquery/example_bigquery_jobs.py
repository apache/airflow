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
Example Airflow DAG for Google BigQuery service.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
from system.openlineage.operator import OpenLineageTestOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
LOCATION = "us-east1"

TABLE_1 = "table1"
TABLE_2 = "table2"
TABLE_3 = "table3"
TABLE_4 = "table4"

SCHEMA = [
    {"name": "value", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "ds", "type": "DATE", "mode": "NULLABLE"},
]

DAG_ID = "bigquery_jobs"
DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
DATASET = f"{DATASET_NAME}1"
INSERT_DATE = "2030-01-01"
INSERT_ROWS_QUERY = (
    f"INSERT {DATASET}.{TABLE_1} VALUES "
    f"(42, 'monty python', '{INSERT_DATE}'), "
    f"(42, 'fishy fish', '{INSERT_DATE}');"
)

CTAS_QUERY = (
    f"CREATE OR REPLACE TABLE {DATASET}.{TABLE_3} AS SELECT value, name, ds FROM {DATASET}.{TABLE_1};"
)

with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "bigquery"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID, storage_class="STANDARD"
    )

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id=DATASET,
    )

    create_table = BigQueryCreateTableOperator(
        task_id="create_table",
        dataset_id=DATASET,
        table_id=TABLE_1,
        table_resource={
            "schema": {"fields": SCHEMA},
        },
    )

    insert_query_job = BigQueryInsertJobOperator(
        task_id="insert_query_job",
        configuration={
            "query": {
                "query": INSERT_ROWS_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    execute_copy_job = BigQueryInsertJobOperator(
        task_id="execute_copy_job",
        configuration={
            "copy": {
                "sourceTables": [{"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE_1}],
                "destinationTable": {"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE_2},
                "writeDisposition": "WRITE_TRUNCATE",
                "operationType": "COPY",
            }
        },
    )

    execute_ctas_query = BigQueryInsertJobOperator(
        task_id="execute_ctas_query",
        configuration={
            "query": {
                "query": CTAS_QUERY,
                "useLegacySql": False,
            }
        },
    )

    execute_load_job = BigQueryInsertJobOperator(
        task_id="execute_load_job",
        configuration={
            "load": {
                "sourceUris": ["gs://cloud-samples-data/bigquery/us-states/us-states.csv"],
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET,
                    "tableId": TABLE_4,
                },
                "schema": {
                    "fields": [
                        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                        {"name": "post_abbr", "type": "STRING", "mode": "NULLABLE"},
                    ]
                },
                "write_disposition": "WRITE_TRUNCATE",
                "sourceFormat": "CSV",
            }
        },
    )

    execute_extract_job = BigQueryInsertJobOperator(
        task_id="execute_extract_job",
        configuration={
            "extract": {
                "sourceTable": {"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE_4},
                "destinationUris": [
                    f"gs://{BUCKET_NAME}/extract.csv",
                ],
                "destinationFormat": "CSV",
            }
        },
    )

    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id=DATASET,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    # if not location:
    check_openlineage_events = OpenLineageTestOperator(
        task_id="check_openlineage_events",
        file_path=str(Path(__file__).parent / "resources" / "openlineage" / "bigquery_jobs.json"),
    )
    delete_dataset >> check_openlineage_events

    # TEST SETUP
    create_dataset >> create_table
    # TEST BODY
    create_table >> insert_query_job >> execute_copy_job >> execute_ctas_query >> delete_dataset
    create_dataset >> execute_load_job >> execute_extract_job >> delete_dataset
    # TEST TEARDOWN
    delete_dataset >> delete_bucket

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
