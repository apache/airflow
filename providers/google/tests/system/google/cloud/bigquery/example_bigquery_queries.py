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
    BigQueryCheckOperator,
    BigQueryColumnCheckOperator,
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryGetDataOperator,
    BigQueryInsertJobOperator,
    BigQueryIntervalCheckOperator,
    BigQueryTableCheckOperator,
    BigQueryValueCheckOperator,
)
from airflow.providers.standard.operators.bash import BashOperator

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
QUERY_SQL_PATH = "resources/example_bigquery_query.sql"

TABLE_1 = "table1"
TABLE_2 = "table2"

SCHEMA = [
    {"name": "value", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "ds", "type": "DATE", "mode": "NULLABLE"},
]


DAG_ID = "bigquery_queries"
DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}"
INSERT_DATE = "2030-01-01"
# [START howto_operator_bigquery_query]
INSERT_ROWS_QUERY = (
    f"INSERT {DATASET_NAME}.{TABLE_1} VALUES "
    f"(42, 'monty python', '{INSERT_DATE}'), "
    f"(42, 'fishy fish', '{INSERT_DATE}');"
)
# [END howto_operator_bigquery_query]

with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "bigquery"],
    user_defined_macros={"DATASET": DATASET_NAME, "TABLE": TABLE_1, "QUERY_SQL_PATH": QUERY_SQL_PATH},
) as dag:
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id=DATASET_NAME,
    )

    create_table_1 = BigQueryCreateTableOperator(
        task_id="create_table_1",
        dataset_id=DATASET_NAME,
        table_id=TABLE_1,
        table_resource={
            "schema": {"fields": SCHEMA},
        },
    )

    create_table_2 = BigQueryCreateTableOperator(
        task_id="create_table_2",
        dataset_id=DATASET_NAME,
        table_id=TABLE_2,
        table_resource={
            "schema": {"fields": SCHEMA},
        },
    )

    # [START howto_operator_bigquery_insert_job]
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
    # [END howto_operator_bigquery_insert_job]

    # [START howto_operator_bigquery_select_job]
    select_query_job = BigQueryInsertJobOperator(
        task_id="select_query_job",
        configuration={
            "query": {
                "query": "{% include QUERY_SQL_PATH %}",
                "useLegacySql": False,
            }
        },
    )
    # [END howto_operator_bigquery_select_job]

    execute_insert_query = BigQueryInsertJobOperator(
        task_id="execute_insert_query",
        configuration={
            "query": {
                "query": INSERT_ROWS_QUERY,
                "useLegacySql": False,
            }
        },
    )

    execute_query_save = BigQueryInsertJobOperator(
        task_id="execute_query_save",
        configuration={
            "query": {
                "query": f"SELECT * FROM {DATASET_NAME}.{TABLE_1}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_NAME,
                    "tableId": TABLE_2,
                },
            }
        },
    )

    bigquery_execute_multi_query = BigQueryInsertJobOperator(
        task_id="execute_multi_query",
        configuration={
            "query": {
                "query": [
                    f"SELECT * FROM {DATASET_NAME}.{TABLE_2}",
                    f"SELECT COUNT(*) FROM {DATASET_NAME}.{TABLE_2}",
                ],
                "useLegacySql": False,
            }
        },
    )

    # [START howto_operator_bigquery_get_data]
    get_data = BigQueryGetDataOperator(
        task_id="get_data",
        dataset_id=DATASET_NAME,
        table_id=TABLE_1,
        max_results=10,
        selected_fields="value,name",
    )
    # [END howto_operator_bigquery_get_data]

    get_data_result = BashOperator(
        task_id="get_data_result",
        bash_command=f'echo "{get_data.output}"',
    )

    # [START howto_operator_bigquery_check]
    check_count = BigQueryCheckOperator(
        task_id="check_count",
        sql=f"SELECT COUNT(*) FROM {DATASET_NAME}.{TABLE_1}",
        use_legacy_sql=False,
    )
    # [END howto_operator_bigquery_check]

    # [START howto_operator_bigquery_value_check]
    check_value = BigQueryValueCheckOperator(
        task_id="check_value",
        sql=f"SELECT COUNT(*) FROM {DATASET_NAME}.{TABLE_1}",
        pass_value=4,
        use_legacy_sql=False,
    )
    # [END howto_operator_bigquery_value_check]

    # [START howto_operator_bigquery_interval_check]
    check_interval = BigQueryIntervalCheckOperator(
        task_id="check_interval",
        table=f"{DATASET_NAME}.{TABLE_1}",
        days_back=1,
        metrics_thresholds={"COUNT(*)": 1.5},
        use_legacy_sql=False,
    )
    # [END howto_operator_bigquery_interval_check]

    # [START howto_operator_bigquery_column_check]
    column_check = BigQueryColumnCheckOperator(
        task_id="column_check",
        table=f"{DATASET_NAME}.{TABLE_1}",
        column_mapping={"value": {"null_check": {"equal_to": 0}}},
    )
    # [END howto_operator_bigquery_column_check]

    # [START howto_operator_bigquery_table_check]
    table_check = BigQueryTableCheckOperator(
        task_id="table_check",
        table=f"{DATASET_NAME}.{TABLE_1}",
        checks={"row_count_check": {"check_statement": "COUNT(*) = 4"}},
    )
    # [END howto_operator_bigquery_table_check]

    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id=DATASET_NAME,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    check_openlineage_events = OpenLineageTestOperator(
        task_id="check_openlineage_events",
        file_path=str(Path(__file__).parent / "resources" / "openlineage" / "bigquery_queries.json"),
    )

    # TEST SETUP
    create_dataset >> [create_table_1, create_table_2]
    # TEST BODY
    [create_table_1, create_table_2] >> insert_query_job >> [select_query_job, execute_insert_query]
    execute_insert_query >> get_data >> get_data_result >> delete_dataset
    execute_insert_query >> execute_query_save >> bigquery_execute_multi_query >> delete_dataset
    execute_insert_query >> [check_count, check_value, check_interval] >> delete_dataset
    execute_insert_query >> [column_check, table_check] >> delete_dataset
    delete_dataset >> check_openlineage_events

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
