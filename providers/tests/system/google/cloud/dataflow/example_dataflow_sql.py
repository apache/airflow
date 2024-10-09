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
Example Airflow DAG for Google Cloud Dataflow service
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryDeleteTableOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.dataflow import DataflowStartSqlJobOperator
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "dataflow_sql"
LOCATION = "europe-west3"
DATAFLOW_SQL_JOB_NAME = f"{DAG_ID}_{ENV_ID}".replace("_", "-")
BQ_SQL_DATASET = f"{DAG_ID}_{ENV_ID}".replace("-", "_")
BQ_SQL_TABLE_INPUT = f"input_{ENV_ID}".replace("-", "_")
BQ_SQL_TABLE_OUTPUT = f"output_{ENV_ID}".replace("-", "_")
INSERT_ROWS_QUERY = (
    f"INSERT {BQ_SQL_DATASET}.{BQ_SQL_TABLE_INPUT} VALUES "
    "('John Doe', 900), "
    "('Alice Storm', 1200),"
    "('Bob Max', 1000),"
    "('Peter Jackson', 800),"
    "('Mia Smith', 1100);"
)


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["example", "dataflow-sql"],
) as dag:
    create_bq_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bq_dataset",
        dataset_id=BQ_SQL_DATASET,
        location=LOCATION,
    )

    create_bq_table = BigQueryCreateEmptyTableOperator(
        task_id="create_bq_table",
        dataset_id=BQ_SQL_DATASET,
        table_id=BQ_SQL_TABLE_INPUT,
        schema_fields=[
            {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
        ],
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
        location=LOCATION,
    )

    # [START howto_operator_start_sql_job]
    start_sql = DataflowStartSqlJobOperator(
        task_id="start_sql_query",
        job_name=DATAFLOW_SQL_JOB_NAME,
        query=f"""
            SELECT
                emp_name as employee,
                salary as employee_salary
            FROM
                bigquery.table.`{PROJECT_ID}`.`{BQ_SQL_DATASET}`.`{BQ_SQL_TABLE_INPUT}`
            WHERE salary >= 1000;
        """,
        options={
            "bigquery-project": PROJECT_ID,
            "bigquery-dataset": BQ_SQL_DATASET,
            "bigquery-table": BQ_SQL_TABLE_OUTPUT,
        },
        location=LOCATION,
        do_xcom_push=True,
    )
    # [END howto_operator_start_sql_job]

    delete_bq_table = BigQueryDeleteTableOperator(
        task_id="delete_bq_table",
        deletion_dataset_table=f"{PROJECT_ID}.{BQ_SQL_DATASET}.{BQ_SQL_TABLE_INPUT}",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_bq_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_bq_dataset",
        dataset_id=BQ_SQL_DATASET,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        # TEST SETUP
        create_bq_dataset
        >> create_bq_table
        >> insert_query_job
        # TEST BODY
        >> start_sql
        # TEST TEARDOWN
        >> delete_bq_table
        >> delete_bq_dataset
    )

    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
