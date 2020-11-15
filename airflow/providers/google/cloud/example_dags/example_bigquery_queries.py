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
import os
from datetime import datetime

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryExecuteQueryOperator,
    BigQueryGetDataOperator,
    BigQueryInsertJobOperator,
    BigQueryIntervalCheckOperator,
    BigQueryValueCheckOperator,
)
from airflow.utils.dates import days_ago

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")
DATASET_NAME = os.environ.get("GCP_BIGQUERY_DATASET_NAME", "test_dataset")
LOCATION = "southamerica-east1"

TABLE_1 = "table1"
TABLE_2 = "table2"


INSERT_DATE = datetime.now().strftime("%Y-%m-%d")

# [START howto_operator_bigquery_query]
INSERT_ROWS_QUERY = (
    f"INSERT {DATASET_NAME}.{TABLE_1} VALUES "
    f"(42, 'monthy python', '{INSERT_DATE}'), "
    f"(42, 'fishy fish', '{INSERT_DATE}');"
)
# [END howto_operator_bigquery_query]

SCHEMA = [
    {"name": "value", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "ds", "type": "DATE", "mode": "NULLABLE"},
]

for location in [None, LOCATION]:
    dag_id = "example_bigquery_queries_location" if location else "example_bigquery_queries"

    with models.DAG(
        dag_id,
        schedule_interval=None,  # Override to match your needs
        start_date=days_ago(1),
        tags=["example"],
        user_defined_macros={"DATASET": DATASET_NAME, "TABLE": TABLE_1},
    ) as dag_with_locations:
        create_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id="create-dataset",
            dataset_id=DATASET_NAME,
            location=location,
        )

        create_table_1 = BigQueryCreateEmptyTableOperator(
            task_id="create_table_1",
            dataset_id=DATASET_NAME,
            table_id=TABLE_1,
            schema_fields=SCHEMA,
            location=location,
        )

        create_table_2 = BigQueryCreateEmptyTableOperator(
            task_id="create_table_2",
            dataset_id=DATASET_NAME,
            table_id=TABLE_2,
            schema_fields=SCHEMA,
            location=location,
        )

        create_dataset >> [create_table_1, create_table_2]

        delete_dataset = BigQueryDeleteDatasetOperator(
            task_id="delete_dataset", dataset_id=DATASET_NAME, delete_contents=True
        )

        # [START howto_operator_bigquery_insert_job]
        insert_query_job = BigQueryInsertJobOperator(
            task_id="insert_query_job",
            configuration={
                "query": {
                    "query": INSERT_ROWS_QUERY,
                    "useLegacySql": "False",
                }
            },
            location=location,
        )
        # [END howto_operator_bigquery_insert_job]

        # [START howto_operator_bigquery_select_job]
        select_query_job = BigQueryInsertJobOperator(
            task_id="select_query_job",
            configuration={
                "query": {
                    "query": "{% include 'example_bigquery_query.sql' %}",
                    "useLegacySql": False,
                }
            },
            location=location,
        )
        # [END howto_operator_bigquery_select_job]

        execute_insert_query = BigQueryExecuteQueryOperator(
            task_id="execute_insert_query", sql=INSERT_ROWS_QUERY, use_legacy_sql=False, location=location
        )

        bigquery_execute_multi_query = BigQueryExecuteQueryOperator(
            task_id="execute_multi_query",
            sql=[
                f"SELECT * FROM {DATASET_NAME}.{TABLE_2}",
                f"SELECT COUNT(*) FROM {DATASET_NAME}.{TABLE_2}",
            ],
            use_legacy_sql=False,
            location=location,
        )

        execute_query_save = BigQueryExecuteQueryOperator(
            task_id="execute_query_save",
            sql=f"SELECT * FROM {DATASET_NAME}.{TABLE_1}",
            use_legacy_sql=False,
            destination_dataset_table=f"{DATASET_NAME}.{TABLE_2}",
            location=location,
        )

        # [START howto_operator_bigquery_get_data]
        get_data = BigQueryGetDataOperator(
            task_id="get_data",
            dataset_id=DATASET_NAME,
            table_id=TABLE_1,
            max_results=10,
            selected_fields="value,name",
            location=location,
        )
        # [END howto_operator_bigquery_get_data]

        get_data_result = BashOperator(
            task_id="get_data_result",
            bash_command="echo \"{{ task_instance.xcom_pull('get_data') }}\"",
        )

        # [START howto_operator_bigquery_check]
        check_count = BigQueryCheckOperator(
            task_id="check_count",
            sql=f"SELECT COUNT(*) FROM {DATASET_NAME}.{TABLE_1}",
            use_legacy_sql=False,
            location=location,
        )
        # [END howto_operator_bigquery_check]

        # [START howto_operator_bigquery_value_check]
        check_value = BigQueryValueCheckOperator(
            task_id="check_value",
            sql=f"SELECT COUNT(*) FROM {DATASET_NAME}.{TABLE_1}",
            pass_value=4,
            use_legacy_sql=False,
            location=location,
        )
        # [END howto_operator_bigquery_value_check]

        # [START howto_operator_bigquery_interval_check]
        check_interval = BigQueryIntervalCheckOperator(
            task_id="check_interval",
            table=f"{DATASET_NAME}.{TABLE_1}",
            days_back=1,
            metrics_thresholds={"COUNT(*)": 1.5},
            use_legacy_sql=False,
            location=location,
        )
        # [END howto_operator_bigquery_interval_check]

        [create_table_1, create_table_2] >> insert_query_job >> select_query_job

        insert_query_job >> execute_insert_query
        execute_insert_query >> get_data >> get_data_result >> delete_dataset
        execute_insert_query >> execute_query_save >> bigquery_execute_multi_query >> delete_dataset
        execute_insert_query >> [check_count, check_value, check_interval] >> delete_dataset
