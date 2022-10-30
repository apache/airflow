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
Example use of MsSql related operators.
"""
from __future__ import annotations

# [START mssql_operator_howto_guide]
import os
from datetime import datetime

from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_mssql"


with DAG(
    DAG_ID,
    schedule="@daily",
    start_date=datetime(2021, 10, 1),
    tags=["example"],
    catchup=False,
) as dag:

    # [START howto_operator_mssql]

    # Example of creating a task to create a table in MsSql

    create_table_mssql_task = MsSqlOperator(
        task_id="create_country_table",
        mssql_conn_id="airflow_mssql",
        sql=r"""
        CREATE TABLE Country (
            country_id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
            name TEXT,
            continent TEXT
        );
        """,
        dag=dag,
    )

    # [END howto_operator_mssql]

    # [START mssql_hook_howto_guide_insert_mssql_hook]

    @dag.task(task_id="insert_mssql_task")
    def insert_mssql_hook():
        mssql_hook = MsSqlHook(mssql_conn_id="airflow_mssql", schema="airflow")

        rows = [
            ("India", "Asia"),
            ("Germany", "Europe"),
            ("Argentina", "South America"),
            ("Ghana", "Africa"),
            ("Japan", "Asia"),
            ("Namibia", "Africa"),
        ]
        target_fields = ["name", "continent"]
        mssql_hook.insert_rows(table="Country", rows=rows, target_fields=target_fields)

    # [END mssql_hook_howto_guide_insert_mssql_hook]

    # [START mssql_operator_howto_guide_create_table_mssql_from_external_file]
    # Example of creating a task that calls an sql command from an external file.
    create_table_mssql_from_external_file = MsSqlOperator(
        task_id="create_table_from_external_file",
        mssql_conn_id="airflow_mssql",
        sql="create_table.sql",
        dag=dag,
    )
    # [END mssql_operator_howto_guide_create_table_mssql_from_external_file]

    # [START mssql_operator_howto_guide_populate_user_table]
    populate_user_table = MsSqlOperator(
        task_id="populate_user_table",
        mssql_conn_id="airflow_mssql",
        sql=r"""
                INSERT INTO Users (username, description)
                VALUES ( 'Danny', 'Musician');
                INSERT INTO Users (username, description)
                VALUES ( 'Simone', 'Chef');
                INSERT INTO Users (username, description)
                VALUES ( 'Lily', 'Florist');
                INSERT INTO Users (username, description)
                VALUES ( 'Tim', 'Pet shop owner');
                """,
    )
    # [END mssql_operator_howto_guide_populate_user_table]

    # [START mssql_operator_howto_guide_get_all_countries]
    get_all_countries = MsSqlOperator(
        task_id="get_all_countries",
        mssql_conn_id="airflow_mssql",
        sql=r"""SELECT * FROM Country;""",
    )
    # [END mssql_operator_howto_guide_get_all_countries]

    # [START mssql_operator_howto_guide_get_all_description]
    get_all_description = MsSqlOperator(
        task_id="get_all_description",
        mssql_conn_id="airflow_mssql",
        sql=r"""SELECT description FROM Users;""",
    )
    # [END mssql_operator_howto_guide_get_all_description]

    # [START mssql_operator_howto_guide_params_passing_get_query]
    get_countries_from_continent = MsSqlOperator(
        task_id="get_countries_from_continent",
        mssql_conn_id="airflow_mssql",
        sql=r"""SELECT * FROM Country where {{ params.column }}='{{ params.value }}';""",
        params={"column": "CONVERT(VARCHAR, continent)", "value": "Asia"},
    )
    # [END mssql_operator_howto_guide_params_passing_get_query]
    (
        create_table_mssql_task
        >> insert_mssql_hook()
        >> create_table_mssql_from_external_file
        >> populate_user_table
        >> get_all_countries
        >> get_all_description
        >> get_countries_from_continent
    )
    # [END mssql_operator_howto_guide]

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()
from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
