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
This is an example DAG for the use of the SQLExecuteQueryOperator with Druid.
"""

from __future__ import annotations

import datetime
from textwrap import dedent

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

DAG_ID = "example_druid"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2025, 1, 1),
    default_args={"conn_id": "my_druid_conn"},
    schedule="@once",
    catchup=False,
) as dag:
    # [START howto_operator_druid]

    # Task: List all published datasources in Druid.
    list_datasources_task = SQLExecuteQueryOperator(
        task_id="list_datasources",
        sql="SELECT DISTINCT datasource FROM sys.segments WHERE is_published = 1",
    )

    # Task: Describe the schema for the 'wikipedia' datasource.
    # Note: This query returns column information if the datasource exists.
    describe_wikipedia_task = SQLExecuteQueryOperator(
        task_id="describe_wikipedia",
        sql=dedent("""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'wikipedia'
        """).strip(),
    )

    # Task: Count rows for the 'wikipedia' datasource.
    # Here we count the segments for 'wikipedia'. If the datasource is not ingested, it returns 0.
    select_count_from_datasource = SQLExecuteQueryOperator(
        task_id="select_count_from_datasource",
        sql="SELECT COUNT(*) FROM sys.segments WHERE datasource = 'wikipedia'",
    )

    # [END howto_operator_druid]

    list_datasources_task >> describe_wikipedia_task >> select_count_from_datasource

    # Optional: watcher for system tests
    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
