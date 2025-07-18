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
This is an example DAG for the use of the SQLExecuteQueryOperator with Exasol.
"""

from __future__ import annotations

import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.exasol.hooks.exasol import exasol_fetch_all_handler

DAG_ID = "example_exasol"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2025, 1, 1),
    default_args={"conn_id": "my_exasol_conn", "handler": exasol_fetch_all_handler},
    schedule="@once",
    catchup=False,
) as dag:
    # [START howto_operator_exasol]
    create_table_exasol = SQLExecuteQueryOperator(
        task_id="create_table_exasol",
        sql="""
            CREATE OR REPLACE TABLE exasol_example (
                a VARCHAR(100),
                b DECIMAL(18,0)
            );
        """,
    )

    alter_table_exasol = SQLExecuteQueryOperator(
        task_id="alter_table_exasol",
        sql="ALTER TABLE exasol_example ADD COLUMN c DECIMAL(18,0);",
    )

    insert_data_exasol = SQLExecuteQueryOperator(
        task_id="insert_data_exasol",
        sql="""
            INSERT INTO exasol_example (a, b, c)
            VALUES
              ('a', 1, 1),
              ('a', 2, 1),
              ('b', 3, 1);
        """,
    )

    select_data_exasol = SQLExecuteQueryOperator(
        task_id="select_data_exasol",
        sql="SELECT * FROM exasol_example;",
    )

    drop_table_exasol = SQLExecuteQueryOperator(
        task_id="drop_table_exasol",
        sql="DROP TABLE exasol_example;",
    )
    # [END howto_operator_exasol]

    (
        create_table_exasol
        >> alter_table_exasol
        >> insert_data_exasol
        >> select_data_exasol
        >> drop_table_exasol
    )

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
