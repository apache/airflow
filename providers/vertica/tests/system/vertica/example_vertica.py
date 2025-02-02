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
This is an example DAG for the use of the SQLExecuteQueryOperator with Vertica.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_vertica"

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2025, 1, 1),
    default_args={"conn_id": "vertica_conn_id"},
    tags=["example"],
    catchup=False,
) as dag:
    # [START howto_operator_vertica]

    create_table_vertica_task = SQLExecuteQueryOperator(
        task_id="create_table_vertica",
        sql=[
            "DROP TABLE IF EXISTS employees;",
            """
            CREATE TABLE employees (
                id IDENTITY,
                name VARCHAR(50),
                salary NUMERIC(10,2),
                hire_date TIMESTAMP DEFAULT NOW()
            )
            """,
        ],
    )

    # [END howto_operator_vertica]

    insert_data_vertica_task = SQLExecuteQueryOperator(
        task_id="insert_data_vertica",
        sql="""
            INSERT INTO employees (name, salary) VALUES ('Alice', 50000);
            INSERT INTO employees (name, salary) VALUES ('Bob', 60000);
        """,
    )

    select_data_vertica_task = SQLExecuteQueryOperator(
        task_id="select_data_vertica",
        sql="SELECT * FROM employees",
    )

    # [START howto_operator_vertica_external_file]

    drop_table_vertica_task = SQLExecuteQueryOperator(
        task_id="drop_table_vertica",
        sql="vertica_drop_table.sql",
    )

    # [END howto_operator_vertica_external_file]

    (
        create_table_vertica_task
        >> insert_data_vertica_task
        >> select_data_vertica_task
        >> drop_table_vertica_task
    )

    from tests_common.test_utils.watcher import watcher

    # Ensure test watchers are triggered for success/failure
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
