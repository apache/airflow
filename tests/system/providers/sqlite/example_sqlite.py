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
This is an example DAG for the use of the SqliteOperator.
In this example, we create two tasks that execute in sequence.
The first task calls an sql command, defined in the SQLite operator,
which when triggered, is performed on the connected sqlite database.
The second task is similar but instead calls the SQL command from an external file.
"""

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_sqlite"

with DAG(
    dag_id=DAG_ID,
    schedule='@daily',
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:

    # [START howto_operator_sqlite]

    # Example of creating a task that calls a common CREATE TABLE sql command.
    create_table_sqlite_task = SqliteOperator(
        task_id='create_table_sqlite',
        sql=r"""
        CREATE TABLE Customers (
            customer_id INT PRIMARY KEY,
            first_name TEXT,
            last_name TEXT
        );
        """,
    )

    # [END howto_operator_sqlite]

    @dag.task(task_id="insert_sqlite_task")
    def insert_sqlite_hook():
        sqlite_hook = SqliteHook()

        rows = [('James', '11'), ('James', '22'), ('James', '33')]
        target_fields = ['first_name', 'last_name']
        sqlite_hook.insert_rows(table='Customers', rows=rows, target_fields=target_fields)

    @dag.task(task_id="replace_sqlite_task")
    def replace_sqlite_hook():
        sqlite_hook = SqliteHook()

        rows = [('James', '11'), ('James', '22'), ('James', '33')]
        target_fields = ['first_name', 'last_name']
        sqlite_hook.insert_rows(table='Customers', rows=rows, target_fields=target_fields, replace=True)

    # [START howto_operator_sqlite_external_file]

    # Example of creating a task that calls an sql command from an external file.
    external_create_table_sqlite_task = SqliteOperator(
        task_id='create_table_sqlite_external_file',
        sql='create_table.sql',
    )

    # [END howto_operator_sqlite_external_file]

    (
        create_table_sqlite_task
        >> external_create_table_sqlite_task
        >> insert_sqlite_hook()
        >> replace_sqlite_hook()
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
