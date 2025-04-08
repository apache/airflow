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
Example DAG demonstrating the usage of the SQLExecuteQueryOperator with Postgres.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_odbc_operator"

with DAG(
    dag_id=DAG_ID,
    schedule="0 0 * * *",
    start_date=datetime(2025, 1, 1),
    dagrun_timeout=timedelta(minutes=60),
    tags=["example", "odbc"],
    catchup=False,
) as dag:
    # [START howto_operator_odbc]

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        sql="""
        CREATE TABLE IF NOT EXISTS my_table (
            dt VARCHAR(50),
            value VARCHAR(255)
        );
        """,
        conn_id="my_odbc_conn",
        autocommit=True,
    )

    # [END howto_operator_odbc]

    # [START howto_operator_odbc_template]

    insert_data = SQLExecuteQueryOperator(
        task_id="insert_data",
        sql="""
        INSERT INTO my_table (dt, value)
        VALUES ('{{ ds }}', 'test_value');
        """,
        conn_id="my_odbc_conn",
        autocommit=True,
    )

    # [END howto_operator_odbc_template]

    delete_data = SQLExecuteQueryOperator(
        task_id="delete_data",
        sql="""
        DELETE FROM my_table
        WHERE dt = '{{ ds }}';
        """,
        conn_id="my_odbc_conn",
        autocommit=True,
    )

    drop_table = SQLExecuteQueryOperator(
        task_id="drop_table",
        sql="DROP TABLE IF EXISTS my_table;",
        conn_id="my_odbc_conn",
        autocommit=True,
    )

    create_table >> insert_data >> delete_data >> drop_table

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
