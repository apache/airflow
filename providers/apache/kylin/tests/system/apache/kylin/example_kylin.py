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
Example DAG for Apache Kylin using SQLExecuteQueryOperator with monkey patching.

This DAG demonstrates using SQLExecuteQueryOperator to run SQL queries
against Apache Kylin. A monkey patch is applied at the top to patch the
KylinHook to be compatible with SQLExecuteQueryOperator.
"""

# ===================== Monkey Patch Start =====================
from __future__ import annotations

import datetime

import airflow.providers.apache.kylin.hooks.kylin as kylin_hooks
from airflow import DAG
from airflow.providers.apache.kylin.hooks.kylin import KylinHook as OriginalKylinHook
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


class PatchedKylinHook(DbApiHook, OriginalKylinHook):
    """
    Patched version of KylinHook that inherits from DbApiHook.
    This allows SQLExecuteQueryOperator to use it.
    """

    conn_name_attr = "kylin_conn_id"
    default_conn_name = "my_kylin_conn"
    supports_autocommit = True

    def get_conn(self):
        """
        Return connection details.
        In a production scenario, you may want to establish a proper connection
        object to interact with Kylin's REST API.
        """
        conn = self.get_connection(self.kylin_conn_id)
        host = conn.host
        port = conn.port or 7070
        return (host, port, conn.login, conn.password)

    def run(self, sql, autocommit=True, parameters=None, **kwargs):
        """
        Executes the given SQL query against Apache Kylin via its REST API.
        The `autocommit` parameter and any additional keyword arguments are accepted.
        It also includes the project name (from connection schema) in the request payload.
        """
        conn = self.get_connection(self.kylin_conn_id)
        host = conn.host
        port = conn.port or 7070
        project = conn.schema  # 使用連線中的 schema 作為 project name
        if not project:
            raise ValueError("Project name must be provided in the connection's schema field.")
        url = f"http://{host}:{port}/kylin/api/query"
        import requests

        payload = {"sql": sql, "project": project}
        response = requests.post(url, json=payload, auth=(conn.login, conn.password))
        response.raise_for_status()
        return response.json()


# Apply the monkey patch by replacing the original KylinHook with our patched version.
kylin_hooks.KylinHook = PatchedKylinHook

# ===================== Monkey Patch End =====================

DAG_ID = "example_kylin"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2025, 1, 1),
    default_args={"conn_id": "my_kylin_conn"},
    schedule="@once",
    catchup=False,
) as dag:
    # [START howto_operator_kylin]
    create_table_kylin_task = SQLExecuteQueryOperator(
        task_id="create_table_kylin",
        sql="""
            CREATE TABLE IF NOT EXISTS kylin_example (
                a VARCHAR(100),
                b INT
            )
        """,
    )

    alter_table_kylin_task = SQLExecuteQueryOperator(
        task_id="alter_table_kylin",
        sql="ALTER TABLE kylin_example ADD COLUMN c INT",
    )

    insert_data_kylin_task = SQLExecuteQueryOperator(
        task_id="insert_data_kylin",
        sql="""
            INSERT INTO kylin_example (a, b, c)
            VALUES ('x', 10, 1), ('y', 20, 2), ('z', 30, 3)
        """,
    )

    select_data_kylin_task = SQLExecuteQueryOperator(
        task_id="select_data_kylin",
        sql="SELECT * FROM kylin_example",
    )

    drop_table_kylin_task = SQLExecuteQueryOperator(
        task_id="drop_table_kylin",
        sql="DROP TABLE kylin_example",
    )
    # [END howto_operator_kylin]

    (
        create_table_kylin_task
        >> alter_table_kylin_task
        >> insert_data_kylin_task
        >> select_data_kylin_task
        >> drop_table_kylin_task
    )

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
