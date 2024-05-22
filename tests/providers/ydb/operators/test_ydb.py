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
from __future__ import annotations

import re
from datetime import datetime, timedelta
from airflow.utils import timezone
from unittest.mock import MagicMock, PropertyMock, call, patch

import pytest
import responses
from responses import matchers

from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.providers.ydb.operators.ydb import YDBOperator
from airflow.providers.common.sql.hooks.sql import fetch_all_handler

# @pytest.mark.db_test
def est_sql_templating(create_task_instance_of_operator):
    ti = create_task_instance_of_operator(
        YDBOperator,
        sql="SELECT * FROM pet WHERE birth_date BETWEEN '{{params.begin_date}}' AND '{{params.end_date}}'",
        params={"begin_date": "2020-01-01", "end_date": "2020-12-31"},
        ydb_conn_id="ydb_default1",
        dag_id="test_template_body_templating_dag",
        task_id="test_template_body_templating_task",
        execution_date=timezone.datetime(2024, 2, 1, tzinfo=timezone.utc),
    )
    ti.render_templates()
    task: YDBOperator = ti.task
    assert task.sql == "SELECT * FROM pet WHERE birth_date BETWEEN '2020-01-01' AND '2020-12-31'"

class X:
    def wait(*args, **kwargs):
        pass

class S1:
    def __init__(self, driver):
        self._driver = driver

class S2:
    def __init__(self, driver):
        self._pool_impl = S1(driver)

class TestYDBOperator:
    def setup_method(self):
        dag_id = "test_dag"
        self.dag = DAG(
            dag_id,
            default_args={
                "owner": "airflow",
                "start_date": datetime.today(),
                "end_date": datetime.today() + timedelta(days=1),
            },
            schedule="@once",
        )

    @patch("airflow.hooks.base.BaseHook.get_connection")
    @patch("ydb.Driver")
    @patch("ydb.SessionPool")
    @patch("airflow.providers.ydb.hooks.dbapi.cursor.Cursor.execute")
    @patch("airflow.providers.ydb.hooks.dbapi.cursor.Cursor.fetchall")
    @patch("airflow.providers.ydb.hooks.dbapi.cursor.Cursor.rowcount", new_callable=PropertyMock)
#    @patch("airflow.providers.ydb.hooks.dbapi.cursor.Cursor.description", new_callable=PropertyMock)
    def test_execute_query(self, mock_cursor_rowcount, mock_cursor_fetchall, mock_cursor_execute, mock_session_pool, mock_driver, mock_get_connection):
        mock_get_connection.return_value = Connection(conn_type="ydb", extra={"oauth": "OAUTH_TOKEN"})
        driver_instance = X()
        #driver_instance.wait.return_value = None

        mock_driver.return_value = driver_instance
        mock_session_pool.return_value = S2(driver_instance)
        mock_cursor_execute.return_value = True
        mock_cursor_rowcount.return_value = 1
        #mock_cursor_description.return_value = True
        mock_cursor_fetchall.return_value = "zzzz"
        operator = YDBOperator(task_id="simple_sql", sql="select 987", is_ddl=True, handler=fetch_all_handler)
        context = {"ti": MagicMock()}

        results = operator.execute(context)
        assert results == "zzzz"
