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

from datetime import datetime, timedelta
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
import ydb

from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.providers.common.sql.hooks.sql import fetch_all_handler, fetch_one_handler
from airflow.providers.ydb.operators.ydb import YDBExecuteQueryOperator
from airflow.utils import timezone


@pytest.mark.db_test
def test_sql_templating(create_task_instance_of_operator):
    ti = create_task_instance_of_operator(
        YDBExecuteQueryOperator,
        sql="SELECT * FROM pet WHERE birth_date BETWEEN '{{params.begin_date}}' AND '{{params.end_date}}'",
        params={"begin_date": "2020-01-01", "end_date": "2020-12-31"},
        ydb_conn_id="ydb_default1",
        dag_id="test_template_body_templating_dag",
        task_id="test_template_body_templating_task",
        execution_date=timezone.datetime(2024, 2, 1, tzinfo=timezone.utc),
    )
    ti.render_templates()
    task: YDBExecuteQueryOperator = ti.task
    assert (
        task.sql
        == "SELECT * FROM pet WHERE birth_date BETWEEN '2020-01-01' AND '2020-12-31'"
    )


class FakeDriver:
    def wait(*args, **kwargs):
        pass


class FakeSessionPoolImpl:
    def __init__(self, driver):
        self._driver = driver


class FakeTableClient:
    def __init__(self, *args):
        self.bulk_upsert_args = []

    def bulk_upsert(self, table_path, rows, column_types, settings=None):
        assert settings is None
        self.bulk_upsert_args.append((table_path, rows, column_types))


class FakeSessionPool:
    def __init__(self, driver):
        self._pool_impl = FakeSessionPoolImpl(driver)


class FakeYDBCursor:
    def __init__(self, *args, **kwargs):
        self.description = True

    def execute(self, operation, parameters=None):
        return True

    def fetchall(self):
        return "fetchall: result"

    def fetchone(self):
        return "fetchone: result"

    def close(self):
        pass

    @property
    def rowcount(self):
        return 1


class TestYDBExecuteQueryOperator:
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
    @patch(
        "airflow.providers.ydb.hooks._vendor.dbapi.connection.Connection._ydb_table_client_class",
        new_callable=PropertyMock,
    )
    @patch(
        "airflow.providers.ydb.hooks._vendor.dbapi.connection.Connection._cursor_class",
        new_callable=PropertyMock,
    )
    def test_execute_query(
        self,
        cursor_class,
        table_client_class,
        mock_session_pool,
        mock_driver,
        mock_get_connection,
    ):
        mock_get_connection.return_value = Connection(
            conn_type="ydb", host="localhost", extra={"database": "/my_db"}
        )

        cursor_class.return_value = FakeYDBCursor
        table_client_class.return_value = FakeTableClient

        driver = FakeDriver()
        mock_driver.return_value = driver

        session_pool = FakeSessionPool(driver)
        mock_session_pool.return_value = session_pool
        context = {"ti": MagicMock()}
        operator = YDBExecuteQueryOperator(
            task_id="simple_sql",
            sql="select 987",
            is_ddl=False,
            handler=fetch_one_handler,
        )

        results = operator.execute(context)
        assert results == "fetchone: result"

        operator = YDBExecuteQueryOperator(
            task_id="simple_sql",
            sql="select 987",
            is_ddl=False,
            handler=fetch_all_handler,
        )

        results = operator.execute(context)
        assert results == "fetchall: result"

        hook = operator.get_db_hook()

        column_types = (
            ydb.BulkUpsertColumns()
            .add_column("a", ydb.OptionalType(ydb.PrimitiveType.Uint64))
            .add_column("b", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        )

        rows = [
            {"a": 1, "b": "hello"},
            {"a": 888, "b": "world"},
        ]
        hook.bulk_upsert("my_table", rows=rows, column_types=column_types)
        assert len(session_pool._pool_impl._driver.table_client.bulk_upsert_args) == 1
        arg0 = session_pool._pool_impl._driver.table_client.bulk_upsert_args[0]
        assert arg0[0] == "/my_db/my_table"
        assert len(arg0[1]) == 2
