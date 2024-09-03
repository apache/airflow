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

from unittest.mock import PropertyMock, patch

from airflow.models import Connection
from airflow.providers.ydb.hooks.ydb import YDBHook


class FakeDriver:
    def wait(*args, **kwargs):
        pass


class FakeSessionPoolImpl:
    def __init__(self, driver):
        self._driver = driver


class FakeSessionPool:
    def __init__(self, driver):
        self._pool_impl = FakeSessionPoolImpl(driver)


class FakeYDBCursor:
    def __init__(self, *args, **kwargs):
        self.description = True

    def execute(self, operation, parameters=None):
        return True

    def fetchone(self):
        return 1, 2

    def fetchmany(self, size=None):
        return [(1, 2), (2, 3), (3, 4)][0:size]

    def fetchall(self):
        return [(1, 2), (2, 3), (3, 4)]

    def close(self):
        pass

    @property
    def rowcount(self):
        return 1


@patch("airflow.hooks.base.BaseHook.get_connection")
@patch("ydb.Driver")
@patch("ydb.SessionPool")
@patch(
    "airflow.providers.ydb.hooks._vendor.dbapi.connection.Connection._cursor_class", new_callable=PropertyMock
)
def test_execute(cursor_class, mock_session_pool, mock_driver, mock_get_connection):
    mock_get_connection.return_value = Connection(
        conn_type="ydb",
        host="grpc://localhost",
        port=2135,
        login="my_user",
        password="my_pwd",
        extra={"database": "/my_db1"},
    )
    driver_instance = FakeDriver()

    cursor_class.return_value = FakeYDBCursor
    mock_driver.return_value = driver_instance
    mock_session_pool.return_value = FakeSessionPool(driver_instance)

    hook = YDBHook()
    assert hook.get_uri() == "ydb://grpc://my_user:my_pwd@localhost:2135/?database=%2Fmy_db1"
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            assert cur.execute("INSERT INTO table VALUES ('aaa'), ('bbbb')")
            conn.commit()
            assert cur.execute("SELECT * FROM table")
            assert cur.fetchone() == (1, 2)
            assert cur.fetchmany(2) == [(1, 2), (2, 3)]
            assert cur.fetchall() == [(1, 2), (2, 3), (3, 4)]
