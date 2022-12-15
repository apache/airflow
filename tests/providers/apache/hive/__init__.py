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
from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

from airflow.models.dag import DAG
from airflow.providers.apache.hive.hooks.hive import HiveCliHook, HiveMetastoreHook, HiveServer2Hook
from airflow.providers.mysql.hooks.mysql import MySqlHook

DEFAULT_DATE = datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]


class TestHiveEnvironment:
    def setup_method(self, method):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        dag = DAG("test_dag_id", default_args=args)
        self.dag = dag
        self.hql = """
        USE airflow;
        DROP TABLE IF EXISTS static_babynames_partitioned;
        CREATE TABLE IF NOT EXISTS static_babynames_partitioned (
            state string,
            year string,
            name string,
            gender string,
            num int)
        PARTITIONED BY (ds string);
        INSERT OVERWRITE TABLE static_babynames_partitioned
            PARTITION(ds='{{ ds }}')
        SELECT state, year, name, gender, num FROM static_babynames;
        """


class MockHiveMetastoreHook(HiveMetastoreHook):
    def __init__(self, *args, **kwargs):
        self._find_valid_server = MagicMock(return_value={})
        self.get_metastore_client = MagicMock(return_value=MagicMock())
        super().__init__()


class MockHiveCliHook(HiveCliHook):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self.conn = MockConnectionCursor()
        self.conn.schema = "default"
        self.conn.host = "localhost"
        self.conn.port = 10000
        self.conn.login = None
        self.conn.password = None

        self.conn.execute = MagicMock()
        self.get_conn = MagicMock(return_value=self.conn)
        self.get_connection = MagicMock(return_value=MockDBConnection({}))


class InvalidHiveCliHook(HiveCliHook):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self.conn = MockConnectionCursor()
        self.conn.schema = "default;"
        self.conn.host = "localhost"
        self.conn.port = 10000
        self.conn.login = None
        self.conn.password = None

        self.conn.execute = MagicMock()
        self.get_conn = MagicMock(return_value=self.conn)
        self.get_connection = MagicMock(return_value=MockDBConnection({}))


class MockHiveServer2Hook(HiveServer2Hook):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self.mock_cursor = kwargs.get("connection_cursor", MockConnectionCursor())
        self.mock_cursor.execute = MagicMock()
        self.get_conn = MagicMock(return_value=self.mock_cursor)
        self.get_connection = MagicMock(return_value=MockDBConnection({}))


class MockMySqlHook(MySqlHook):
    def __init__(self, *args, **kwargs):
        self.conn = MockConnectionCursor()

        self.conn.execute = MagicMock()
        self.get_conn = MagicMock(return_value=self.conn)
        self.get_records = MagicMock(return_value=[])
        self.insert_rows = MagicMock(return_value=True)
        super().__init__(*args, **kwargs)

    def get_connection(self, *args, **kwargs):
        return self.conn


class MockDBConnection:
    def __init__(self, extra_dejson=None, *args, **kwargs):
        self.extra_dejson = extra_dejson
        self.get_records = MagicMock(return_value=[["test_record"]])

        output = kwargs.get("output", ["" for _ in range(10)])
        self.readline = MagicMock(side_effect=[line.encode() for line in output])

    def status(self, *args, **kwargs):
        return True


class BaseMockConnectionCursor:
    def __init__(self, **kwargs):
        self.arraysize = None
        self.description = [
            ("hive_server_hook.a", "INT_TYPE", None, None, None, None, True),
            ("hive_server_hook.b", "INT_TYPE", None, None, None, None, True),
        ]
        self.conn_exists = kwargs.get("exists", True)

    def close(self):
        pass

    def cursor(self):
        return self

    def execute(self, values=None):
        pass

    def exists(self):
        return self.conn_exists

    def isfile(self):
        return True

    def remove(self):
        pass

    def upload(self, local_filepath, destination_filepath):
        pass

    def __next__(self):
        return self.iterable

    def __iter__(self):
        yield from self.iterable


class MockConnectionCursor(BaseMockConnectionCursor):
    def __init__(self):
        super().__init__()
        self.iterable = [(1, 1), (2, 2)]


class MockStdOut:
    def __init__(self, *args, **kwargs):
        output = kwargs.get("output", ["" for _ in range(10)])
        self.readline = MagicMock(side_effect=[line.encode() for line in output])


class MockSubProcess:
    PIPE = -1
    STDOUT = -2
    returncode: int | None = None

    def __init__(self, *args, **kwargs):
        self.stdout = MockStdOut(*args, **kwargs)

    def wait(self):
        return
