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

from tests.compat import MagicMock

from airflow.hooks.hive_hooks import HiveMetastoreHook, HiveCliHook, HiveServer2Hook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.presto_hook import PrestoHook
from airflow.hooks.samba_hook import SambaHook
from tests.test_utils.mock_process import MockConnectionCursor, MockDBConnection


class MockHiveMetastoreHook(HiveMetastoreHook):
    def __init__(self, *args, **kwargs):
        self._find_valid_server = MagicMock(return_value={})
        self.get_metastore_client = MagicMock(
            return_value=MagicMock())
        super(MockHiveMetastoreHook, self).__init__()


class MockHiveCliHook(HiveCliHook):
    def __init__(self, *args, **kwargs):
        super(MockHiveCliHook, self).__init__()
        self.conn = MockConnectionCursor()
        self.conn.schema = 'default'
        self.conn.host = 'localhost'
        self.conn.port = 10000
        self.conn.login = None
        self.conn.password = None

        self.conn.execute = MagicMock()
        self.get_conn = MagicMock(return_value=self.conn)
        self.get_connection = MagicMock(return_value=MockDBConnection({}))


class MockSambaHook(SambaHook):
    def __init__(self, *args, **kwargs):
        self.conn = MockConnectionCursor()
        self.conn.execute = MagicMock()
        self.get_conn = MagicMock(return_value=self.conn)
        super(MockSambaHook, self).__init__(*args, **kwargs)

    def get_connection(self, *args):
        return self.conn


class MockPrestoHook(PrestoHook):
    def __init__(self, *args, **kwargs):
        self.conn = MockConnectionCursor()

        self.conn.execute = MagicMock()
        self.get_conn = MagicMock(return_value=self.conn)
        self.get_first = MagicMock(
            return_value=[['val_0', 'val_1'], 'val_2'])

        super(MockPrestoHook, self).__init__(*args, **kwargs)

    def get_connection(self, *args):
        return self.conn


class MockMySqlHook(MySqlHook):
    def __init__(self, *args, **kwargs):
        self.conn = MockConnectionCursor()

        self.conn.execute = MagicMock()
        self.get_conn = MagicMock(return_value=self.conn)
        self.get_records = MagicMock(return_value=[])
        self.insert_rows = MagicMock(return_value=True)
        super(MockMySqlHook, self).__init__(*args, **kwargs)

    def get_connection(self, *args, **kwargs):
        return self.conn


class MockHiveServer2Hook(HiveServer2Hook):
    def __init__(self, *args, **kwargs):
        super(MockHiveServer2Hook, self).__init__()
        self.mock_cursor = kwargs.get('connection_cursor', MockConnectionCursor())
        self.mock_cursor.execute = MagicMock()
        self.get_conn = MagicMock(return_value=self.mock_cursor)
        self.get_connection = MagicMock(return_value=MockDBConnection({}))
