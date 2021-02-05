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
import unittest
from unittest import mock

from airflow.providers.google.leveldb.hooks.leveldb import LevelDBHook


class TestLevelDBHook(unittest.TestCase):
    @mock.patch.dict('os.environ', AIRFLOW_CONN_LEVELDB_DEFAULT="test")
    def test_get_conn_db_is_not_none(self):
        hook = LevelDBHook(leveldb_conn_id='leveldb_default')
        hook.get_conn()
        assert hook.db is not None, "Check existence of DB object in connection creation"

    @mock.patch.dict('os.environ', AIRFLOW_CONN_LEVELDB_DEFAULT="test")
    def test_real_get_put_delete(self):
        hook = LevelDBHook(leveldb_conn_id='leveldb_default')
        leveldb = hook.get_conn()
        assert leveldb.get(b'test_key') is None, "Initial LevelDB is empty"
        leveldb.put(b'test_key', b'test_value')
        assert leveldb.get(b'test_key') == b'test_value', 'Connection to LevelDB with PUT and GET works.'
        leveldb.delete(b'test_key')
        assert leveldb.get(b'test_key') is None, 'Connection to LevelDB with DELETE works.'
