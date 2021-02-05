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
"""Hook for Level DB"""
import plyvel
from plyvel import DB

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class LevelDBHookException(AirflowException):
    """Exception specific for LevelDB"""


class LevelDBHook(BaseHook):
    """
    Plyvel Wrapper to Interact With LevelDB Database
    LevelDB Connection Documentation https://plyvel.readthedocs.io/en/latest/
    """

    conn_name_attr = 'leveldb_conn_id'
    default_conn_name = 'leveldb_default'
    conn_type = 'leveldb'
    hook_name = 'LevelDB'

    def __init__(self, conn_id: str = default_conn_name, *args, **kwargs):
        super().__init__()
        self.leveldb_conn_id = conn_id
        self.connection = self.get_connection(conn_id)
        self.db = None

    def __exit__(self):
        self.close_conn()

    def get_conn(self, path: str = '/tmp/testdb/') -> DB:
        """Fetches Plyvel DB
        :param path - path to create database(str, e.g. '/tmp/testdb/')
        """
        if self.db is not None:
            return self.db
        self.db = plyvel.DB(path, create_if_missing=True)
        return self.db

    def close_conn(self) -> None:
        """Closes connection"""
        db = self.db
        if db is not None:
            db.close()
            self.db = None

    def run(self, command: str, key: bytes, value: bytes = None):
        """
        Execute operation with leveldb
                :param command - command of plyvel(python wrap for leveldb) for DB object (str, e.g. 'put','get','delete')
                :param key - key for command(put,get,delete) execution(bytes, e.g. b'key', b'another-key')
                :param value - value for command(put) execution(bytes, e.g. b'value', b'another-value')
        """
        if command == 'put':
            self.put(key, value)
        elif command == 'get':
            self.get(key)
        elif command == 'delete':
            self.get(key)
        else:
            raise LevelDBHookException("Unknown command for LevelDB hook")

    def put(self, key: bytes, value: bytes):
        """
        Put a single value into a leveldb db by key
            :param key - key for put execution(bytes, e.g. b'key', b'another-key')
            :param value - value for put execution(bytes, e.g. b'value', b'another-value')
        """
        self.db.put(key, value)

    def get(self, key: bytes) -> bytes:
        """
        Get a single value into a leveldb db by key
            :param key - key for get execution(bytes, e.g. b'key', b'another-key')
            :returns value(bytes, e.g. b'value', b'another-value')
        """
        return self.db.get(key)

    def delete(self, key: bytes):
        """
        Delete a single document a single value in a leveldb db by key
            :param key - key for delete execution(bytes, e.g. b'key', b'another-key')
        """
        self.db.delete(key)
