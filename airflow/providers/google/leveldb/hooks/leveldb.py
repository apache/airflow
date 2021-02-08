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
from typing import List, Callable, Optional

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

    def __init__(self, leveldb_conn_id: str = default_conn_name):
        super().__init__()
        self.leveldb_conn_id = leveldb_conn_id
        self.connection = self.get_connection(leveldb_conn_id)
        self.db = None

    def get_conn(self, name: str = '/tmp/testdb/', create_if_missing: bool = False, error_if_exists: bool = False,
                 paranoid_checks: bool = None, write_buffer_size: bool = None, max_open_files: int = None,
                 lru_cache_size: int = None, block_size: int = None, block_restart_interval: int = None,
                 max_file_size: bool = None, compression: str = 'snappy', bloom_filter_bits: int = 0,
                 comparator: Callable = None, comparator_name: bytes = None, ) -> DB:
        """Creates Plyvel DB https://plyvel.readthedocs.io/en/latest/api.html#DB
        :param name(str - path to create database(str, e.g. '/tmp/testdb/')
        :param create_if_missing (bool) – whether a new database should be created if needed
        :param error_if_exists (bool) – whether to raise an exception if the database already exists
        :param  paranoid_checks (bool) – whether to enable paranoid checks
        :param write_buffer_size (int) – size of the write buffer (in bytes)
        :param max_open_files (int) – maximum number of files to keep open
        :param lru_cache_size (int) – size of the LRU cache (in bytes)
        :param block_size (int) – block size (in bytes)
        :param block_restart_interval (int) – block restart interval for delta encoding of keys
        :param max_file_size (bool) – maximum file size (in bytes)
        :param compression (bool) – whether to use Snappy compression (enabled by default))
        :param bloom_filter_bits (int) – the number of bits to use for a bloom filter; the default of 0 means that no
        bloom filter will be used
        :param comparator (callable) – a custom comparator callable that takes two byte strings and returns an integer
        :param comparator_name (bytes) – name for the custom comparator
        """
        if self.db is not None:
            return self.db
        self.db = plyvel.DB(name=name, create_if_missing=create_if_missing, error_if_exists=error_if_exists,
                            paranoid_checks=paranoid_checks, write_buffer_size=write_buffer_size,
                            max_open_files=max_open_files, lru_cache_size=lru_cache_size, block_size=block_size,
                            block_restart_interval=block_restart_interval, max_file_size=max_file_size,
                            compression=compression, bloom_filter_bits=bloom_filter_bits, comparator=comparator,
                            comparator_name=comparator_name)
        return self.db

    def close_conn(self) -> None:
        """Closes connection"""
        db = self.db
        if db is not None:
            db.close()
            self.db = None

    def run(self, command: str, key: bytes, value: bytes = None, keys: List[bytes] = None,
            values: List[bytes] = None) -> Optional[bytes]:
        """
        Execute operation with leveldb
                :param command - command of plyvel(python wrap for leveldb) for DB object (str, e.g. 'put','get',
                'delete','write_batch')
                :param key - key for command(put,get,delete) execution(bytes, e.g. b'key', b'another-key')
                :param value - value for command(put) execution(bytes, e.g. b'value', b'another-value')
                :param keys - keys for command(write_batch) execution(List[bytes], e.g. [b'key', b'another-key'])
                :param values - values for command(write_batch) execution(List[bytes], e.g. [b'value', b'another-value']
                :returns value from get or None(Optional[bytes])
                )
        """
        if command == 'put':
            self.put(key, value)
        elif command == 'get':
            return self.get(key)
        elif command == 'delete':
            self.delete(key)
        elif command == 'write_batch':
            self.write_batch(keys, values)
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
        Delete a single value in a leveldb db by key
            :param key - key for delete execution(bytes, e.g. b'key', b'another-key')
        """
        self.db.delete(key)

    def write_batch(self, keys: List[bytes], values: List[bytes]):
        """
        Write batch of values in a leveldb db by keys
            :param keys - keys for write_batch execution(List[bytes], e.g. [b'key', b'another-key'])
            :param values - values for write_batch execution(List[bytes], e.g. [b'value', b'another-value'])
        """
        with self.db.write_batch() as b:
            for i in range(len(keys)):
                b.put(keys[i], values[i])
