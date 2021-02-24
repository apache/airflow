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
from typing import Callable, List, Optional

import plyvel
from plyvel import DB

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class LevelDBHookException(AirflowException):
    """Exception specific for LevelDB"""


class LevelDBHook(BaseHook):
    """
    Plyvel Wrapper to Interact With LevelDB Database
    `LevelDB Connection Documentation <https://plyvel.readthedocs.io/en/latest/>`__
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

    # pylint: disable=too-many-arguments
    # Fifteen is reasonable in this case.

    def get_conn(
        self,
        name: str = '/tmp/testdb/',
        create_if_missing: bool = False,
        error_if_exists: bool = False,
        paranoid_checks: bool = None,
        write_buffer_size: bool = None,
        max_open_files: int = None,
        lru_cache_size: int = None,
        block_size: int = None,
        block_restart_interval: int = None,
        max_file_size: bool = None,
        compression: str = 'snappy',
        bloom_filter_bits: int = 0,
        comparator: Callable = None,
        comparator_name: bytes = None,
    ) -> DB:
        """
        Creates `Plyvel DB <https://plyvel.readthedocs.io/en/latest/api.html#DB>`__

        :param name: path to create database e.g. `/tmp/testdb/`)
        :type name: str
        :param create_if_missing: whether a new database should be created if needed
        :type create_if_missing: bool
        :param error_if_exists: whether to raise an exception if the database already exists
        :type error_if_exists: bool
        :param paranoid_checks: whether to enable paranoid checks
        :type paranoid_checks: bool
        :param write_buffer_size: size of the write buffer (in bytes)
        :type write_buffer_size: int
        :param max_open_files: maximum number of files to keep open
        :type max_open_files: int
        :param lru_cache_size: size of the LRU cache (in bytes)
        :type lru_cache_size: int
        :param block_size: block size (in bytes)
        :type block_size: int
        :param block_restart_interval: block restart interval for delta encoding of keys
        :type block_restart_interval: int
        :param max_file_size: maximum file size (in bytes)
        :type max_file_size: bool
        :param compression: whether to use Snappy compression (enabled by default))
        :type compression: bool
        :param bloom_filter_bits: the number of bits to use for a bloom filter; the default of 0
            means that no bloom filter will be used
        :type bloom_filter_bits: int
        :param comparator: a custom comparator callable that takes two byte strings and
            returns an integer
        :type comparator: callable
        :param comparator_name: name for the custom comparator
        :type comparator_name: bytes
        """
        if self.db is not None:
            return self.db
        self.db = plyvel.DB(
            name=name,
            create_if_missing=create_if_missing,
            error_if_exists=error_if_exists,
            paranoid_checks=paranoid_checks,
            write_buffer_size=write_buffer_size,
            max_open_files=max_open_files,
            lru_cache_size=lru_cache_size,
            block_size=block_size,
            block_restart_interval=block_restart_interval,
            max_file_size=max_file_size,
            compression=compression,
            bloom_filter_bits=bloom_filter_bits,
            comparator=comparator,
            comparator_name=comparator_name,
        )
        return self.db

    def close_conn(self) -> None:
        """Closes connection"""
        db = self.db
        if db is not None:
            db.close()
            self.db = None

    def run(
        self,
        command: str,
        key: bytes,
        value: bytes = None,
        keys: List[bytes] = None,
        values: List[bytes] = None,
    ) -> Optional[bytes]:
        """
        Execute operation with leveldb

        :param command: command of plyvel(python wrap for leveldb) for DB object e.g.
            ``"put"``, ``"get"``, ``"delete"``, ``"write_batch"``.
        :type command: str
        :param key: key for command(put,get,delete) execution(, e.g. ``b'key'``, ``b'another-key'``)
        :type key: bytes
        :param value: value for command(put) execution(bytes, e.g. ``b'value'``, ``b'another-value'``)
        :type value: bytes
        :param keys: keys for command(write_batch) execution(List[bytes], e.g. ``[b'key', b'another-key'])``
        :type keys: List[bytes]
        :param values: values for command(write_batch) execution e.g. ``[b'value'``, ``b'another-value']``
        :param values: List[bytes]
        :returns: value from get or None
        :rtype: Optional[bytes]
        """
        if command == 'put':
            return self.put(key, value)
        elif command == 'get':
            return self.get(key)
        elif command == 'delete':
            return self.delete(key)
        elif command == 'write_batch':
            return self.write_batch(keys, values)
        else:
            raise LevelDBHookException("Unknown command for LevelDB hook")

    def put(self, key: bytes, value: bytes):
        """
        Put a single value into a leveldb db by key

        :param key: key for put execution, e.g. ``b'key'``, ``b'another-key'``
        :type key: bytes
        :param value: value for put execution e.g. ``b'value'``, ``b'another-value'``
        :type value: bytes
        """
        self.db.put(key, value)

    def get(self, key: bytes) -> bytes:
        """
        Get a single value into a leveldb db by key

        :param key: key for get execution, e.g. ``b'key'``, ``b'another-key'``
        :type key: bytes
        :param value: value for get execution e.g. ``b'value'``, ``b'another-value'``
        :type value: bytes
        """
        return self.db.get(key)

    def delete(self, key: bytes):
        """
        Delete a single value in a leveldb db by key.

        :param key: key for delete execution, e.g. ``b'key'``, ``b'another-key'``
        :type key: bytes
        """
        self.db.delete(key)

    def write_batch(self, keys: List[bytes], values: List[bytes]):
        """
        Write batch of values in a leveldb db by keys

        :param keys: keys for write_batch execution e.g. ``[b'key', b'another-key']``
        :param keys: List[bytes]
        :param values: values for write_batch execution e.g. ``[b'value', b'another-value']``
        :param values: List[bytes]

        """
        with self.db.write_batch() as batch:
            for i, key in enumerate(keys):
                batch.put(key, values[i])
