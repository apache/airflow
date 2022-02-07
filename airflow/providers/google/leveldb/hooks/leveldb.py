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
from typing import List, Optional

try:
    import plyvel
    from plyvel import DB

    from airflow.exceptions import AirflowException
    from airflow.hooks.base import BaseHook

except ImportError as e:
    # Plyvel is an optional feature and if imports are missing, it should be silently ignored
    # As of Airflow 2.3  and above the operator can throw OptionalProviderFeatureException
    try:
        from airflow.exceptions import AirflowOptionalProviderFeatureException
    except ImportError:
        # However, in order to keep backwards-compatibility with Airflow 2.1 and 2.2, if the
        # 2.3 exception cannot be imported, the original ImportError should be raised.
        # This try/except can be removed when the provider depends on Airflow >= 2.3.0
        raise e from None
    raise AirflowOptionalProviderFeatureException(e)

DB_NOT_INITIALIZED_BEFORE = "The `get_conn` method should be called before!"


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
        self.db: Optional[plyvel.DB] = None

    def get_conn(self, name: str = '/tmp/testdb/', create_if_missing: bool = False, **kwargs) -> DB:
        """
        Creates `Plyvel DB <https://plyvel.readthedocs.io/en/latest/api.html#DB>`__

        :param name: path to create database e.g. `/tmp/testdb/`)
        :param create_if_missing: whether a new database should be created if needed
        :param kwargs: other options of creation plyvel.DB. See more in the link above.
        :returns: DB
        :rtype: plyvel.DB
        """
        if self.db is not None:
            return self.db
        self.db = plyvel.DB(name=name, create_if_missing=create_if_missing, **kwargs)
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
        value: Optional[bytes] = None,
        keys: Optional[List[bytes]] = None,
        values: Optional[List[bytes]] = None,
    ) -> Optional[bytes]:
        """
        Execute operation with leveldb

        :param command: command of plyvel(python wrap for leveldb) for DB object e.g.
            ``"put"``, ``"get"``, ``"delete"``, ``"write_batch"``.
        :param key: key for command(put,get,delete) execution(, e.g. ``b'key'``, ``b'another-key'``)
        :param value: value for command(put) execution(bytes, e.g. ``b'value'``, ``b'another-value'``)
        :param keys: keys for command(write_batch) execution(List[bytes], e.g. ``[b'key', b'another-key'])``
        :param values: values for command(write_batch) execution e.g. ``[b'value'``, ``b'another-value']``
        :returns: value from get or None
        :rtype: Optional[bytes]
        """
        if command == 'put':
            if not value:
                raise Exception("Please provide `value`!")
            return self.put(key, value)
        elif command == 'get':
            return self.get(key)
        elif command == 'delete':
            return self.delete(key)
        elif command == 'write_batch':
            if not keys:
                raise Exception("Please provide `keys`!")
            if not values:
                raise Exception("Please provide `values`!")
            return self.write_batch(keys, values)
        else:
            raise LevelDBHookException("Unknown command for LevelDB hook")

    def put(self, key: bytes, value: bytes):
        """
        Put a single value into a leveldb db by key

        :param key: key for put execution, e.g. ``b'key'``, ``b'another-key'``
        :param value: value for put execution e.g. ``b'value'``, ``b'another-value'``
        """
        if not self.db:
            raise Exception(DB_NOT_INITIALIZED_BEFORE)
        self.db.put(key, value)

    def get(self, key: bytes) -> bytes:
        """
        Get a single value into a leveldb db by key

        :param key: key for get execution, e.g. ``b'key'``, ``b'another-key'``
        :returns: value of key from db.get
        :rtype: bytes
        """
        if not self.db:
            raise Exception(DB_NOT_INITIALIZED_BEFORE)
        return self.db.get(key)

    def delete(self, key: bytes):
        """
        Delete a single value in a leveldb db by key.

        :param key: key for delete execution, e.g. ``b'key'``, ``b'another-key'``
        """
        if not self.db:
            raise Exception(DB_NOT_INITIALIZED_BEFORE)
        self.db.delete(key)

    def write_batch(self, keys: List[bytes], values: List[bytes]):
        """
        Write batch of values in a leveldb db by keys

        :param keys: keys for write_batch execution e.g. ``[b'key', b'another-key']``
        :param values: values for write_batch execution e.g. ``[b'value', b'another-value']``
        """
        if not self.db:
            raise Exception(DB_NOT_INITIALIZED_BEFORE)
        with self.db.write_batch() as batch:
            for i, key in enumerate(keys):
                batch.put(key, values[i])
