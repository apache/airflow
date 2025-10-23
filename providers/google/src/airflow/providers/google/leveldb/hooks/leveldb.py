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
"""Hook for Level DB."""

from __future__ import annotations

from typing import Any

from airflow.exceptions import AirflowException, AirflowOptionalProviderFeatureException
from airflow.providers.common.compat.sdk import BaseHook

try:
    import plyvel
    from plyvel import DB
except ImportError as e:
    raise AirflowOptionalProviderFeatureException(e)

DB_NOT_INITIALIZED_BEFORE = "The `get_conn` method should be called before!"


class LevelDBHookException(AirflowException):
    """Exception specific for LevelDB."""


class LevelDBHook(BaseHook):
    """
    Plyvel Wrapper to Interact With LevelDB Database.

    `LevelDB Connection Documentation <https://plyvel.readthedocs.io/en/latest/>`__
    """

    conn_name_attr = "leveldb_conn_id"
    default_conn_name = "leveldb_default"
    conn_type = "leveldb"
    hook_name = "LevelDB"

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to LevelDB connection form."""
        from flask_babel import lazy_gettext
        from wtforms import BooleanField

        return {
            "create_if_missing": BooleanField(
                lazy_gettext("Create a database if it does not exist"), default=False
            ),
            "error_if_exists": BooleanField(
                lazy_gettext("Raise an exception if the database already exists"), default=False
            ),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom UI field behaviour for LevelDB connection."""
        return {
            "hidden_fields": ["login", "password", "schema", "port"],
            "relabeling": {},
        }

    def __init__(self, leveldb_conn_id: str = default_conn_name):
        super().__init__()
        self.leveldb_conn_id = leveldb_conn_id
        self.connection = self.get_connection(leveldb_conn_id)
        self.db: plyvel.DB | None = None

    def get_conn(self, name: str = "/tmp/testdb/", create_if_missing: bool = False, **kwargs) -> DB:
        """
        Create `Plyvel DB <https://plyvel.readthedocs.io/en/latest/api.html#DB>`__.

        :param name: path to create database e.g. `/tmp/testdb/`)
        :param create_if_missing: whether a new database should be created if needed
        :param kwargs: other options of creation plyvel.DB. See more in the link above.
        :returns: DB
        """
        if self.db is not None:
            return self.db
        self.db = plyvel.DB(name=name, create_if_missing=create_if_missing, **kwargs)
        return self.db

    def close_conn(self) -> None:
        """Close connection."""
        db = self.db
        if db is not None:
            db.close()
            self.db = None

    def run(
        self,
        command: str,
        key: bytes,
        value: bytes | None = None,
        keys: list[bytes] | None = None,
        values: list[bytes] | None = None,
    ) -> bytes | None:
        """
        Execute operation with leveldb.

        :param command: command of plyvel(python wrap for leveldb) for DB object e.g.
            ``"put"``, ``"get"``, ``"delete"``, ``"write_batch"``.
        :param key: key for command(put,get,delete) execution(, e.g. ``b'key'``, ``b'another-key'``)
        :param value: value for command(put) execution(bytes, e.g. ``b'value'``, ``b'another-value'``)
        :param keys: keys for command(write_batch) execution(list[bytes], e.g. ``[b'key', b'another-key'])``
        :param values: values for command(write_batch) execution e.g. ``[b'value'``, ``b'another-value']``
        :returns: value from get or None
        """
        if command == "put":
            if not value:
                raise ValueError("Please provide `value`!")
            return self.put(key, value)
        if command == "get":
            return self.get(key)
        if command == "delete":
            return self.delete(key)
        if command == "write_batch":
            if not keys:
                raise ValueError("Please provide `keys`!")
            if not values:
                raise ValueError("Please provide `values`!")
            return self.write_batch(keys, values)
        raise LevelDBHookException("Unknown command for LevelDB hook")

    def put(self, key: bytes, value: bytes):
        """
        Put a single value into a leveldb db by key.

        :param key: key for put execution, e.g. ``b'key'``, ``b'another-key'``
        :param value: value for put execution e.g. ``b'value'``, ``b'another-value'``
        """
        if not self.db:
            raise AirflowException(DB_NOT_INITIALIZED_BEFORE)
        self.db.put(key, value)

    def get(self, key: bytes) -> bytes:
        """
        Get a single value into a leveldb db by key.

        :param key: key for get execution, e.g. ``b'key'``, ``b'another-key'``
        :returns: value of key from db.get
        """
        if not self.db:
            raise AirflowException(DB_NOT_INITIALIZED_BEFORE)
        return self.db.get(key)

    def delete(self, key: bytes):
        """
        Delete a single value in a leveldb db by key.

        :param key: key for delete execution, e.g. ``b'key'``, ``b'another-key'``
        """
        if not self.db:
            raise AirflowException(DB_NOT_INITIALIZED_BEFORE)
        self.db.delete(key)

    def write_batch(self, keys: list[bytes], values: list[bytes]):
        """
        Write batch of values in a leveldb db by keys.

        :param keys: keys for write_batch execution e.g. ``[b'key', b'another-key']``
        :param values: values for write_batch execution e.g. ``[b'value', b'another-value']``
        """
        if not self.db:
            raise AirflowException(DB_NOT_INITIALIZED_BEFORE)
        with self.db.write_batch() as batch:
            for i, key in enumerate(keys):
                batch.put(key, values[i])
