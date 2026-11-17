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
"""
A real ``DbApiHook`` over a SQLite file, for tests that need a database to fail like one.

The sqlite provider is not a dependency of this one, so the hook is defined here over the
standard library's ``sqlite3``. Statements run through ``DbApiHook.run`` and introspection
through the SQLAlchemy inspector, so the errors the toolset sees are the driver's own.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import closing, contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from airflow.providers.common.compat.sdk import AirflowNotFoundException
from airflow.providers.common.sql.hooks.sql import DbApiHook


class SQLiteFileHook(DbApiHook):
    conn_name_attr = "sqlite_conn_id"
    default_conn_name = "sqlite_default"

    def __init__(self, path: Path) -> None:
        super().__init__()
        self.path = path

    def get_conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path)

    def get_uri(self) -> str:
        return f"sqlite:///{self.path}"


def run_sql(path: Path, *statements: str) -> None:
    """Run ``statements`` against the database at ``path``, creating it if needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(path)) as connection:
        for statement in statements:
            connection.execute(statement)
        connection.commit()


def create_orders_table(path: Path) -> None:
    """Create the database at ``path`` with an ``orders`` table of two rows."""
    run_sql(
        path,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT)",
        "INSERT INTO orders (status) VALUES ('shipped'), ('pending')",
    )


@contextmanager
def sqlite_connection(conn_id: str, path: Path) -> Iterator[None]:
    """Resolve the Airflow connection ``conn_id`` to a fresh :class:`SQLiteFileHook` over ``path``."""

    def get_connection(requested: str) -> SimpleNamespace:
        if requested != conn_id:
            raise AirflowNotFoundException(f"The conn_id `{requested}` isn't defined")
        return SimpleNamespace(get_hook=lambda: SQLiteFileHook(path))

    with patch(
        "airflow.providers.common.ai.toolsets.sql.BaseHook.get_connection",
        autospec=True,
        side_effect=get_connection,
    ):
        yield
