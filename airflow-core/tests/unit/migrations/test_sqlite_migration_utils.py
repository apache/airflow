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

import pytest

from airflow.migrations.utils import disable_sqlite_fkeys


class _Dialect:
    def __init__(self, name: str) -> None:
        self.name = name


class _Bind:
    def __init__(self, dialect_name: str) -> None:
        self.dialect = _Dialect(name=dialect_name)


class _Context:
    def __init__(self, dialect_name: str) -> None:
        self.dialect = _Dialect(name=dialect_name)


class _FakeOp:
    def __init__(self, dialect_name: str) -> None:
        self._bind = _Bind(dialect_name=dialect_name)
        self.executed: list[str] = []

    def get_bind(self) -> _Bind:
        return self._bind

    def execute(self, statement: str) -> None:
        self.executed.append(statement)


class _OfflineFakeOp:
    """Simulates Alembic offline mode where get_bind() returns None."""

    def __init__(self, dialect_name: str) -> None:
        self._context = _Context(dialect_name=dialect_name)
        self.executed: list[str] = []

    def get_bind(self) -> None:
        return None

    def get_context(self) -> _Context:
        return self._context

    def execute(self, statement: str) -> None:
        self.executed.append(statement)


def test_disable_sqlite_fkeys_restores_pragma_on_exception() -> None:
    op = _FakeOp(dialect_name="sqlite")

    with pytest.raises(RuntimeError, match="boom"):
        with disable_sqlite_fkeys(op):
            raise RuntimeError("boom")

    assert op.executed == ["PRAGMA foreign_keys=off", "PRAGMA foreign_keys=on"]


def test_disable_sqlite_fkeys_noop_for_non_sqlite() -> None:
    op = _FakeOp(dialect_name="postgresql")

    with disable_sqlite_fkeys(op) as yielded_op:
        assert yielded_op is op

    assert op.executed == []


def test_disable_sqlite_fkeys_offline_mode_sqlite() -> None:
    """Alembic offline mode: get_bind() returns None; dialect comes from the migration context."""
    op = _OfflineFakeOp(dialect_name="sqlite")

    with disable_sqlite_fkeys(op) as yielded_op:
        assert yielded_op is op

    assert op.executed == ["PRAGMA foreign_keys=off", "PRAGMA foreign_keys=on"]


def test_disable_sqlite_fkeys_offline_mode_non_sqlite() -> None:
    """Alembic offline mode: get_bind() returns None; non-SQLite dialect is a noop."""
    op = _OfflineFakeOp(dialect_name="postgresql")

    with disable_sqlite_fkeys(op) as yielded_op:
        assert yielded_op is op

    assert op.executed == []
