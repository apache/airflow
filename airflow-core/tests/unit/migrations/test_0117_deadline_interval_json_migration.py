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

"""
Regression tests for migration 0117 (8812eb67b63c): change deadline_alert.interval to JSON.

The downgrade is intentionally lossy — a serialized ``VariableInterval`` has no numeric
``__data__`` to recover, so it must be converted to NULL. Two defects this guards against:

1. The post-downgrade ``interval`` column must be NULLABLE, otherwise the ``ALTER`` fails with
   a NOT NULL violation on the VariableInterval rows and the whole downgrade crashes.
2. A VariableInterval must downgrade to NULL, NOT a bogus ``0.0`` — the value-conversion must
   gate on the ``datetime.timedelta`` classname before extracting ``__data__`` (a VariableInterval
   also has a ``__data__`` key, but it is a nested object that would ``CAST`` to ``0.0``).
"""

from __future__ import annotations

import importlib.util
import json
import uuid
from pathlib import Path
from unittest import mock

import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations

from tests_common.test_utils.paths import AIRFLOW_CORE_SOURCES_PATH

_MIGRATION_PATH = (
    Path(AIRFLOW_CORE_SOURCES_PATH)
    / "airflow/migrations/versions/0117_3_3_0_change_deadline_interval_to_json.py"
)
_spec = importlib.util.spec_from_file_location("migration_0117", _MIGRATION_PATH)
_migration = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(_migration)  # type: ignore[union-attr]

upgrade = _migration.upgrade
downgrade = _migration.downgrade

# Minimal deadline_alert schema. Pre-0117: interval is FLOAT NOT NULL. The other columns are
# only here so rows can be inserted; their exact types don't matter for the interval migration.
_PRE_0117_DDL = """
CREATE TABLE deadline_alert (
    id               TEXT    PRIMARY KEY,
    serialized_dag_id TEXT,
    name             TEXT,
    reference        TEXT    NOT NULL,
    interval         FLOAT   NOT NULL,
    callback_def     TEXT    NOT NULL,
    created_at       TEXT
)
"""

_POST_0117_DDL = """
CREATE TABLE deadline_alert (
    id               TEXT    PRIMARY KEY,
    serialized_dag_id TEXT,
    name             TEXT,
    reference        TEXT    NOT NULL,
    interval         JSON    NOT NULL,
    callback_def     TEXT    NOT NULL,
    created_at       TEXT
)
"""

_REF = json.dumps({"reference_type": "DagRunQueuedAtDeadline"})
_CB = json.dumps({"path": "my.callback"})

_TIMEDELTA_JSON = json.dumps({"__classname__": "datetime.timedelta", "__version__": 2, "__data__": 300.0})
_VARIABLE_INTERVAL_JSON = json.dumps(
    {
        "__classname__": "airflow.sdk.definitions.deadline.VariableInterval",
        "__version__": 1,
        "__data__": {"key": "deadline_seconds"},
    }
)


def _engine(ddl):
    engine = sa.create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        conn.execute(sa.text(ddl))
        conn.commit()
    return engine


def _run(engine, func):
    with engine.begin() as conn:
        with Operations.context(MigrationContext.configure(conn)):
            with mock.patch.object(_migration, "context") as mock_ctx:
                mock_ctx.is_offline_mode.return_value = False
                func()


def _insert(engine, *, interval, name):
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO deadline_alert (id, serialized_dag_id, name, reference, interval, callback_def) "
                "VALUES (:id, :sdag, :name, :ref, :interval, :cb)"
            ),
            {
                "id": str(uuid.uuid4()),
                "sdag": str(uuid.uuid4()),
                "name": name,
                "ref": _REF,
                "interval": interval,
                "cb": _CB,
            },
        )


def _rows(engine):
    with engine.connect() as conn:
        return {
            r["name"]: r["interval"]
            for r in conn.execute(sa.text("SELECT name, interval FROM deadline_alert")).mappings()
        }


class TestMigration0117Upgrade:
    def test_upgrade_empty_table(self):
        engine = _engine(_PRE_0117_DDL)
        _run(engine, upgrade)
        assert _rows(engine) == {}

    def test_upgrade_wraps_float_in_timedelta_envelope(self):
        engine = _engine(_PRE_0117_DDL)
        _insert(engine, interval=300.0, name="fixed")
        _run(engine, upgrade)
        stored = json.loads(_rows(engine)["fixed"])
        assert stored["__classname__"] == "datetime.timedelta"
        assert stored["__data__"] == 300.0


class TestMigration0117Downgrade:
    def test_downgrade_timedelta_recovers_float(self):
        engine = _engine(_POST_0117_DDL)
        _insert(engine, interval=_TIMEDELTA_JSON, name="fixed")
        _run(engine, downgrade)
        assert _rows(engine)["fixed"] == 300.0

    def test_downgrade_variable_interval_becomes_null_not_zero(self):
        """The core regression: a VariableInterval must downgrade to NULL, never a bogus 0.0,
        and the NOT NULL constraint must not crash the downgrade."""
        engine = _engine(_POST_0117_DDL)
        _insert(engine, interval=_VARIABLE_INTERVAL_JSON, name="dynamic")
        _insert(engine, interval=_TIMEDELTA_JSON, name="fixed")
        _run(engine, downgrade)
        rows = _rows(engine)
        assert rows["fixed"] == 300.0
        assert rows["dynamic"] is None, (
            "VariableInterval must downgrade to NULL (lossy), not be silently coerced to 0.0"
        )

    def test_downgrade_only_variable_intervals_does_not_crash(self):
        """A table containing ONLY VariableInterval rows downgrades cleanly (all NULL) — this is
        the case that crashes if the resulting FLOAT column is left NOT NULL."""
        engine = _engine(_POST_0117_DDL)
        _insert(engine, interval=_VARIABLE_INTERVAL_JSON, name="d1")
        _insert(engine, interval=_VARIABLE_INTERVAL_JSON, name="d2")
        _run(engine, downgrade)
        assert _rows(engine) == {"d1": None, "d2": None}


class TestMigration0117RoundTrip:
    def test_timedelta_round_trip_preserves_value(self):
        engine = _engine(_PRE_0117_DDL)
        _insert(engine, interval=42.0, name="rt")
        _run(engine, upgrade)
        _run(engine, downgrade)
        assert _rows(engine)["rt"] == 42.0
