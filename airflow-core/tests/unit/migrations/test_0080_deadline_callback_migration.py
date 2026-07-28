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
Regression tests for migration 0080 (808787349f22):
upgrade() and downgrade() must correctly migrate existing deadline rows
without raising NotNullViolation.
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

# Migration filenames start with a digit so they cannot be imported via the
# normal import system; load the module by file path instead.
_MIGRATION_PATH = (
    Path(AIRFLOW_CORE_SOURCES_PATH)
    / "airflow/migrations/versions/0080_3_1_0_modify_deadline_callback_schema.py"
)
_spec = importlib.util.spec_from_file_location("migration_0080", _MIGRATION_PATH)
_migration = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(_migration)  # type: ignore[union-attr]

upgrade = _migration.upgrade
downgrade = _migration.downgrade
_ASYNC_CALLBACK_CLASSNAME = _migration._ASYNC_CALLBACK_CLASSNAME

_PRE_0080_DDL = """
CREATE TABLE deadline (
    id          TEXT        PRIMARY KEY,
    dagrun_id   INTEGER     NOT NULL,
    deadline_time TEXT      NOT NULL,
    callback    TEXT        NOT NULL,
    callback_kwargs TEXT,
    created_at  TEXT,
    last_updated_at TEXT
)
"""

_POST_0080_DDL = """
CREATE TABLE deadline (
    id          TEXT        PRIMARY KEY,
    dagrun_id   INTEGER     NOT NULL,
    deadline_time TEXT      NOT NULL,
    callback    TEXT        NOT NULL,
    created_at  TEXT,
    last_updated_at TEXT
)
"""


def _make_engine_pre_0080():
    """Return an in-memory SQLite engine with the pre-0080 deadline schema."""
    engine = sa.create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        conn.execute(sa.text(_PRE_0080_DDL))
        conn.commit()
    return engine


def _make_engine_post_0080():
    """Return an in-memory SQLite engine with the post-0080 deadline schema."""
    engine = sa.create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        conn.execute(sa.text(_POST_0080_DDL))
        conn.commit()
    return engine


def _run_upgrade(engine):
    # alembic.context is a proxy that is only populated when running through
    # Alembic's full migration runner (alembic upgrade).  When calling the
    # migration function directly in a test we must mock it so that the
    # is_offline_mode() guard does not raise AttributeError.
    with engine.begin() as conn:
        with Operations.context(MigrationContext.configure(conn)):
            with mock.patch.object(_migration, "context") as mock_ctx:
                mock_ctx.is_offline_mode.return_value = False
                upgrade()


def _run_downgrade(engine):
    with engine.begin() as conn:
        with Operations.context(MigrationContext.configure(conn)):
            with mock.patch.object(_migration, "context") as mock_ctx:
                mock_ctx.is_offline_mode.return_value = False
                downgrade()


def _read_deadline(engine):
    with engine.connect() as conn:
        return conn.execute(sa.text("SELECT * FROM deadline")).mappings().all()


class TestMigration0080Upgrade:
    def test_upgrade_empty_table(self):
        """Upgrade on an empty table must not raise."""
        engine = _make_engine_pre_0080()
        _run_upgrade(engine)
        rows = _read_deadline(engine)
        assert rows == []

    def test_upgrade_migrates_existing_row(self):
        """Upgrade converts VARCHAR callback + JSON kwargs to the expected JSON envelope."""
        engine = _make_engine_pre_0080()
        row_id = str(uuid.uuid4())
        with engine.begin() as conn:
            conn.execute(
                sa.text(
                    "INSERT INTO deadline (id, dagrun_id, deadline_time, callback, callback_kwargs)"
                    " VALUES (:id, 1, '2025-01-01', :cb, :kw)"
                ),
                {"id": row_id, "cb": "mymodule.my_callback", "kw": json.dumps({"key": "val"})},
            )

        _run_upgrade(engine)

        rows = _read_deadline(engine)
        assert len(rows) == 1
        cb = rows[0]["callback"]
        if isinstance(cb, str):
            cb = json.loads(cb)
        assert cb["__classname__"] == _ASYNC_CALLBACK_CLASSNAME
        assert cb["__version__"] == 0
        assert cb["__data__"]["path"] == "mymodule.my_callback"
        assert cb["__data__"]["kwargs"] == {"key": "val"}
        assert "callback_kwargs" not in rows[0]

    def test_upgrade_null_kwargs_defaults_to_empty_dict(self):
        """Upgrade with NULL callback_kwargs must produce an empty dict in the envelope."""
        engine = _make_engine_pre_0080()
        row_id = str(uuid.uuid4())
        with engine.begin() as conn:
            conn.execute(
                sa.text(
                    "INSERT INTO deadline (id, dagrun_id, deadline_time, callback, callback_kwargs)"
                    " VALUES (:id, 1, '2025-01-01', :cb, NULL)"
                ),
                {"id": row_id, "cb": "mymodule.my_callback"},
            )

        _run_upgrade(engine)

        rows = _read_deadline(engine)
        cb = rows[0]["callback"]
        if isinstance(cb, str):
            cb = json.loads(cb)
        assert cb["__data__"]["kwargs"] == {}

    def test_upgrade_exact_batch_boundary(self, monkeypatch):
        """Rows == batch_size must force a second iteration that returns 0 rows and exits cleanly."""
        # Force a small batch_size so 2 inserted rows == batch_size exactly.
        monkeypatch.setattr(_migration.conf, "getint", lambda *a, **kw: 2)
        engine = _make_engine_pre_0080()
        with engine.begin() as conn:
            for i in range(2):
                conn.execute(
                    sa.text(
                        "INSERT INTO deadline (id, dagrun_id, deadline_time, callback, callback_kwargs)"
                        " VALUES (:id, 1, '2025-01-01', :cb, :kw)"
                    ),
                    {"id": str(uuid.uuid4()), "cb": f"mod.cb_{i}", "kw": json.dumps({"i": i})},
                )

        _run_upgrade(engine)

        rows = _read_deadline(engine)
        assert len(rows) == 2
        paths = sorted(
            (json.loads(r["callback"]) if isinstance(r["callback"], str) else r["callback"])["__data__"][
                "path"
            ]
            for r in rows
        )
        assert paths == ["mod.cb_0", "mod.cb_1"]


class TestMigration0080Downgrade:
    def test_downgrade_empty_table(self):
        """Downgrade on an empty table must not raise."""
        engine = _make_engine_post_0080()
        _run_downgrade(engine)
        rows = _read_deadline(engine)
        assert rows == []

    def test_downgrade_restores_existing_row(self):
        """Downgrade extracts path and kwargs back from the JSON envelope."""
        engine = _make_engine_post_0080()
        row_id = str(uuid.uuid4())
        callback_json = json.dumps(
            {
                "__data__": {"path": "mymodule.my_callback", "kwargs": {"key": "val"}},
                "__classname__": _ASYNC_CALLBACK_CLASSNAME,
                "__version__": 0,
            }
        )
        with engine.begin() as conn:
            conn.execute(
                sa.text(
                    "INSERT INTO deadline (id, dagrun_id, deadline_time, callback)"
                    " VALUES (:id, 1, '2025-01-01', :cb)"
                ),
                {"id": row_id, "cb": callback_json},
            )

        _run_downgrade(engine)

        rows = _read_deadline(engine)
        assert len(rows) == 1
        assert rows[0]["callback"] == "mymodule.my_callback"
        kw = rows[0]["callback_kwargs"]
        if isinstance(kw, str):
            kw = json.loads(kw)
        assert kw == {"key": "val"}


class TestMigration0080RoundTrip:
    def test_round_trip_preserves_data(self):
        """Upgrade followed by downgrade preserves the original callback path."""
        engine = _make_engine_pre_0080()
        row_id = str(uuid.uuid4())
        original_path = "mymodule.my_callback"
        original_kwargs = {"x": 1}

        with engine.begin() as conn:
            conn.execute(
                sa.text(
                    "INSERT INTO deadline (id, dagrun_id, deadline_time, callback, callback_kwargs)"
                    " VALUES (:id, 1, '2025-01-01', :cb, :kw)"
                ),
                {"id": row_id, "cb": original_path, "kw": json.dumps(original_kwargs)},
            )

        _run_upgrade(engine)
        _run_downgrade(engine)

        rows = _read_deadline(engine)
        assert len(rows) == 1
        assert rows[0]["callback"] == original_path
        kw = rows[0]["callback_kwargs"]
        if isinstance(kw, str):
            kw = json.loads(kw)
        assert kw == original_kwargs
