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

from __future__ import annotations

import sys
import types
from contextlib import contextmanager
from typing import Any

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from tests_common import pytest_plugin


def _create_sqlite_db(sqlite_db_url: str, table_names: set[str]) -> None:
    engine = create_engine(sqlite_db_url)
    try:
        with engine.begin() as conn:
            for table_name in table_names:
                conn.execute(text(f"CREATE TABLE {table_name} (id INTEGER PRIMARY KEY)"))
    finally:
        engine.dispose()


class _FakeMigrationContext:
    def __init__(self, db_heads: set[str], db_heads_exception: Exception | None = None) -> None:
        self.db_heads = db_heads
        self.db_heads_exception = db_heads_exception

    def get_current_heads(self) -> set[str]:
        if self.db_heads_exception:
            raise self.db_heads_exception
        return self.db_heads


class _FakeAlembicScript:
    def __init__(self, source_heads: set[str]) -> None:
        self.source_heads = source_heads

    def get_heads(self) -> set[str]:
        return self.source_heads


class _FakeAlembicEnvironment:
    def __init__(
        self, *, db_heads: set[str], source_heads: set[str], db_heads_exception: Exception | None = None
    ) -> None:
        self.script = _FakeAlembicScript(source_heads)
        self._context = _FakeMigrationContext(db_heads, db_heads_exception)

    def get_context(self) -> _FakeMigrationContext:
        return self._context


def _install_fake_airflow_db_module(
    monkeypatch,
    *,
    db_heads: set[str],
    source_heads: set[str],
    db_heads_exception: Exception | None = None,
) -> None:
    @contextmanager
    def configured_alembic_environment():
        yield _FakeAlembicEnvironment(
            db_heads=db_heads,
            source_heads=source_heads,
            db_heads_exception=db_heads_exception,
        )

    fake_airflow_module = types.ModuleType("airflow")
    fake_utils_module = types.ModuleType("airflow.utils")
    fake_db_module = types.ModuleType("airflow.utils.db")
    setattr(fake_db_module, "_configured_alembic_environment", configured_alembic_environment)
    setattr(fake_airflow_module, "utils", fake_utils_module)
    setattr(fake_utils_module, "db", fake_db_module)

    monkeypatch.setitem(sys.modules, "airflow", fake_airflow_module)
    monkeypatch.setitem(sys.modules, "airflow.utils", fake_utils_module)
    monkeypatch.setitem(sys.modules, "airflow.utils.db", fake_db_module)


class _FakeConf:
    def __init__(self, sql_alchemy_conn: str) -> None:
        self.sql_alchemy_conn = sql_alchemy_conn

    def get(self, section: str, key: str) -> str:
        return self.sql_alchemy_conn


class _FakeMetadata:
    tables = {"dag": object()}


class _FakeBase:
    metadata = _FakeMetadata()


class _FakeTable:
    def __init__(self, name: str) -> None:
        self.name = name


class _FakeSession:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


def _install_fake_airflow_metadata_modules(monkeypatch, *, sql_alchemy_conn: str) -> None:
    fake_airflow_module = types.ModuleType("airflow")
    fake_configuration_module = types.ModuleType("airflow.configuration")
    fake_models_module = types.ModuleType("airflow.models")
    fake_base_module = types.ModuleType("airflow.models.base")

    setattr(fake_configuration_module, "conf", _FakeConf(sql_alchemy_conn))
    setattr(fake_models_module, "import_all_models", lambda: None)
    setattr(fake_base_module, "Base", _FakeBase)
    setattr(fake_airflow_module, "configuration", fake_configuration_module)
    setattr(fake_airflow_module, "models", fake_models_module)
    setattr(fake_models_module, "base", fake_base_module)

    monkeypatch.setitem(sys.modules, "airflow", fake_airflow_module)
    monkeypatch.setitem(sys.modules, "airflow.configuration", fake_configuration_module)
    monkeypatch.setitem(sys.modules, "airflow.models", fake_models_module)
    monkeypatch.setitem(sys.modules, "airflow.models.base", fake_base_module)


def _install_fake_external_db_manager_modules(
    monkeypatch,
    *,
    managers: list[Any],
    migration_check_result: bool,
    session: _FakeSession,
    migration_check_exception: Exception | None = None,
) -> None:
    class FakeRunDBManager:
        def get_required_table_names(self) -> set[str]:
            required_tables: set[str] = set()
            for manager in managers:
                required_tables.update(table.name for table in manager.metadata.tables.values())
                required_tables.add(manager.version_table_name)
            return required_tables

        def check_migration(self, received_session) -> bool:
            assert received_session is session
            if migration_check_exception:
                raise migration_check_exception
            return migration_check_result

    fake_airflow_module = types.ModuleType("airflow")
    fake_settings_module = types.ModuleType("airflow.settings")
    fake_utils_module = types.ModuleType("airflow.utils")
    fake_db_manager_module = types.ModuleType("airflow.utils.db_manager")

    setattr(fake_settings_module, "Session", lambda: session)
    setattr(fake_db_manager_module, "RunDBManager", FakeRunDBManager)
    setattr(fake_airflow_module, "settings", fake_settings_module)
    setattr(fake_airflow_module, "utils", fake_utils_module)
    setattr(fake_utils_module, "db_manager", fake_db_manager_module)

    monkeypatch.setitem(sys.modules, "airflow", fake_airflow_module)
    monkeypatch.setitem(sys.modules, "airflow.settings", fake_settings_module)
    monkeypatch.setitem(sys.modules, "airflow.utils", fake_utils_module)
    monkeypatch.setitem(sys.modules, "airflow.utils.db_manager", fake_db_manager_module)


class TestSqliteDbHealthCheck:
    def test_non_sqlite_db_is_not_checked(self):
        assert (
            pytest_plugin._get_sqlite_db_health_failure_reason(
                "postgresql://localhost/airflow",
                required_tables={"dag"},
            )
            is None
        )

    def test_missing_sqlite_db_file_fails_health_check(self, tmp_path):
        missing_db_url = f"sqlite:///{tmp_path / 'missing.db'}"

        reason = pytest_plugin._get_sqlite_db_health_failure_reason(
            missing_db_url,
            required_tables={"dag"},
        )

        assert reason == f"SQLite test DB file `{tmp_path / 'missing.db'}` does not exist."

    def test_in_memory_sqlite_db_fails_health_check(self):
        reason = pytest_plugin._get_sqlite_db_health_failure_reason(
            "sqlite:///:memory:",
            required_tables={"dag"},
        )

        assert (
            reason == "SQLite test DB uses an in-memory URL, so it must be re-initialized for this test run."
        )

    def test_missing_required_tables_fail_health_check(self, tmp_path):
        sqlite_db_url = f"sqlite:///{tmp_path / 'airflow.db'}"
        _create_sqlite_db(sqlite_db_url, {"alembic_version"})

        reason = pytest_plugin._get_sqlite_db_health_failure_reason(
            sqlite_db_url,
            required_tables={"alembic_version", "dag", "dag_run"},
        )

        assert reason == "SQLite test DB schema is missing expected tables: dag, dag_run."

    def test_existing_required_tables_pass_health_check(self, tmp_path):
        sqlite_db_url = f"sqlite:///{tmp_path / 'airflow.db'}"
        _create_sqlite_db(sqlite_db_url, {"alembic_version", "dag", "dag_run"})

        assert (
            pytest_plugin._get_sqlite_db_health_failure_reason(
                sqlite_db_url,
                required_tables={"alembic_version", "dag", "dag_run"},
            )
            is None
        )


class TestAirflowDbHealthCheck:
    def test_sqlite_db_health_check_includes_core_migration_health(self, monkeypatch):
        _install_fake_airflow_metadata_modules(monkeypatch, sql_alchemy_conn="sqlite:////tmp/airflow.db")

        def get_sqlite_db_health_failure_reason(
            sql_alchemy_conn: str, *, required_tables: set[str]
        ) -> str | None:
            assert sql_alchemy_conn == "sqlite:////tmp/airflow.db"
            assert required_tables == {"alembic_version", "dag"}
            return None

        monkeypatch.setattr(
            pytest_plugin,
            "_get_sqlite_db_health_failure_reason",
            get_sqlite_db_health_failure_reason,
        )
        monkeypatch.setattr(
            pytest_plugin,
            "_get_core_migration_health_failure_reason",
            lambda: "SQLite test DB migration revision does not match.",
        )

        assert (
            pytest_plugin._get_airflow_db_health_failure_reason()
            == "SQLite test DB migration revision does not match."
        )

    def test_sqlite_db_health_check_includes_external_db_manager_health(self, monkeypatch):
        _install_fake_airflow_metadata_modules(monkeypatch, sql_alchemy_conn="sqlite:////tmp/airflow.db")
        monkeypatch.setattr(pytest_plugin, "_get_sqlite_db_health_failure_reason", lambda *_, **__: None)
        monkeypatch.setattr(pytest_plugin, "_get_core_migration_health_failure_reason", lambda: None)
        monkeypatch.setattr(
            pytest_plugin,
            "_get_external_db_manager_health_failure_reason",
            lambda sql_alchemy_conn: "SQLite test DB external DB manager migrations do not match.",
        )

        assert (
            pytest_plugin._get_airflow_db_health_failure_reason()
            == "SQLite test DB external DB manager migrations do not match."
        )


class TestCoreMigrationHealthCheck:
    def test_matching_core_migration_heads_pass_health_check(self, monkeypatch):
        _install_fake_airflow_db_module(
            monkeypatch, db_heads={"current_revision"}, source_heads={"current_revision"}
        )

        assert pytest_plugin._get_core_migration_health_failure_reason() is None

    def test_mismatched_core_migration_heads_fail_health_check(self, monkeypatch):
        _install_fake_airflow_db_module(
            monkeypatch, db_heads={"old_revision"}, source_heads={"current_revision"}
        )

        reason = pytest_plugin._get_core_migration_health_failure_reason()

        assert (
            reason == "SQLite test DB migration revision does not match the current Airflow migration head. "
            "DB heads: old_revision; source heads: current_revision."
        )

    def test_core_migration_inspection_error_fails_health_check(self, monkeypatch):
        _install_fake_airflow_db_module(
            monkeypatch,
            db_heads=set(),
            source_heads={"current_revision"},
            db_heads_exception=SQLAlchemyError("malformed alembic_version"),
        )

        assert (
            pytest_plugin._get_core_migration_health_failure_reason()
            == "Unable to inspect SQLite test DB core migration state: malformed alembic_version"
        )


class TestExternalDbManagerHealthCheck:
    def test_missing_external_db_manager_table_fails_health_check(self, monkeypatch):
        class FakeExternalDbManager:
            metadata = types.SimpleNamespace(
                tables={"external_table": _FakeTable("external_table")},
            )
            version_table_name = "alembic_version_external"

        _install_fake_external_db_manager_modules(
            monkeypatch,
            managers=[FakeExternalDbManager],
            migration_check_result=True,
            session=_FakeSession(),
        )

        def get_sqlite_db_health_failure_reason(
            sql_alchemy_conn: str, *, required_tables: set[str]
        ) -> str | None:
            assert sql_alchemy_conn == "sqlite:////tmp/airflow.db"
            assert required_tables == {"alembic_version_external", "external_table"}
            return "SQLite test DB schema is missing expected tables: external_table."

        monkeypatch.setattr(
            pytest_plugin,
            "_get_sqlite_db_health_failure_reason",
            get_sqlite_db_health_failure_reason,
        )

        assert (
            pytest_plugin._get_external_db_manager_health_failure_reason("sqlite:////tmp/airflow.db")
            == "SQLite test DB schema is missing expected tables: external_table."
        )

    def test_external_db_manager_migration_mismatch_fails_health_check(self, monkeypatch):
        class FakeExternalDbManager:
            metadata = types.SimpleNamespace(
                tables={"external_table": _FakeTable("external_table")},
            )
            version_table_name = "alembic_version_external"

        session = _FakeSession()
        _install_fake_external_db_manager_modules(
            monkeypatch,
            managers=[FakeExternalDbManager],
            migration_check_result=False,
            session=session,
        )
        monkeypatch.setattr(pytest_plugin, "_get_sqlite_db_health_failure_reason", lambda *_, **__: None)

        assert (
            pytest_plugin._get_external_db_manager_health_failure_reason("sqlite:////tmp/airflow.db")
            == "SQLite test DB external DB manager migrations do not match their current migration heads."
        )
        assert session.closed is True

    def test_external_db_manager_migration_inspection_error_fails_health_check(self, monkeypatch):
        class FakeExternalDbManager:
            metadata = types.SimpleNamespace(
                tables={"external_table": _FakeTable("external_table")},
            )
            version_table_name = "alembic_version_external"

        session = _FakeSession()
        _install_fake_external_db_manager_modules(
            monkeypatch,
            managers=[FakeExternalDbManager],
            migration_check_result=True,
            session=session,
            migration_check_exception=SQLAlchemyError("malformed external version table"),
        )
        monkeypatch.setattr(pytest_plugin, "_get_sqlite_db_health_failure_reason", lambda *_, **__: None)

        assert (
            pytest_plugin._get_external_db_manager_health_failure_reason("sqlite:////tmp/airflow.db")
            == "Unable to inspect SQLite test DB external DB manager migration state: "
            "malformed external version table"
        )
        assert session.closed is True


class TestInitializeAirflowDb:
    def test_skips_initialization_when_marker_exists_and_db_is_healthy(self, monkeypatch, tmp_path):
        called = False

        def initial_db_init():
            nonlocal called
            called = True

        fake_db_module = types.ModuleType("tests_common.test_utils.db")
        setattr(fake_db_module, "initial_db_init", initial_db_init)
        monkeypatch.setitem(sys.modules, "tests_common.test_utils.db", fake_db_module)
        monkeypatch.setattr(pytest_plugin, "_get_airflow_db_health_failure_reason", lambda: None)
        tmp_path.joinpath(".airflow_db_initialised").touch()

        pytest_plugin._initialize_airflow_db(force_db_init=False, airflow_home=tmp_path)

        assert called is False

    def test_initializes_when_marker_exists_but_db_health_check_fails(self, monkeypatch, tmp_path):
        called = False

        def initial_db_init():
            nonlocal called
            called = True

        fake_db_module = types.ModuleType("tests_common.test_utils.db")
        setattr(fake_db_module, "initial_db_init", initial_db_init)
        monkeypatch.setitem(sys.modules, "tests_common.test_utils.db", fake_db_module)
        monkeypatch.setattr(
            pytest_plugin,
            "_get_airflow_db_health_failure_reason",
            lambda: "SQLite test DB file `/tmp/airflow.db` does not exist.",
        )
        tmp_path.joinpath(".airflow_db_initialised").touch()

        pytest_plugin._initialize_airflow_db(force_db_init=False, airflow_home=tmp_path)

        assert called is True

    def test_sqlite_initialization_failure_includes_db_file_removal_guidance(self, monkeypatch, tmp_path):
        sqlite_db_file = tmp_path / "airflow.db"

        def initial_db_init():
            raise SQLAlchemyError("reset failed")

        fake_db_module = types.ModuleType("tests_common.test_utils.db")
        setattr(fake_db_module, "initial_db_init", initial_db_init)
        monkeypatch.setitem(sys.modules, "tests_common.test_utils.db", fake_db_module)
        monkeypatch.setattr(
            pytest_plugin,
            "_get_airflow_db_health_failure_reason",
            lambda: "SQLite test DB migration revision does not match.",
        )
        _install_fake_airflow_metadata_modules(
            monkeypatch,
            sql_alchemy_conn=f"sqlite:///{sqlite_db_file}",
        )
        tmp_path.joinpath(".airflow_db_initialised").touch()

        with pytest.raises(RuntimeError) as exc_info:
            pytest_plugin._initialize_airflow_db(force_db_init=False, airflow_home=tmp_path)

        assert str(exc_info.value) == (
            f"Unable to re-initialize the SQLite test DB. Remove `{sqlite_db_file}` and rerun pytest."
        )
        assert isinstance(exc_info.value.__cause__, SQLAlchemyError)
