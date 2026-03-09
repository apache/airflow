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
Tests for migration 0082 – make bundle_name not nullable.

The key goal is to verify that upgrade() respects the user's configured
bundles (via DagBundlesManager) rather than hardcoding 'dags-folder' /
'example_dags', and that it picks the *first* configured bundle as the
fallback for DAG rows whose bundle_name is NULL.
"""

from __future__ import annotations

import contextlib
import importlib
from unittest.mock import MagicMock, patch

import pytest

# The module name starts with a digit so we must use importlib.
_migration = importlib.import_module("airflow.migrations.versions.0082_3_1_0_make_bundle_name_not_nullable")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_op(dialect_name: str) -> tuple[MagicMock, MagicMock, MagicMock]:
    """
    Build a minimal mock of alembic's *op* object.

    Returns (mock_op, mock_conn, mock_batch_op) where:
    - mock_op   replaces ``alembic.op`` inside the migration module
    - mock_conn replaces the live DB connection returned by ``op.get_bind()``
    - mock_batch_op replaces the batch-alter context manager yield value
    """
    mock_op = MagicMock()

    mock_conn = MagicMock()
    mock_conn.dialect.name = dialect_name
    mock_op.get_bind.return_value = mock_conn

    mock_batch_op = MagicMock()
    # batch_alter_table is used as a context manager three times in upgrade().
    # Each __enter__ returns the same mock_batch_op so we can assert on it.
    ctx_mgr = MagicMock()
    ctx_mgr.__enter__ = MagicMock(return_value=mock_batch_op)
    ctx_mgr.__exit__ = MagicMock(return_value=False)
    mock_op.batch_alter_table.return_value = ctx_mgr

    return mock_op, mock_conn, mock_batch_op


def _executed_sql_strings(mock_conn: MagicMock) -> list[str]:
    """Return the string representation of every SQL statement executed."""
    return [str(c.args[0]) for c in mock_conn.execute.call_args_list]


def _executed_params(mock_conn: MagicMock) -> list[dict]:
    """Return the parameter dicts passed to each execute() call."""
    return [c.args[1] if len(c.args) > 1 else {} for c in mock_conn.execute.call_args_list]


def _run_upgrade(mock_op: MagicMock, bundle_names: list[str]) -> None:
    """Patch the migration's dependencies and run upgrade()."""
    mock_manager = MagicMock()
    mock_manager.bundle_names = bundle_names

    with (
        patch.object(_migration, "op", mock_op),
        # StringID is a lazy proxy that resolves via alembic.context.get_bind();
        # replace with a plain MagicMock so tests don't need a live Alembic context.
        patch.object(_migration, "StringID", MagicMock()),
        # ignore_sqlite_value_error uses 'from alembic import op' internally;
        # replace it with a no-op context manager to keep tests dialect-agnostic
        # unless the test specifically exercises SQLite pragma behaviour.
        patch.object(_migration, "ignore_sqlite_value_error", return_value=contextlib.nullcontext()),
        patch(
            "airflow.dag_processing.bundles.manager.DagBundlesManager",
            return_value=mock_manager,
        ),
    ):
        _migration.upgrade()


# ---------------------------------------------------------------------------
# upgrade() – bundle insertion per dialect
# ---------------------------------------------------------------------------


class TestUpgradeDialects:
    """Verify correct INSERT syntax is used for every supported dialect."""

    def test_postgresql_uses_on_conflict_do_nothing(self):
        mock_op, mock_conn, _ = _make_mock_op("postgresql")
        _run_upgrade(mock_op, ["dags-folder"])

        sqls = _executed_sql_strings(mock_conn)
        insert_sqls = [s for s in sqls if "INSERT" in s.upper()]
        assert insert_sqls, "Expected at least one INSERT statement"
        assert all("ON CONFLICT" in s and "DO NOTHING" in s for s in insert_sqls)

    def test_mysql_uses_insert_ignore(self):
        mock_op, mock_conn, _ = _make_mock_op("mysql")
        _run_upgrade(mock_op, ["dags-folder"])

        sqls = _executed_sql_strings(mock_conn)
        insert_sqls = [s for s in sqls if "INSERT" in s.upper()]
        assert insert_sqls, "Expected at least one INSERT statement"
        assert all("INSERT IGNORE" in s for s in insert_sqls)

    def test_sqlite_uses_insert_or_ignore(self):
        mock_op, mock_conn, _ = _make_mock_op("sqlite")
        _run_upgrade(mock_op, ["dags-folder"])

        sqls = _executed_sql_strings(mock_conn)
        insert_sqls = [s for s in sqls if "INSERT" in s.upper()]
        assert insert_sqls, "Expected at least one INSERT statement"
        assert all("INSERT OR IGNORE" in s for s in insert_sqls)

    @pytest.mark.parametrize("dialect", ["postgresql", "mysql", "sqlite"])
    def test_bundle_name_passed_as_parameter(self, dialect):
        """Ensure bundle names are passed as bound parameters, not interpolated."""
        mock_op, mock_conn, _ = _make_mock_op(dialect)
        _run_upgrade(mock_op, ["my-bundle"])

        params_list = _executed_params(mock_conn)
        insert_params = [p for p in params_list if p.get("name") is not None]
        assert insert_params, "Expected execute() to be called with {'name': ...} for INSERT"
        assert any(p["name"] == "my-bundle" for p in insert_params)


# ---------------------------------------------------------------------------
# upgrade() – user config bundles are respected
# ---------------------------------------------------------------------------


class TestUpgradeUserConfigBundles:
    """The migration must use DagBundlesManager, not hardcoded bundle names."""

    def test_default_bundle_is_inserted(self):
        mock_op, mock_conn, _ = _make_mock_op("postgresql")
        _run_upgrade(mock_op, ["dags-folder"])

        params_list = _executed_params(mock_conn)
        inserted_names = {p["name"] for p in params_list if "name" in p}
        assert "dags-folder" in inserted_names

    def test_custom_bundle_is_inserted_instead_of_hardcoded(self):
        """When the user configured a custom bundle, *that* bundle is inserted."""
        mock_op, mock_conn, _ = _make_mock_op("postgresql")
        _run_upgrade(mock_op, ["my-custom-bundle"])

        params_list = _executed_params(mock_conn)
        inserted_names = {p["name"] for p in params_list if "name" in p}
        assert "my-custom-bundle" in inserted_names
        # hardcoded names must NOT appear unless user actually configured them
        assert "dags-folder" not in inserted_names
        assert "example_dags" not in inserted_names

    def test_all_user_bundles_are_inserted(self):
        """Every bundle returned by DagBundlesManager must be inserted."""
        mock_op, mock_conn, _ = _make_mock_op("postgresql")
        user_bundles = ["bundle-a", "bundle-b", "bundle-c"]
        _run_upgrade(mock_op, user_bundles)

        params_list = _executed_params(mock_conn)
        inserted_names = {p["name"] for p in params_list if "name" in p}
        assert inserted_names == set(user_bundles)

    def test_three_bundles_all_inserted_and_first_is_default(self):
        """With three configured bundles, all three are added to dag_bundle and NULL
        DAG rows are assigned the first one — not hardcoded 'dags-folder'."""
        mock_op, mock_conn, _ = _make_mock_op("postgresql")
        user_bundles = ["primary-bundle", "secondary-bundle", "tertiary-bundle"]
        _run_upgrade(mock_op, user_bundles)

        sqls = _executed_sql_strings(mock_conn)
        params_list = _executed_params(mock_conn)

        # All three bundles must be inserted into dag_bundle
        inserted_names = {p["name"] for p in params_list if "name" in p}
        assert inserted_names == set(user_bundles), (
            f"Expected exactly {set(user_bundles)} inserted, got {inserted_names}"
        )

        # The UPDATE for NULL dag rows must use the first bundle, not any other
        update_indices = [i for i, s in enumerate(sqls) if "UPDATE dag" in s]
        assert update_indices, "Expected an UPDATE dag statement for NULL bundle_name rows"
        default_used = params_list[update_indices[0]]["default_bundle"]
        assert default_used == "primary-bundle", (
            f"Expected first bundle 'primary-bundle' as default, got '{default_used}'"
        )


# ---------------------------------------------------------------------------
# upgrade() – NULL DAG rows get the *first* configured bundle
# ---------------------------------------------------------------------------


class TestUpgradeNullDagUpdate:
    """DAGs with NULL bundle_name are updated to the first user-configured bundle."""

    def test_null_dags_updated_to_first_bundle(self):
        mock_op, mock_conn, _ = _make_mock_op("postgresql")
        _run_upgrade(mock_op, ["first-bundle", "second-bundle"])

        sqls = _executed_sql_strings(mock_conn)
        params_list = _executed_params(mock_conn)

        update_indices = [i for i, s in enumerate(sqls) if "UPDATE dag" in s]
        assert update_indices, "Expected an UPDATE dag statement"

        update_params = params_list[update_indices[0]]
        assert update_params.get("default_bundle") == "first-bundle"

    def test_single_bundle_used_as_default(self):
        mock_op, mock_conn, _ = _make_mock_op("postgresql")
        _run_upgrade(mock_op, ["only-bundle"])

        sqls = _executed_sql_strings(mock_conn)
        params_list = _executed_params(mock_conn)
        update_indices = [i for i, s in enumerate(sqls) if "UPDATE dag" in s]
        assert update_indices
        assert params_list[update_indices[0]]["default_bundle"] == "only-bundle"

    def test_custom_bundle_not_dags_folder_used_as_default(self):
        """When user replaces dags-folder with their own bundle, that bundle is the default."""
        mock_op, mock_conn, _ = _make_mock_op("postgresql")
        _run_upgrade(mock_op, ["corp-dags", "team-dags"])

        sqls = _executed_sql_strings(mock_conn)
        params_list = _executed_params(mock_conn)
        update_indices = [i for i, s in enumerate(sqls) if "UPDATE dag" in s]
        assert update_indices
        default = params_list[update_indices[0]]["default_bundle"]
        assert default == "corp-dags"
        assert default != "dags-folder"


# ---------------------------------------------------------------------------
# upgrade() – schema alterations (column + FK)
# ---------------------------------------------------------------------------


class TestUpgradeSchemaAlterations:
    """Verify that the column nullability and FK constraints are modified correctly."""

    def test_bundle_name_column_made_not_null(self):
        mock_op, mock_conn, mock_batch_op = _make_mock_op("postgresql")
        _run_upgrade(mock_op, ["dags-folder"])

        alter_calls = mock_batch_op.alter_column.call_args_list
        dag_bundle_name_alters = [c for c in alter_calls if c.args and c.args[0] == "bundle_name"]
        assert dag_bundle_name_alters, "alter_column('bundle_name', ...) was not called"
        assert any(c.kwargs.get("nullable") is False for c in dag_bundle_name_alters)

    def test_fk_constraint_dropped_and_recreated(self):
        mock_op, mock_conn, mock_batch_op = _make_mock_op("postgresql")
        _run_upgrade(mock_op, ["dags-folder"])

        assert mock_batch_op.drop_constraint.called, "FK constraint should be dropped"
        assert mock_batch_op.create_foreign_key.called, "FK constraint should be recreated"

    def test_batch_alter_called_for_dag_table(self):
        mock_op, mock_conn, mock_batch_op = _make_mock_op("postgresql")
        _run_upgrade(mock_op, ["dags-folder"])

        table_names = [c.args[0] for c in mock_op.batch_alter_table.call_args_list]
        assert "dag" in table_names
        assert "dag_bundle" in table_names


# ---------------------------------------------------------------------------
# upgrade() – SQLite-specific PRAGMA handling
# ---------------------------------------------------------------------------


class TestUpgradeSQLitePragma:
    """SQLite requires FK constraints to be disabled during batch operations."""

    def _run_sqlite_upgrade(self, bundle_names: list[str]) -> MagicMock:
        """Run upgrade() with SQLite dialect, returning the mock connection."""
        mock_op, mock_conn, _ = _make_mock_op("sqlite")
        mock_manager = MagicMock()
        mock_manager.bundle_names = bundle_names

        # ignore_sqlite_value_error does 'from alembic import op' locally, so we
        # must patch alembic.op (not just the migration module's op reference).
        with (
            patch.object(_migration, "op", mock_op),
            patch.object(_migration, "StringID", MagicMock()),
            patch("alembic.op", mock_op),
            patch(
                "airflow.dag_processing.bundles.manager.DagBundlesManager",
                return_value=mock_manager,
            ),
        ):
            _migration.upgrade()
        return mock_conn

    def test_sqlite_disables_foreign_keys(self):
        mock_conn = self._run_sqlite_upgrade(["dags-folder"])
        sqls = _executed_sql_strings(mock_conn)
        assert any("PRAGMA foreign_keys=OFF" in s for s in sqls), (
            "SQLite upgrade should disable foreign keys before batch operations"
        )

    def test_sqlite_re_enables_foreign_keys(self):
        mock_conn = self._run_sqlite_upgrade(["dags-folder"])
        sqls = _executed_sql_strings(mock_conn)
        assert any("PRAGMA foreign_keys=ON" in s for s in sqls), (
            "SQLite upgrade should re-enable foreign keys after batch operations"
        )

    def test_non_sqlite_skips_pragma(self):
        for dialect in ("postgresql", "mysql"):
            mock_op, mock_conn, _ = _make_mock_op(dialect)
            _run_upgrade(mock_op, ["dags-folder"])

            sqls = _executed_sql_strings(mock_conn)
            assert not any("PRAGMA" in s for s in sqls), (
                f"{dialect}: PRAGMA statements must not be issued for non-SQLite dialects"
            )


# ---------------------------------------------------------------------------
# downgrade()
# ---------------------------------------------------------------------------


class TestDowngrade:
    """downgrade() should reverse the nullability change on bundle_name."""

    def _run_downgrade(self, mock_op: MagicMock) -> None:
        with (
            patch.object(_migration, "op", mock_op),
            patch.object(_migration, "StringID", MagicMock()),
        ):
            _migration.downgrade()

    def test_bundle_name_made_nullable_again(self):
        mock_op, mock_conn, mock_batch_op = _make_mock_op("postgresql")
        self._run_downgrade(mock_op)

        alter_calls = mock_batch_op.alter_column.call_args_list
        bundle_name_alters = [c for c in alter_calls if c.args and c.args[0] == "bundle_name"]
        assert bundle_name_alters, "alter_column('bundle_name', ...) was not called in downgrade"
        assert any(c.kwargs.get("nullable") is True for c in bundle_name_alters)

    def test_downgrade_drops_and_recreates_fk(self):
        mock_op, mock_conn, mock_batch_op = _make_mock_op("postgresql")
        self._run_downgrade(mock_op)

        assert mock_batch_op.drop_constraint.called, "FK should be dropped in downgrade"
        assert mock_batch_op.create_foreign_key.called, "FK should be recreated in downgrade"

    def test_downgrade_sqlite_disables_foreign_keys(self):
        mock_op, mock_conn, _ = _make_mock_op("sqlite")
        self._run_downgrade(mock_op)

        sqls = _executed_sql_strings(mock_conn)
        assert any("PRAGMA foreign_keys=OFF" in s for s in sqls)
        assert any("PRAGMA foreign_keys=ON" in s for s in sqls)

    def test_downgrade_non_sqlite_skips_pragma(self):
        for dialect in ("postgresql", "mysql"):
            mock_op, mock_conn, _ = _make_mock_op(dialect)
            self._run_downgrade(mock_op)

            sqls = _executed_sql_strings(mock_conn)
            assert not any("PRAGMA" in s for s in sqls)
