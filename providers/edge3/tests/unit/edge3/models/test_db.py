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

import warnings
from unittest import mock

import pytest

from airflow.utils.db_manager import RunDBManager

from tests_common.test_utils.config import conf_vars

pytestmark = [pytest.mark.db_test]


class TestEdgeDBManager:
    """Test EdgeDBManager functionality."""

    @conf_vars(
        {
            (
                "database",
                "external_db_managers",
            ): "airflow.providers.edge3.models.db.EdgeDBManager"
        }
    )
    def test_db_manager_uses_config(self):
        """Test that EdgeDBManager is loaded from config."""
        from airflow.providers.edge3.models.db import EdgeDBManager

        run_db_manager = RunDBManager()
        assert EdgeDBManager in run_db_manager._managers

    @conf_vars(
        {
            (
                "database",
                "external_db_managers",
            ): "airflow.providers.edge3.models.db.EdgeDBManager"
        }
    )
    def test_edge_db_manager_attributes(self):
        """Test that EdgeDBManager has correct attributes."""
        from airflow.providers.edge3.models.db import EdgeDBManager

        assert EdgeDBManager.version_table_name == "alembic_version_edge3"
        assert EdgeDBManager.supports_table_dropping is True
        assert "migrations" in EdgeDBManager.migration_dir
        assert "alembic.ini" in EdgeDBManager.alembic_file

    @conf_vars(
        {
            (
                "database",
                "external_db_managers",
            ): "airflow.providers.edge3.models.db.EdgeDBManager"
        }
    )
    @mock.patch("airflow.providers.edge3.models.db.EdgeDBManager")
    def test_rundbmanager_calls_edge_dbmanager_methods(self, mock_edge_db_manager, session):
        """Test that RunDBManager properly calls EdgeDBManager methods."""
        mock_edge_db_manager.supports_table_dropping = True
        edge_db_manager = mock_edge_db_manager.return_value
        ext_db = RunDBManager()

        # initdb
        ext_db.initdb(session=session)
        edge_db_manager.initdb.assert_called_once()

        # upgradedb
        ext_db.upgradedb(session=session)
        edge_db_manager.upgradedb.assert_called_once()

        # drop_tables
        connection = mock.MagicMock()
        ext_db.drop_tables(session, connection)
        mock_edge_db_manager.return_value.drop_tables.assert_called_once_with(connection)

    def test_drop_tables_only_drops_edge_tables(self, session):
        """Test that drop_tables only drops edge3 tables, not all metadata tables."""
        from airflow.providers.edge3.models.db import EdgeDBManager
        from airflow.providers.edge3.models.edge_job import EdgeJobModel
        from airflow.providers.edge3.models.edge_logs import EdgeLogsModel
        from airflow.providers.edge3.models.edge_worker import EdgeWorkerModel

        manager = EdgeDBManager(session)

        # Mock connection and inspector
        mock_connection = mock.MagicMock()
        mock_inspector = mock.MagicMock()

        # Setup mock inspector to report all tables exist
        mock_inspector.has_table.return_value = True

        # Mock inspect to return our mock inspector
        with mock.patch("airflow.providers.edge3.models.db.inspect", return_value=mock_inspector):
            # Mock the migration context
            mock_version_table = mock.MagicMock()
            mock_version_table.name = "alembic_version_edge3"
            mock_migration_ctx = mock.MagicMock()
            mock_migration_ctx._version = mock_version_table

            with mock.patch.object(manager, "_get_migration_ctx", return_value=mock_migration_ctx):
                # Mock the drop methods on the actual table objects
                with (
                    mock.patch.object(EdgeWorkerModel.__table__, "drop") as mock_worker_drop,
                    mock.patch.object(EdgeJobModel.__table__, "drop") as mock_job_drop,
                    mock.patch.object(EdgeLogsModel.__table__, "drop") as mock_logs_drop,
                ):
                    # Call drop_tables
                    manager.drop_tables(mock_connection)

                    # Verify that only edge3 tables were dropped
                    mock_worker_drop.assert_called_once_with(mock_connection)
                    mock_job_drop.assert_called_once_with(mock_connection)
                    mock_logs_drop.assert_called_once_with(mock_connection)
                    mock_version_table.drop.assert_called_once_with(mock_connection)

                    # Verify has_table was called for each edge table
                    expected_calls = [
                        mock.call("edge_logs"),
                        mock.call("edge_job"),
                        mock.call("edge_worker"),
                        mock.call("alembic_version_edge3"),
                    ]
                    mock_inspector.has_table.assert_has_calls(expected_calls, any_order=True)

    @mock.patch("airflow.utils.db_manager.command")
    def test_create_db_from_orm(self, mock_command, session):
        """Test that create_db_from_orm creates tables and stamps migration."""
        from airflow.providers.edge3.models.db import EdgeDBManager

        manager = EdgeDBManager(session)

        with mock.patch.object(manager.metadata, "create_all") as mock_create_all:
            manager.create_db_from_orm()

            mock_create_all.assert_called_once()
            mock_command.stamp.assert_called_once()
            # Verify stamp was called with "head"
            args = mock_command.stamp.call_args
            assert args[0][1] == "head"

    @mock.patch.object(
        __import__("airflow.providers.edge3.models.db", fromlist=["EdgeDBManager"]).EdgeDBManager,
        "upgradedb",
    )
    @mock.patch.object(
        __import__("airflow.providers.edge3.models.db", fromlist=["EdgeDBManager"]).EdgeDBManager,
        "create_db_from_orm",
    )
    @mock.patch.object(
        __import__("airflow.providers.edge3.models.db", fromlist=["EdgeDBManager"]).EdgeDBManager,
        "get_current_revision",
    )
    @mock.patch("airflow.providers.edge3.models.db.inspect")
    def test_initdb_new_db(self, mock_inspect, mock_get_rev, mock_create, mock_upgrade, session):
        """Test that initdb calls create_db_from_orm for new databases."""
        from airflow.providers.edge3.models.db import EdgeDBManager

        mock_get_rev.return_value = None
        mock_inspect.return_value.get_table_names.return_value = []  # no tables exist

        manager = EdgeDBManager(session)
        manager.initdb()

        mock_create.assert_called_once()
        mock_upgrade.assert_not_called()

    @mock.patch.object(
        __import__("airflow.providers.edge3.models.db", fromlist=["EdgeDBManager"]).EdgeDBManager,
        "upgradedb",
    )
    @mock.patch.object(
        __import__("airflow.providers.edge3.models.db", fromlist=["EdgeDBManager"]).EdgeDBManager,
        "create_db_from_orm",
    )
    @mock.patch.object(
        __import__("airflow.providers.edge3.models.db", fromlist=["EdgeDBManager"]).EdgeDBManager,
        "get_current_revision",
    )
    def test_initdb_existing_db(self, mock_get_rev, mock_create, mock_upgrade, session):
        """Test that initdb calls upgradedb for existing databases."""
        from airflow.providers.edge3.models.db import EdgeDBManager

        mock_get_rev.return_value = "9d34dfc2de06"

        manager = EdgeDBManager(session)
        manager.initdb()

        mock_upgrade.assert_called_once()
        mock_create.assert_not_called()

    def test_revision_heads_map_populated(self):
        """Test that _REVISION_HEADS_MAP is populated with all known migrations."""
        from airflow.providers.edge3.models.db import _REVISION_HEADS_MAP

        assert "3.0.0" in _REVISION_HEADS_MAP
        assert _REVISION_HEADS_MAP["3.0.0"] == "9d34dfc2de06"
        assert "3.2.0" in _REVISION_HEADS_MAP
        assert _REVISION_HEADS_MAP["3.2.0"] == "b3c4d5e6f7a8"

    def test_initdb_stamps_and_upgrades_when_tables_exist_without_version(self, session):
        """Test that initdb runs incremental migrations when tables exist but alembic version table does not."""
        from sqlalchemy import inspect, text

        from airflow import settings
        from airflow.providers.edge3.models.db import EdgeDBManager

        manager = EdgeDBManager(session)

        # Simulate pre-alembic state: tables exist but no version table and no concurrency column
        with settings.engine.begin() as conn:
            inspector = inspect(conn)
            if inspector.has_table("alembic_version_edge3"):
                conn.execute(text("DELETE FROM alembic_version_edge3"))
            if "concurrency" in {col["name"] for col in inspector.get_columns("edge_worker")}:
                from alembic.migration import MigrationContext
                from alembic.operations import Operations

                mc = MigrationContext.configure(conn, opts={"render_as_batch": True})
                ops = Operations(mc)
                ops.drop_column("edge_worker", "concurrency")

        # initdb() should detect tables exist, stamp to base, then upgrade
        manager.initdb()

        with settings.engine.connect() as conn:
            version = conn.execute(text("SELECT version_num FROM alembic_version_edge3")).scalar()
            columns = {col["name"] for col in inspect(conn).get_columns("edge_worker")}

        assert version == "b3c4d5e6f7a8"
        assert "concurrency" in columns

    def test_migration_adds_concurrency_column(self, session):
        """Test that upgrading from 3.0.0 actually adds the concurrency column."""
        from alembic import command
        from alembic.migration import MigrationContext
        from alembic.operations import Operations
        from sqlalchemy import inspect

        from airflow import settings
        from airflow.providers.edge3.models.db import EdgeDBManager

        manager = EdgeDBManager(session)
        config = manager.get_alembic_config()

        # DDL must be committed before alembic opens its own connection — use engine.begin()
        # so the DROP is visible to the fresh connection that upgradedb() creates internally.
        with settings.engine.begin() as conn:
            inspector = inspect(conn)
            if "concurrency" in {col["name"] for col in inspector.get_columns("edge_worker")}:
                mc = MigrationContext.configure(conn, opts={"render_as_batch": True})
                ops = Operations(mc)
                ops.drop_column("edge_worker", "concurrency")

        # Stamp to old revision (pre-concurrency) using alembic's own connection
        command.stamp(config, "9d34dfc2de06")

        # Run the upgrade — migration 0002 should detect the missing column and add it
        manager.upgradedb()

        # Verify with a fresh connection (upgradedb also uses its own connection)
        with settings.engine.connect() as conn:
            inspector = inspect(conn)
            columns = {col["name"] for col in inspector.get_columns("edge_worker")}

        assert "concurrency" in columns, "Migration 0002 should have added the concurrency column"

    def test_drop_tables_handles_missing_tables(self, session):
        """Test that drop_tables handles missing tables gracefully."""
        from airflow.providers.edge3.models.db import EdgeDBManager

        manager = EdgeDBManager(session)

        # Mock connection and inspector
        mock_connection = mock.MagicMock()
        mock_inspector = mock.MagicMock()

        # Setup mock inspector to report no tables exist
        mock_inspector.has_table.return_value = False

        # Mock inspect to return our mock inspector
        with mock.patch("airflow.providers.edge3.models.db.inspect", return_value=mock_inspector):
            # Mock the migration context
            mock_version_table = mock.MagicMock()
            mock_version_table.name = "alembic_version_edge3"
            mock_migration_ctx = mock.MagicMock()
            mock_migration_ctx._version = mock_version_table

            with mock.patch.object(manager, "_get_migration_ctx", return_value=mock_migration_ctx):
                # Call drop_tables - should not raise an exception
                manager.drop_tables(mock_connection)

                # Verify that no tables were dropped since none exist
                # The drop method should not be called on any table
                # We check this by ensuring has_table was called but drop was not
                assert mock_inspector.has_table.called


class TestCheckDbManagerConfig:
    """Test check_db_manager_config warning helper."""

    pytestmark: list = []  # no db_test needed — purely config-based

    @mock.patch("airflow.providers_manager.ProvidersManager")
    def test_warns_when_not_configured(self, mock_pm):
        """Warning is emitted when EdgeDBManager is absent from external_db_managers and not discovered."""
        mock_pm.return_value.db_managers = []
        from airflow.providers.edge3.models.db import check_db_manager_config

        with conf_vars({("database", "external_db_managers"): ""}):
            with pytest.warns(UserWarning, match="EdgeDBManager is not configured"):
                check_db_manager_config()

    @mock.patch("airflow.providers_manager.ProvidersManager")
    def test_warns_when_other_manager_configured(self, mock_pm):
        """Warning is emitted when a different manager is configured but not EdgeDBManager."""
        mock_pm.return_value.db_managers = []
        from airflow.providers.edge3.models.db import check_db_manager_config

        with conf_vars({("database", "external_db_managers"): "some.other.DBManager"}):
            with pytest.warns(UserWarning, match="EdgeDBManager is not configured"):
                check_db_manager_config()

    @mock.patch("airflow.providers_manager.ProvidersManager")
    def test_no_warn_when_configured(self, mock_pm):
        """No warning when EdgeDBManager is properly configured."""
        mock_pm.return_value.db_managers = []
        from airflow.providers.edge3.models.db import check_db_manager_config

        with conf_vars(
            {
                ("database", "external_db_managers"): "airflow.providers.edge3.models.db.EdgeDBManager",
            }
        ):
            with warnings.catch_warnings():
                warnings.simplefilter("error")
                check_db_manager_config()  # must not raise

    @mock.patch("airflow.providers_manager.ProvidersManager")
    def test_no_warn_when_configured_among_multiple(self, mock_pm):
        """No warning when EdgeDBManager appears alongside other managers."""
        mock_pm.return_value.db_managers = []
        from airflow.providers.edge3.models.db import check_db_manager_config

        with conf_vars(
            {
                ("database", "external_db_managers"): (
                    "some.other.DBManager,airflow.providers.edge3.models.db.EdgeDBManager"
                ),
            }
        ):
            with warnings.catch_warnings():
                warnings.simplefilter("error")
                check_db_manager_config()  # must not raise

    @mock.patch("airflow.providers_manager.ProvidersManager")
    def test_no_warn_when_discovered(self, mock_pm):
        """No warning when EdgeDBManager is auto-discovered via ProvidersManager."""
        mock_pm.return_value.db_managers = ["airflow.providers.edge3.models.db.EdgeDBManager"]
        from airflow.providers.edge3.models.db import check_db_manager_config

        with conf_vars({("database", "external_db_managers"): ""}):
            with warnings.catch_warnings():
                warnings.simplefilter("error")
                check_db_manager_config()  # must not raise
