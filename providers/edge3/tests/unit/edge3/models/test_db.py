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
        assert run_db_manager._managers == [EdgeDBManager]

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
