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

from unittest.mock import MagicMock, patch

import pytest

from airflow.api.common.delete_dag import _cleanup_dag_permissions, delete_dag
from airflow.exceptions import DagNotFound

pytestmark = pytest.mark.db_test


class TestCleanupDagPermissions:
    """Test cases for _cleanup_dag_permissions function."""

    @patch("airflow.configuration.conf.get")
    def test_cleanup_dag_permissions_non_fab_auth_manager(self, mock_conf_get, session):
        """Test that cleanup is skipped when not using FabAuthManager."""
        mock_conf_get.return_value = "BasicAuthManager"

        # Should return early without any database operations
        _cleanup_dag_permissions("test_dag", session)

        mock_conf_get.assert_called_once_with("core", "auth_manager")

    @patch("airflow.configuration.conf.get")
    def test_cleanup_dag_permissions_fab_provider_not_available(self, mock_conf_get, session):
        """Test that cleanup is skipped when FAB provider is not available."""
        mock_conf_get.return_value = "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"

        # Mock ImportError specifically for FAB provider imports
        original_import = __builtins__["__import__"]

        def mock_import(name, *args, **kwargs):
            if "airflow.providers.fab" in name:
                raise ImportError(f"No module named '{name}'")
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            # Mock session.query to track if it's called
            with patch.object(session, "query") as mock_query:
                # Should return early when FAB models are not available
                _cleanup_dag_permissions("test_dag", session)

                # No database operations should be performed
                mock_query.assert_not_called()

    @patch("airflow.configuration.conf.get")
    @patch("airflow.api.common.delete_dag.log")
    def test_cleanup_dag_permissions_full_cleanup(self, mock_log, mock_conf_get, session):
        """Test full cleanup process when resources and permissions exist."""
        mock_conf_get.return_value = "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"

        # Mock successful FAB import
        with patch("airflow.providers.fab.auth_manager.models") as mock_models:
            mock_resource = MagicMock()
            mock_permission = MagicMock()
            mock_assoc = MagicMock()
            mock_models.Resource = mock_resource
            mock_models.Permission = mock_permission
            mock_models.assoc_permission_role = mock_assoc

            # Mock resources and permissions
            mock_dag_resource = MagicMock()
            mock_dag_resource.id = 1
            mock_dag_permission = MagicMock()
            mock_dag_permission.id = 1

            def mock_query_side_effect(model):
                mock_query = MagicMock()
                if model == mock_resource:
                    mock_query.filter.return_value.all.return_value = [mock_dag_resource]
                    mock_query.filter.return_value.delete.return_value = None
                elif model == mock_permission:
                    mock_query.filter.return_value.all.return_value = [mock_dag_permission]
                    mock_query.filter.return_value.delete.return_value = None
                elif model == mock_assoc:
                    mock_query.filter.return_value.delete.return_value = None
                return mock_query

            # Mock session.query method properly
            mock_session_query = MagicMock(side_effect=mock_query_side_effect)
            session.query = mock_session_query

            _cleanup_dag_permissions("test_dag", session)

            # Should log successful cleanup
            mock_log.info.assert_called_once_with(
                "Cleaned up %d DAG-specific permissions for dag_id: %s", 1, "test_dag"
            )


class TestDeleteDag:
    """Test cases for delete_dag function integration with permission cleanup."""

    def test_delete_dag_calls_cleanup_permissions(self, dag_maker, session):
        """Test that delete_dag calls _cleanup_dag_permissions."""
        # Mock all the dependencies to avoid database constraints
        with (
            patch("airflow.api.common.delete_dag._cleanup_dag_permissions") as mock_cleanup,
            patch.object(session, "scalar") as mock_scalar,
            patch.object(session, "execute") as mock_execute,
        ):
            # Mock that there are no running task instances
            mock_scalar.side_effect = [
                None,
                MagicMock(dag_id="test_dag"),
            ]  # First call for running TIs, second for DAG model
            mock_execute.return_value.rowcount = 1

            # This should call the cleanup function
            result = delete_dag("test_dag", session=session)

            # Should call cleanup permissions with correct arguments
            mock_cleanup.assert_called_once_with("test_dag", session)
            # Should return count of deleted records
            assert result >= 0

    def test_delete_dag_nonexistent_dag_does_not_call_cleanup(self, session):
        """Test that delete_dag does not call cleanup for non-existent DAG."""
        with patch("airflow.api.common.delete_dag._cleanup_dag_permissions") as mock_cleanup:
            with pytest.raises(DagNotFound):
                delete_dag("nonexistent_dag", session=session)

            # Should not call cleanup for non-existent DAG
            mock_cleanup.assert_not_called()
