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

    @patch("airflow.api_fastapi.app.get_auth_manager")
    def test_cleanup_dag_permissions_auth_manager_without_cleanup_method(
        self, mock_get_auth_manager, session
    ):
        """Test that cleanup is skipped when auth manager doesn't have cleanup_dag_permissions method."""
        mock_auth_manager = MagicMock()
        # Mock auth manager without cleanup_dag_permissions method
        del mock_auth_manager.cleanup_dag_permissions
        mock_get_auth_manager.return_value = mock_auth_manager

        # Should return early without calling any methods
        _cleanup_dag_permissions("test_dag", session)

        # Should still get the auth manager but not call cleanup
        mock_get_auth_manager.assert_called_once()

    @patch("airflow.api_fastapi.app.get_auth_manager")
    def test_cleanup_dag_permissions_auth_manager_with_cleanup_method(self, mock_get_auth_manager, session):
        """Test that cleanup is called when auth manager has cleanup_dag_permissions method."""
        mock_auth_manager = MagicMock()
        mock_auth_manager.cleanup_dag_permissions = MagicMock()
        mock_get_auth_manager.return_value = mock_auth_manager

        _cleanup_dag_permissions("test_dag", session)

        # Should call the auth manager's cleanup method
        mock_get_auth_manager.assert_called_once()
        mock_auth_manager.cleanup_dag_permissions.assert_called_once_with("test_dag", session)

    @patch("airflow.api_fastapi.app.get_auth_manager")
    def test_cleanup_dag_permissions_handles_get_auth_manager_exception(self, mock_get_auth_manager, session):
        """Test that cleanup handles exceptions from get_auth_manager gracefully."""
        mock_get_auth_manager.side_effect = Exception("Auth manager not available")

        # Should not raise exception, should handle gracefully
        try:
            _cleanup_dag_permissions("test_dag", session)
        except Exception:
            pytest.fail("_cleanup_dag_permissions should handle get_auth_manager exceptions gracefully")


class TestDeleteDag:
    """Test cases for delete_dag function integration with permission cleanup."""

    def test_delete_dag_calls_cleanup_permissions(self, dag_maker, session):
        """Test that delete_dag calls _cleanup_dag_permissions."""
        with dag_maker(dag_id="test_dag", session=session):
            pass

        # Mock the cleanup function to track if it's called
        with patch("airflow.api.common.delete_dag._cleanup_dag_permissions") as mock_cleanup:
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

    @patch("airflow.api_fastapi.app.get_auth_manager")
    def test_delete_dag_with_fab_auth_manager(self, mock_get_auth_manager, dag_maker, session):
        """Test that delete_dag works correctly with FabAuthManager."""
        with dag_maker(dag_id="test_dag", session=session):
            pass

        # Mock FabAuthManager with cleanup method
        mock_auth_manager = MagicMock()
        mock_auth_manager.cleanup_dag_permissions = MagicMock()
        mock_get_auth_manager.return_value = mock_auth_manager

        result = delete_dag("test_dag", session=session)

        # Should call the auth manager's cleanup method
        mock_auth_manager.cleanup_dag_permissions.assert_called_once_with("test_dag", session)
        assert result >= 0

    @patch("airflow.api_fastapi.app.get_auth_manager")
    def test_delete_dag_with_simple_auth_manager(self, mock_get_auth_manager, dag_maker, session):
        """Test that delete_dag works correctly with SimpleAuthManager (no cleanup method)."""
        with dag_maker(dag_id="test_dag", session=session):
            pass

        # Mock SimpleAuthManager without cleanup method
        mock_auth_manager = MagicMock()
        del mock_auth_manager.cleanup_dag_permissions
        mock_get_auth_manager.return_value = mock_auth_manager

        # Should not raise any exception
        result = delete_dag("test_dag", session=session)
        assert result >= 0
