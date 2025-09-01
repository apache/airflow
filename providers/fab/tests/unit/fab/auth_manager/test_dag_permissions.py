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


class TestDagPermissions:
    """Test cases for dag_permissions module."""

    def test_cleanup_dag_permissions_with_session(self):
        """Test cleanup_dag_permissions with provided session."""
        from airflow.providers.fab.auth_manager.dag_permissions import cleanup_dag_permissions

        # Mock FAB models and select/delete
        with (
            patch("airflow.providers.fab.auth_manager.models") as mock_models,
            patch("sqlalchemy.select"),
            patch("sqlalchemy.delete"),
        ):
            mock_resource = MagicMock()
            mock_permission = MagicMock()
            mock_assoc = MagicMock()
            mock_models.Resource = mock_resource
            mock_models.Permission = mock_permission
            mock_models.assoc_permission_role = mock_assoc

            # Mock session with SQLAlchemy 2.0 methods
            mock_session = MagicMock()
            mock_session.scalars.return_value.all.return_value = []
            mock_session.execute.return_value = None

            cleanup_dag_permissions("test_dag", mock_session)

            # Should call session.scalars and session.execute for SQLAlchemy 2.0
            assert mock_session.scalars.called or mock_session.execute.called

    def test_cleanup_dag_permissions_without_session(self):
        """Test cleanup_dag_permissions without provided session (creates new session)."""
        from airflow.providers.fab.auth_manager.dag_permissions import cleanup_dag_permissions

        # Mock FAB models and select/delete
        with (
            patch("airflow.providers.fab.auth_manager.models") as mock_models,
            patch("sqlalchemy.select"),
            patch("sqlalchemy.delete"),
        ):
            mock_resource = MagicMock()
            mock_permission = MagicMock()
            mock_assoc = MagicMock()
            mock_models.Resource = mock_resource
            mock_models.Permission = mock_permission
            mock_models.assoc_permission_role = mock_assoc

            # Mock create_session
            with patch("airflow.utils.session.create_session") as mock_create_session:
                mock_session = MagicMock()
                mock_create_session.return_value.__enter__.return_value = mock_session
                mock_session.scalars.return_value.all.return_value = []
                mock_session.execute.return_value = None

                cleanup_dag_permissions("test_dag")

                # Should create a new session
                mock_create_session.assert_called_once()

    def test_cleanup_dag_permissions_with_resources_and_permissions(self):
        """Test cleanup_dag_permissions with actual resources and permissions."""
        from airflow.providers.fab.auth_manager.dag_permissions import cleanup_dag_permissions

        # Mock FAB models and select/delete
        with (
            patch("airflow.providers.fab.auth_manager.models") as mock_models,
            patch("sqlalchemy.select"),
            patch("sqlalchemy.delete"),
        ):
            mock_resource = MagicMock()
            mock_permission = MagicMock()
            mock_assoc = MagicMock()
            mock_models.Resource = mock_resource
            mock_models.Permission = mock_permission
            mock_models.assoc_permission_role = mock_assoc

            # Mock session
            mock_session = MagicMock()

            # Mock resources exist
            mock_dag_resource = MagicMock()
            mock_dag_resource.id = 1

            # Mock permissions exist
            mock_dag_permission = MagicMock()
            mock_dag_permission.id = 1

            # Setup mock returns for different select queries
            def mock_scalars_side_effect(stmt):
                mock_result = MagicMock()
                # Return resources for resource queries
                mock_result.all.return_value = [mock_dag_resource]
                return mock_result

            mock_session.scalars.side_effect = mock_scalars_side_effect
            mock_session.execute.return_value = None

            cleanup_dag_permissions("test_dag", mock_session)

            # Should perform database operations
            assert mock_session.scalars.called or mock_session.execute.called

    def test_cleanup_dag_permissions_with_resources_but_no_permissions(self):
        """Test cleanup_dag_permissions with resources but no permissions."""
        from airflow.providers.fab.auth_manager.dag_permissions import cleanup_dag_permissions

        # Mock FAB models and select/delete
        with (
            patch("airflow.providers.fab.auth_manager.models") as mock_models,
            patch("sqlalchemy.select"),
            patch("sqlalchemy.delete"),
        ):
            mock_resource = MagicMock()
            mock_permission = MagicMock()
            mock_assoc = MagicMock()
            mock_models.Resource = mock_resource
            mock_models.Permission = mock_permission
            mock_models.assoc_permission_role = mock_assoc

            # Mock session
            mock_session = MagicMock()

            # Mock resources exist but no permissions
            mock_dag_resource = MagicMock()
            mock_dag_resource.id = 1

            def mock_scalars_side_effect(stmt):
                mock_result = MagicMock()
                # Return resources for resource queries
                mock_result.all.return_value = [mock_dag_resource]
                return mock_result

            mock_session.scalars.side_effect = mock_scalars_side_effect
            mock_session.execute.return_value = None

            cleanup_dag_permissions("test_dag", mock_session)

            # Should still perform database operations
            assert mock_session.scalars.called or mock_session.execute.called
