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
"""Test permissions command."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from airflow.providers.fab.auth_manager.cli_commands.permissions_command import (
    permissions_cleanup,
)


class TestPermissionsCommand:
    """Test permissions cleanup CLI commands."""

    @patch("airflow.providers.fab.auth_manager.dag_permissions.cleanup_dag_permissions")
    @patch("airflow.utils.session.create_session")
    @patch("airflow.providers.fab.auth_manager.cli_commands.utils.get_application_builder")
    def test_permissions_cleanup_success(
        self, mock_get_application_builder, mock_create_session, mock_cleanup_dag_permissions
    ):
        """Test successful cleanup of DAG permissions."""
        # Mock session
        mock_session = MagicMock()
        mock_create_session.return_value.__enter__.return_value = mock_session

        # Mock application builder
        mock_appbuilder = MagicMock()
        mock_get_application_builder.return_value.__enter__.return_value = mock_appbuilder

        # Mock DAG models (existing DAGs)
        mock_dag_model = MagicMock()
        mock_dag_model.dag_id = "existing_dag"
        mock_session.query.return_value.all.return_value = [mock_dag_model]

        # Mock orphaned resources
        mock_resource = MagicMock()
        mock_resource.name = "DAG:deleted_dag"
        mock_session.query.return_value.filter.return_value.all.return_value = [mock_resource]

        # Mock args
        import argparse

        args = argparse.Namespace()
        args.dag_id = None
        args.dry_run = False
        args.yes = True
        args.verbose = True

        # Execute command
        permissions_cleanup(args)

        # Verify function calls
        mock_cleanup_dag_permissions.assert_called()

    @patch("airflow.utils.session.create_session")
    @patch("airflow.providers.fab.auth_manager.cli_commands.utils.get_application_builder")
    def test_permissions_cleanup_dry_run(self, mock_get_application_builder, mock_create_session):
        """Test dry run mode for permissions cleanup."""
        # Mock session and data
        mock_session = MagicMock()
        mock_create_session.return_value.__enter__.return_value = mock_session

        # Mock DAG models (existing DAGs)
        mock_dag_model = MagicMock()
        mock_dag_model.dag_id = "existing_dag"
        mock_session.query.return_value.all.return_value = [mock_dag_model]

        # Mock orphaned resources
        mock_resource = MagicMock()
        mock_resource.name = "DAG:deleted_dag"
        mock_session.query.return_value.filter.return_value.all.return_value = [mock_resource]

        # Mock args
        import argparse

        args = argparse.Namespace()
        args.dag_id = None
        args.dry_run = True
        args.verbose = True

        # Mock application builder
        mock_appbuilder = MagicMock()
        mock_get_application_builder.return_value.__enter__.return_value = mock_appbuilder

        # Execute command
        permissions_cleanup(args)
