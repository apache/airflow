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

import argparse
from contextlib import redirect_stdout
from importlib import reload
from io import StringIO
from unittest.mock import MagicMock, patch

import pytest

from airflow.cli import cli_parser
from airflow.providers.fab.auth_manager.cli_commands import permissions_command
from airflow.providers.fab.auth_manager.cli_commands.utils import get_application_builder

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test


class TestPermissionsCommand:
    """Test permissions cleanup CLI commands."""

    @pytest.fixture(autouse=True)
    def _set_attrs(self):
        with conf_vars(
            {
                (
                    "core",
                    "auth_manager",
                ): "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager",
            }
        ):
            # Reload the module to use FAB auth manager
            reload(cli_parser)
            # Clearing the cache before calling it
            cli_parser.get_parser.cache_clear()
            self.parser = cli_parser.get_parser()
            with get_application_builder() as appbuilder:
                self.appbuilder = appbuilder
                yield

    @patch("airflow.providers.fab.auth_manager.cli_commands.permissions_command.cleanup_dag_permissions")
    @patch("airflow.providers.fab.auth_manager.models.Resource")
    def test_permissions_cleanup_success(self, mock_resource, mock_cleanup_dag_permissions):
        """Test successful cleanup of DAG permissions."""
        # Mock args
        args = argparse.Namespace()
        args.dag_id = None
        args.dry_run = False
        args.yes = True
        args.verbose = True

        # Mock orphaned resources
        mock_orphaned_resource = MagicMock()
        mock_orphaned_resource.name = "DAG:orphaned_dag"

        with (
            patch("airflow.providers.fab.auth_manager.cli_commands.utils.get_application_builder"),
            patch("airflow.utils.session.create_session") as mock_session_ctx,
            patch("sqlalchemy.select"),
            redirect_stdout(StringIO()),
        ):
            mock_session = MagicMock()
            mock_session_ctx.return_value.__enter__.return_value = mock_session

            # Mock DagModel query - return existing DAGs
            mock_dag_result = MagicMock()
            mock_dag_result.all.return_value = [MagicMock(dag_id="existing_dag")]

            # Mock Resource query - return orphaned resources
            mock_resource_result = MagicMock()
            mock_resource_result.all.return_value = [mock_orphaned_resource]

            # Setup session.scalars to return different results for different queries
            mock_session.scalars.side_effect = [mock_dag_result, mock_resource_result]

            permissions_command.permissions_cleanup(args)

        # Verify function calls - it should be called exactly once for the orphaned DAG
        mock_cleanup_dag_permissions.assert_called_once_with("orphaned_dag", mock_session)

    @patch("airflow.providers.fab.auth_manager.cli_commands.permissions_command.cleanup_dag_permissions")
    @patch("airflow.providers.fab.auth_manager.models.Resource")
    def test_permissions_cleanup_dry_run(self, mock_resource, mock_cleanup_dag_permissions):
        """Test dry run mode for permissions cleanup."""
        # Mock args
        args = argparse.Namespace()
        args.dag_id = None
        args.dry_run = True
        args.verbose = True

        # Mock orphaned resources
        mock_orphaned_resource = MagicMock()
        mock_orphaned_resource.name = "DAG:orphaned_dag"

        with (
            patch("airflow.providers.fab.auth_manager.cli_commands.utils.get_application_builder"),
            patch("airflow.utils.session.create_session") as mock_session_ctx,
            patch("sqlalchemy.select"),
            redirect_stdout(StringIO()) as stdout,
        ):
            mock_session = MagicMock()
            mock_session_ctx.return_value.__enter__.return_value = mock_session

            # Mock DagModel query - return existing DAGs
            mock_dag_result = MagicMock()
            mock_dag_result.all.return_value = [MagicMock(dag_id="existing_dag")]

            # Mock Resource query - return orphaned resources
            mock_resource_result = MagicMock()
            mock_resource_result.all.return_value = [mock_orphaned_resource]

            # Setup session.scalars to return different results for different queries
            mock_session.scalars.side_effect = [mock_dag_result, mock_resource_result]

            permissions_command.permissions_cleanup(args)

        output = stdout.getvalue()
        assert "Dry run mode" in output or "No orphaned DAG permissions found" in output
        # In dry run mode, cleanup_dag_permissions should NOT be called
        mock_cleanup_dag_permissions.assert_not_called()

    @patch("airflow.providers.fab.auth_manager.cli_commands.permissions_command.cleanup_dag_permissions")
    @patch("airflow.providers.fab.auth_manager.models.Resource")
    def test_permissions_cleanup_specific_dag(self, mock_resource, mock_cleanup_dag_permissions):
        """Test cleanup for a specific DAG."""
        # Mock args
        args = argparse.Namespace()
        args.dag_id = "test_dag"
        args.dry_run = False
        args.yes = True
        args.verbose = True

        # Mock orphaned resource for the specific DAG
        mock_orphaned_resource = MagicMock()
        mock_orphaned_resource.name = "DAG:test_dag"

        with (
            patch("airflow.providers.fab.auth_manager.cli_commands.utils.get_application_builder"),
            patch("airflow.utils.session.create_session") as mock_session_ctx,
            patch("sqlalchemy.select"),
            redirect_stdout(StringIO()),
        ):
            mock_session = MagicMock()
            mock_session_ctx.return_value.__enter__.return_value = mock_session

            # Mock DagModel query - return existing DAGs (NOT including the target DAG)
            mock_dag_result = MagicMock()
            mock_dag_result.all.return_value = [
                MagicMock(dag_id="existing_dag"),
                MagicMock(dag_id="another_existing_dag"),
            ]

            # Mock Resource query - return orphaned resources
            mock_resource_result = MagicMock()
            mock_resource_result.all.return_value = [mock_orphaned_resource]

            # Setup session.scalars to return different results for different queries
            mock_session.scalars.side_effect = [mock_dag_result, mock_resource_result]

            permissions_command.permissions_cleanup(args)

        # Should call cleanup_dag_permissions specifically for test_dag
        mock_cleanup_dag_permissions.assert_called_once_with("test_dag", mock_session)

    @patch("airflow.providers.fab.auth_manager.cli_commands.permissions_command.cleanup_dag_permissions")
    @patch("airflow.providers.fab.auth_manager.models.Resource")
    @patch("builtins.input", return_value="n")
    def test_permissions_cleanup_no_confirmation(
        self, mock_input, mock_resource, mock_cleanup_dag_permissions
    ):
        """Test cleanup cancellation when user doesn't confirm."""
        # Mock args
        args = argparse.Namespace()
        args.dag_id = None
        args.dry_run = False
        args.yes = False
        args.verbose = False

        # Mock orphaned resources
        mock_orphaned_resource = MagicMock()
        mock_orphaned_resource.name = "DAG:orphaned_dag"

        with (
            patch("airflow.providers.fab.auth_manager.cli_commands.utils.get_application_builder"),
            patch("airflow.utils.session.create_session") as mock_session_ctx,
            patch("sqlalchemy.select"),
            redirect_stdout(StringIO()) as stdout,
        ):
            mock_session = MagicMock()
            mock_session_ctx.return_value.__enter__.return_value = mock_session

            # Mock DagModel query - return existing DAGs
            mock_dag_result = MagicMock()
            mock_dag_result.all.return_value = [MagicMock(dag_id="existing_dag")]

            # Mock Resource query - return orphaned resources
            mock_resource_result = MagicMock()
            mock_resource_result.all.return_value = [mock_orphaned_resource]

            # Setup session.scalars to return different results for different queries
            mock_session.scalars.side_effect = [mock_dag_result, mock_resource_result]

            permissions_command.permissions_cleanup(args)

        output = stdout.getvalue()
        # Should not call cleanup if user declines or no orphaned permissions found
        assert "Cleanup cancelled" in output or "No orphaned DAG permissions found" in output

        # cleanup_dag_permissions should NOT be called when user cancels
        if "Cleanup cancelled" in output:
            mock_cleanup_dag_permissions.assert_not_called()


class TestDagPermissions:
    """Test cases for cleanup_dag_permissions function with real database operations."""

    @pytest.fixture(autouse=True)
    def _setup_fab_test(self):
        """Setup FAB for testing."""
        with conf_vars(
            {
                (
                    "core",
                    "auth_manager",
                ): "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager",
            }
        ):
            with get_application_builder():
                yield

    def test_cleanup_dag_permissions_removes_specific_dag_resources(self):
        """Test that cleanup_dag_permissions removes only the specified DAG resources."""
        from sqlalchemy import select

        from airflow.providers.fab.auth_manager.cli_commands.permissions_command import (
            cleanup_dag_permissions,
        )
        from airflow.providers.fab.auth_manager.models import Action, Permission, Resource
        from airflow.providers.fab.www.security.permissions import RESOURCE_DAG_PREFIX
        from airflow.utils.session import create_session

        with create_session() as session:
            # Create resources for two different DAGs
            target_resource = Resource(name=f"{RESOURCE_DAG_PREFIX}target_dag")
            keep_resource = Resource(name=f"{RESOURCE_DAG_PREFIX}keep_dag")
            session.add_all([target_resource, keep_resource])
            session.flush()

            # Get or create action
            read_action = session.scalars(select(Action).where(Action.name == "can_read")).first()
            if not read_action:
                read_action = Action(name="can_read")
                session.add(read_action)
                session.flush()

            # Create permissions
            target_perm = Permission(action=read_action, resource=target_resource)
            keep_perm = Permission(action=read_action, resource=keep_resource)
            session.add_all([target_perm, keep_perm])
            session.commit()

            # Execute cleanup
            cleanup_dag_permissions("target_dag", session)

            # Verify: target resource deleted, keep resource remains
            assert not session.get(Resource, target_resource.id)
            assert session.get(Resource, keep_resource.id)
            assert not session.get(Permission, target_perm.id)
            assert session.get(Permission, keep_perm.id)

    def test_cleanup_dag_permissions_handles_no_matching_resources(self):
        """Test that cleanup_dag_permissions handles DAGs with no matching resources gracefully."""
        from sqlalchemy import func, select

        from airflow.providers.fab.auth_manager.cli_commands.permissions_command import (
            cleanup_dag_permissions,
        )
        from airflow.providers.fab.auth_manager.models import Resource
        from airflow.utils.session import create_session

        with create_session() as session:
            initial_count = session.scalar(select(func.count(Resource.id)))
            cleanup_dag_permissions("non_existent_dag", session)
            assert session.scalar(select(func.count(Resource.id))) == initial_count

    def test_cleanup_dag_permissions_handles_resources_without_permissions(self):
        """Test cleanup when resources exist but have no permissions."""
        from airflow.providers.fab.auth_manager.cli_commands.permissions_command import (
            cleanup_dag_permissions,
        )
        from airflow.providers.fab.auth_manager.models import Resource
        from airflow.providers.fab.www.security.permissions import RESOURCE_DAG_PREFIX
        from airflow.utils.session import create_session

        with create_session() as session:
            # Create resource without permissions
            resource = Resource(name=f"{RESOURCE_DAG_PREFIX}test_dag")
            session.add(resource)
            session.commit()
            resource_id = resource.id

            cleanup_dag_permissions("test_dag", session)
            assert not session.get(Resource, resource_id)

    def test_cleanup_dag_permissions_with_default_session(self):
        """Test cleanup_dag_permissions when no session is provided (uses default)."""
        from sqlalchemy import func, select

        from airflow.providers.fab.auth_manager.cli_commands.permissions_command import (
            cleanup_dag_permissions,
        )
        from airflow.providers.fab.auth_manager.models import Resource
        from airflow.providers.fab.www.security.permissions import RESOURCE_DAG_PREFIX
        from airflow.utils.session import create_session

        # Setup test data
        with create_session() as session:
            resource = Resource(name=f"{RESOURCE_DAG_PREFIX}test_dag")
            session.add(resource)
            session.commit()

        # Call cleanup without session parameter
        cleanup_dag_permissions("test_dag")

        # Verify deletion
        with create_session() as session:
            count = session.scalar(
                select(func.count(Resource.id)).where(Resource.name == f"{RESOURCE_DAG_PREFIX}test_dag")
            )
            assert count == 0
