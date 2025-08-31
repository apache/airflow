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
from unittest.mock import patch

import pytest

from airflow.cli import cli_parser

from tests_common.test_utils.compat import ignore_provider_compatibility_error
from tests_common.test_utils.config import conf_vars

with ignore_provider_compatibility_error("2.9.0+", __file__):
    from airflow.providers.fab.auth_manager.cli_commands import permissions_command
    from airflow.providers.fab.auth_manager.cli_commands.utils import get_application_builder

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

    @patch("airflow.providers.fab.auth_manager.dag_permissions.cleanup_dag_permissions")
    def test_permissions_cleanup_success(self, mock_cleanup_dag_permissions):
        """Test successful cleanup of DAG permissions."""
        # Mock args
        args = argparse.Namespace()
        args.dag_id = None
        args.dry_run = False
        args.yes = True
        args.verbose = True

        with redirect_stdout(StringIO()) as stdout:
            permissions_command.permissions_cleanup(args)

        # Verify function calls - it should be called for real DAGs
        output = stdout.getvalue()
        # Should either call cleanup or report no orphaned permissions found
        assert (
            "Successfully cleaned up permissions" in output or "No orphaned DAG permissions found" in output
        )

    def test_permissions_cleanup_dry_run(self):
        """Test dry run mode for permissions cleanup."""
        # Mock args
        args = argparse.Namespace()
        args.dag_id = None
        args.dry_run = True
        args.verbose = True

        with redirect_stdout(StringIO()) as stdout:
            permissions_command.permissions_cleanup(args)

        output = stdout.getvalue()
        assert "Dry run mode" in output or "No orphaned DAG permissions found" in output

    def test_permissions_cleanup_specific_dag(self):
        """Test cleanup for a specific DAG."""
        # Mock args
        args = argparse.Namespace()
        args.dag_id = "test_dag"
        args.dry_run = True
        args.verbose = True

        with redirect_stdout(StringIO()) as stdout:
            permissions_command.permissions_cleanup(args)

        output = stdout.getvalue()
        # Check for appropriate output indicating DAG-specific operation
        assert (
            "test_dag" in output
            or "No orphaned permissions found for DAG" in output
            or "not found in orphaned permissions" in output
            or "No orphaned DAG permissions found" in output
        )

    @patch("builtins.input", return_value="n")
    def test_permissions_cleanup_no_confirmation(self, mock_input):
        """Test cleanup cancellation when user doesn't confirm."""
        # Mock args
        args = argparse.Namespace()
        args.dag_id = None
        args.dry_run = False
        args.yes = False
        args.verbose = False

        with redirect_stdout(StringIO()) as stdout:
            permissions_command.permissions_cleanup(args)

        output = stdout.getvalue()
        # Should not call cleanup if user declines or no orphaned permissions found
        assert "Cleanup cancelled" in output or "No orphaned DAG permissions found" in output
