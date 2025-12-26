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

from unittest.mock import patch

import pytest

from airflow import models, settings
from airflow.cli import cli_parser
from airflow.cli.commands import team_command
from airflow.models import Connection, Pool, Variable
from airflow.models.dagbundle import DagBundleModel
from airflow.models.team import Team, dag_bundle_team_association_table
from airflow.settings import Session

from tests_common.test_utils.db import (
    clear_db_connections,
    clear_db_dag_bundles,
    clear_db_pools,
    clear_db_teams,
    clear_db_variables,
)

pytestmark = pytest.mark.db_test


class TestCliTeams:
    @classmethod
    def _cleanup(cls):
        clear_db_connections(add_default_connections_back=False)
        clear_db_variables()
        clear_db_pools()
        clear_db_dag_bundles()
        clear_db_teams()

    @classmethod
    def setup_class(cls):
        cls.dagbag = models.DagBag(include_examples=True)
        cls.parser = cli_parser.get_parser()
        settings.configure_orm()
        cls.session = Session
        cls._cleanup()

    def teardown_method(self):
        """Called after each test method."""
        self._cleanup()

    def test_team_create_success(self, stdout_capture):
        """Test successful team creation."""
        with stdout_capture as stdout:
            team_command.team_create(self.parser.parse_args(["teams", "create", "test-team"]))

        # Verify team was created in database
        team = self.session.query(Team).filter(Team.name == "test-team").first()
        assert team is not None
        assert team.name == "test-team"

        # Verify output message
        output = stdout.getvalue()
        assert "Team 'test-team' created successfully" in output
        assert str(team.name) in output

    def test_team_create_empty_name(self):
        """Test team creation with empty name."""
        with pytest.raises(SystemExit, match="Team name cannot be empty"):
            team_command.team_create(self.parser.parse_args(["teams", "create", ""]))

    def test_team_create_invalid_name(self):
        with pytest.raises(SystemExit, match="Invalid team name"):
            team_command.team_create(self.parser.parse_args(["teams", "create", "test with space"]))

    def test_team_create_whitespace_name(self):
        """Test team creation with whitespace-only name."""
        with pytest.raises(SystemExit, match="Team name cannot be empty"):
            team_command.team_create(self.parser.parse_args(["teams", "create", "   "]))

    def test_team_create_duplicate_name(self):
        """Test team creation with duplicate name."""
        # Create first team
        team_command.team_create(self.parser.parse_args(["teams", "create", "duplicate-team"]))

        # Try to create team with same name
        with pytest.raises(SystemExit, match="Team with name 'duplicate-team' already exists"):
            team_command.team_create(self.parser.parse_args(["teams", "create", "duplicate-team"]))

    def test_team_list_empty(self, stdout_capture):
        """Test listing teams when none exist."""
        with stdout_capture as stdout:
            team_command.team_list(self.parser.parse_args(["teams", "list"]))

        # Should not error, just show empty result
        output = stdout.getvalue()
        # The exact output format depends on the AirflowConsole implementation
        # but it should not contain any team names
        assert team_command.NO_TEAMS_LIST_MSG in output

    def test_team_list_with_teams(self, stdout_capture):
        """Test listing teams when teams exist."""
        # Create test teams
        team_command.team_create(self.parser.parse_args(["teams", "create", "team-alpha"]))
        team_command.team_create(self.parser.parse_args(["teams", "create", "team-beta"]))

        with stdout_capture as stdout:
            team_command.team_list(self.parser.parse_args(["teams", "list"]))

        output = stdout.getvalue()
        assert "team-alpha" in output
        assert "team-beta" in output

    def test_team_list_with_output_format(self):
        """Test listing teams with different output formats."""
        # Create a test team
        team_command.team_create(self.parser.parse_args(["teams", "create", "format-test"]))

        # Test different output formats
        team_command.team_list(self.parser.parse_args(["teams", "list", "--output", "json"]))
        team_command.team_list(self.parser.parse_args(["teams", "list", "--output", "yaml"]))
        team_command.team_list(self.parser.parse_args(["teams", "list", "--output", "plain"]))

    def test_team_delete_success(self, stdout_capture):
        """Test successful team deletion."""
        # Create team first
        team_command.team_create(self.parser.parse_args(["teams", "create", "delete-me"]))

        # Verify team exists
        team = self.session.query(Team).filter(Team.name == "delete-me").first()
        assert team is not None

        # Delete team with --yes flag
        with stdout_capture as stdout:
            team_command.team_delete(self.parser.parse_args(["teams", "delete", "delete-me", "--yes"]))

        # Verify team was deleted
        team = self.session.query(Team).filter(Team.name == "delete-me").first()
        assert team is None

        # Verify output message
        output = stdout.getvalue()
        assert "Team 'delete-me' deleted successfully" in output

    def test_team_delete_nonexistent(self):
        """Test deleting a team that doesn't exist."""
        with pytest.raises(SystemExit, match="Team 'nonexistent' does not exist"):
            team_command.team_delete(self.parser.parse_args(["teams", "delete", "nonexistent", "--yes"]))

    def test_team_delete_empty_name(self):
        """Test deleting team with empty name."""
        with pytest.raises(SystemExit, match="Team name cannot be empty"):
            team_command.team_delete(self.parser.parse_args(["teams", "delete", "", "--yes"]))

    def test_team_delete_with_dag_bundle_association(self):
        """Test deleting team that has DAG bundle associations."""
        # Create team
        team_command.team_create(self.parser.parse_args(["teams", "create", "bundle-team"]))
        team = self.session.query(Team).filter(Team.name == "bundle-team").first()

        # Create a DAG bundle first
        dag_bundle = DagBundleModel(name="test-bundle")
        self.session.add(dag_bundle)
        self.session.commit()

        # Create a DAG bundle association
        self.session.execute(
            dag_bundle_team_association_table.insert().values(
                dag_bundle_name="test-bundle", team_name=team.name
            )
        )
        self.session.commit()

        # Try to delete team
        with pytest.raises(
            SystemExit,
            match="Cannot delete team 'bundle-team' because it is associated with: 1 DAG bundle\\(s\\)",
        ):
            team_command.team_delete(self.parser.parse_args(["teams", "delete", "bundle-team", "--yes"]))

    def test_team_delete_with_connection_association(self):
        """Test deleting team that has connection associations."""
        # Create team
        team_command.team_create(self.parser.parse_args(["teams", "create", "conn-team"]))
        team = self.session.query(Team).filter(Team.name == "conn-team").first()

        # Create connection associated with team
        conn = Connection(conn_id="test-conn", conn_type="http", team_name=team.name)
        self.session.add(conn)
        self.session.commit()

        # Try to delete team
        with pytest.raises(
            SystemExit,
            match="Cannot delete team 'conn-team' because it is associated with: 1 connection\\(s\\)",
        ):
            team_command.team_delete(self.parser.parse_args(["teams", "delete", "conn-team", "--yes"]))

    def test_team_delete_with_variable_association(self):
        """Test deleting team that has variable associations."""
        # Create team
        team_command.team_create(self.parser.parse_args(["teams", "create", "var-team"]))
        team = self.session.query(Team).filter(Team.name == "var-team").first()

        # Create variable associated with team
        var = Variable(key="test-var", val="test-value", team_name=team.name)
        self.session.add(var)
        self.session.commit()

        # Try to delete team
        with pytest.raises(
            SystemExit, match="Cannot delete team 'var-team' because it is associated with: 1 variable\\(s\\)"
        ):
            team_command.team_delete(self.parser.parse_args(["teams", "delete", "var-team", "--yes"]))

    def test_team_delete_with_pool_association(self):
        """Test deleting team that has pool associations."""
        # Create team
        team_command.team_create(self.parser.parse_args(["teams", "create", "pool-team"]))
        team = self.session.query(Team).filter(Team.name == "pool-team").first()

        # Create pool associated with team
        pool = Pool(
            pool="test-pool", slots=5, description="Test pool", include_deferred=False, team_name=team.name
        )
        self.session.add(pool)
        self.session.commit()

        # Try to delete team
        with pytest.raises(
            SystemExit, match="Cannot delete team 'pool-team' because it is associated with: 1 pool\\(s\\)"
        ):
            team_command.team_delete(self.parser.parse_args(["teams", "delete", "pool-team", "--yes"]))

    def test_team_delete_with_multiple_associations(self):
        """Test deleting team that has multiple types of associations."""
        # Create team
        team_command.team_create(self.parser.parse_args(["teams", "create", "multi-team"]))
        team = self.session.query(Team).filter(Team.name == "multi-team").first()

        # Create a DAG bundle first
        dag_bundle = DagBundleModel(name="multi-bundle")
        self.session.add(dag_bundle)
        self.session.commit()

        # Create multiple associations
        conn = Connection(conn_id="multi-conn", conn_type="http", team_name=team.name)
        var = Variable(key="multi-var", val="value", team_name=team.name)
        pool = Pool(
            pool="multi-pool", slots=3, description="Multi pool", include_deferred=False, team_name=team.name
        )

        self.session.add_all([conn, var, pool])
        self.session.execute(
            dag_bundle_team_association_table.insert().values(
                dag_bundle_name="multi-bundle", team_name=team.name
            )
        )
        self.session.commit()

        # Try to delete team
        with pytest.raises(SystemExit) as exc_info:
            team_command.team_delete(self.parser.parse_args(["teams", "delete", "multi-team", "--yes"]))

        error_msg = str(exc_info.value)
        assert "Cannot delete team 'multi-team' because it is associated with:" in error_msg
        assert "1 DAG bundle(s)" in error_msg
        assert "1 connection(s)" in error_msg
        assert "1 variable(s)" in error_msg
        assert "1 pool(s)" in error_msg

    @patch("builtins.input", return_value="Y")
    def test_team_delete_with_confirmation_yes(self, mock_input, stdout_capture):
        """Test team deletion with user confirmation (Yes)."""
        # Create team
        team_command.team_create(self.parser.parse_args(["teams", "create", "confirm-yes"]))

        # Delete without --yes flag (should prompt for confirmation)
        with stdout_capture as stdout:
            team_command.team_delete(self.parser.parse_args(["teams", "delete", "confirm-yes"]))

        # Verify team was deleted
        team = self.session.query(Team).filter(Team.name == "confirm-yes").first()
        assert team is None

        output = stdout.getvalue()
        assert "Team 'confirm-yes' deleted successfully" in output

    @patch("builtins.input", return_value="N")
    def test_team_delete_with_confirmation_no(self, mock_input, stdout_capture):
        """Test team deletion with user confirmation (No)."""
        # Create team
        team_command.team_create(self.parser.parse_args(["teams", "create", "confirm-no"]))

        # Delete without --yes flag (should prompt for confirmation)
        with stdout_capture as stdout:
            team_command.team_delete(self.parser.parse_args(["teams", "delete", "confirm-no"]))

        # Verify team was NOT deleted
        team = self.session.query(Team).filter(Team.name == "confirm-no").first()
        assert team is not None

        output = stdout.getvalue()
        assert "Team deletion cancelled" in output

    @patch("builtins.input", return_value="invalid")
    def test_team_delete_with_confirmation_invalid(self, mock_input, stdout_capture):
        """Test team deletion with invalid confirmation input."""
        # Create team
        team_command.team_create(self.parser.parse_args(["teams", "create", "confirm-invalid"]))

        # Delete without --yes flag (should prompt for confirmation)
        with stdout_capture as stdout:
            team_command.team_delete(self.parser.parse_args(["teams", "delete", "confirm-invalid"]))

        # Verify team was NOT deleted (invalid input treated as No)
        team = self.session.query(Team).filter(Team.name == "confirm-invalid").first()
        assert team is not None

        output = stdout.getvalue()
        assert "Team deletion cancelled" in output

    def test_team_operations_integration(self):
        """Test integration of create, list, and delete operations."""
        # Start with empty state
        teams = self.session.query(Team).all()
        assert len(teams) == 0

        # Create multiple teams
        team_command.team_create(self.parser.parse_args(["teams", "create", "integration-1"]))
        team_command.team_create(self.parser.parse_args(["teams", "create", "integration-2"]))
        team_command.team_create(self.parser.parse_args(["teams", "create", "integration-3"]))

        # Verify all teams exist
        teams = self.session.query(Team).all()
        assert len(teams) == 3
        team_names = [team.name for team in teams]
        assert "integration-1" in team_names
        assert "integration-2" in team_names
        assert "integration-3" in team_names

        # Delete one team
        team_command.team_delete(self.parser.parse_args(["teams", "delete", "integration-2", "--yes"]))

        # Verify correct team was deleted
        teams = self.session.query(Team).all()
        assert len(teams) == 2
        team_names = [team.name for team in teams]
        assert "integration-1" in team_names
        assert "integration-2" not in team_names
        assert "integration-3" in team_names
