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

from airflow.cli import cli_parser
from airflow.cli.commands import db_manager_command
from airflow.utils.db_manager import BaseDBManager

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test


class FakeDBManager(BaseDBManager):
    metadata = mock.MagicMock()
    migration_dir = "migrations"
    alembic_file = "alembic.ini"
    version_table_name = "alembic_version_ext"
    revision_heads_map = {}

    # Test controls
    raise_on_init = False
    instances: list[FakeDBManager] = []
    last_instance: FakeDBManager | None = None

    def __init__(self, session):
        if self.raise_on_init:
            raise AssertionError("Should not instantiate manager when cancelled")
        super().__init__(session)
        self._resetdb_mock = mock.MagicMock(name="resetdb")
        self._upgradedb_mock = mock.MagicMock(name="upgradedb")
        self._downgrade_mock = mock.MagicMock(name="downgrade")
        FakeDBManager.instances.append(self)
        FakeDBManager.last_instance = self

    def resetdb(self, skip_init=False):
        return self._resetdb_mock(skip_init=skip_init)

    def upgradedb(self, to_revision=None, from_revision=None, show_sql_only=False):
        return self._upgradedb_mock(
            to_revision=to_revision, from_revision=from_revision, show_sql_only=show_sql_only
        )

    def downgrade(self, to_revision, from_revision=None, show_sql_only=False):
        return self._downgrade_mock(
            to_revision=to_revision, from_revision=from_revision, show_sql_only=show_sql_only
        )


@pytest.fixture(autouse=True)
def _reset_fake_db_manager():
    FakeDBManager.revision_heads_map = {}
    FakeDBManager.raise_on_init = False
    FakeDBManager.instances = []
    FakeDBManager.last_instance = None
    return None


class TestCliDbManager:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    @mock.patch("airflow.cli.commands.db_manager_command.settings.Session", autospec=True)
    @mock.patch("airflow.cli.commands.db_manager_command._get_db_manager")
    def test_cli_resetdb_yes_calls_reset(self, mock_get_db_manager, mock_session):
        manager_name = "path.to.FakeDBManager"
        mock_get_db_manager.return_value = FakeDBManager

        args = self.parser.parse_args(["db-manager", "reset", manager_name, "--yes"])
        db_manager_command.resetdb(args)

        mock_get_db_manager.assert_called_once_with(manager_name)
        assert len(FakeDBManager.instances) == 1
        FakeDBManager.last_instance._resetdb_mock.assert_called_once_with(skip_init=False)

    @mock.patch("airflow.cli.commands.db_manager_command.settings.Session", autospec=True)
    @mock.patch("airflow.cli.commands.db_manager_command._get_db_manager")
    def test_cli_resetdb_skip_init(self, mock_get_db_manager, mock_session):
        manager_name = "path.to.FakeDBManager"
        mock_get_db_manager.return_value = FakeDBManager

        args = self.parser.parse_args(["db-manager", "reset", manager_name, "--yes", "--skip-init"])
        db_manager_command.resetdb(args)
        mock_get_db_manager.assert_called_once_with(manager_name)
        assert len(FakeDBManager.instances) == 1
        FakeDBManager.last_instance._resetdb_mock.assert_called_once_with(skip_init=True)

    @mock.patch("airflow.cli.commands.db_manager_command.input")
    @mock.patch("airflow.cli.commands.db_manager_command._get_db_manager")
    def test_cli_resetdb_prompt_yes(self, mock_get_db_manager, mock_input):
        mock_input.return_value = "Y"
        manager_name = "path.to.FakeDBManager"
        mock_get_db_manager.return_value = FakeDBManager
        args = self.parser.parse_args(["db-manager", "reset", manager_name])
        db_manager_command.resetdb(args)
        assert len(FakeDBManager.instances) == 1
        FakeDBManager.last_instance._resetdb_mock.assert_called_once_with(skip_init=False)

    @mock.patch("airflow.cli.commands.db_manager_command.input")
    @mock.patch("airflow.cli.commands.db_manager_command._get_db_manager")
    def test_cli_resetdb_prompt_cancel(self, mock_get_db_manager, mock_input):
        mock_input.return_value = "n"
        manager_name = "path.to.FakeDBManager"
        FakeDBManager.raise_on_init = True
        mock_get_db_manager.return_value = FakeDBManager
        args = self.parser.parse_args(["db-manager", "reset", manager_name])
        with pytest.raises(SystemExit, match="Cancelled"):
            db_manager_command.resetdb(args)
        assert FakeDBManager.instances == []

    @mock.patch("airflow.cli.commands.db_manager_command.settings.Session", autospec=True)
    @mock.patch("airflow.cli.commands.db_manager_command._get_db_manager")
    @mock.patch("airflow.cli.commands.db_manager_command.run_db_migrate_command")
    def test_cli_migrate_db(self, mock_run_db_migrate_cmd, mock_get_db_manager, mock_session):
        manager_name = "path.to.FakeDBManager"
        FakeDBManager.revision_heads_map = {"2.10.0": "22ed7efa9da2"}
        mock_get_db_manager.return_value = FakeDBManager

        args = self.parser.parse_args(["db-manager", "migrate", manager_name])
        db_manager_command.migratedb(args)

        mock_get_db_manager.assert_called_once_with(manager_name)
        assert len(FakeDBManager.instances) == 1
        # Validate run_db_migrate_command was called with the instance's upgradedb and correct heads map
        called_args, called_kwargs = mock_run_db_migrate_cmd.call_args
        assert called_args[0] is args
        # Verify the bound method refers to the instance's upgradedb implementation
        assert called_args[1].__self__ is FakeDBManager.last_instance
        assert called_args[1].__func__ is FakeDBManager.upgradedb
        assert called_kwargs["revision_heads_map"] == {"2.10.0": "22ed7efa9da2"}

    @mock.patch("airflow.cli.commands.db_manager_command.settings.Session", autospec=True)
    @mock.patch("airflow.cli.commands.db_manager_command._get_db_manager")
    def test_cli_migrate_db_calls_upgradedb_with_args(self, mock_get_db_manager, mock_session):
        manager_name = "path.to.FakeDBManager"
        mock_get_db_manager.return_value = FakeDBManager

        args = self.parser.parse_args(
            [
                "db-manager",
                "migrate",
                manager_name,
                "--to-revision",
                "abc",
                "--from-revision",
                "def",
                "--show-sql-only",
            ]
        )
        db_manager_command.migratedb(args)

        assert FakeDBManager.last_instance is not None
        FakeDBManager.last_instance._upgradedb_mock.assert_called_once_with(
            to_revision="abc", from_revision="def", show_sql_only=True
        )

    @mock.patch("airflow.cli.commands.db_manager_command.settings.Session", autospec=True)
    @mock.patch("airflow.cli.commands.db_manager_command._get_db_manager")
    def test_cli_downgrade_db_calls_downgrade_with_args(self, mock_get_db_manager, mock_session):
        manager_name = "path.to.FakeDBManager"
        mock_get_db_manager.return_value = FakeDBManager

        args = self.parser.parse_args(
            [
                "db-manager",
                "downgrade",
                manager_name,
                "--to-revision",
                "abc",
                "--from-revision",
                "def",
                "--show-sql-only",
            ]
        )
        db_manager_command.downgrade(args)

        assert FakeDBManager.last_instance is not None
        FakeDBManager.last_instance._downgrade_mock.assert_called_once_with(
            to_revision="abc", from_revision="def", show_sql_only=True
        )

    @mock.patch("airflow.cli.commands.db_manager_command.settings.Session", autospec=True)
    @mock.patch("airflow.cli.commands.db_manager_command._get_db_manager")
    @mock.patch("airflow.cli.commands.db_manager_command.run_db_downgrade_command")
    def test_cli_downgrade_db(self, mock_run_db_downgrade_cmd, mock_get_db_manager, mock_session):
        manager_name = "path.to.FakeDBManager"
        FakeDBManager.revision_heads_map = {"2.10.0": "22ed7efa9da2"}
        mock_get_db_manager.return_value = FakeDBManager

        args = self.parser.parse_args(["db-manager", "downgrade", manager_name])
        db_manager_command.downgrade(args)

        mock_get_db_manager.assert_called_once_with(manager_name)
        assert len(FakeDBManager.instances) == 1
        called_args, called_kwargs = mock_run_db_downgrade_cmd.call_args
        assert called_args[0] is args
        # Verify the bound method refers to the instance's downgrade implementation
        assert called_args[1].__self__ is FakeDBManager.last_instance
        assert called_args[1].__func__ is FakeDBManager.downgrade
        assert called_kwargs["revision_heads_map"] == {"2.10.0": "22ed7efa9da2"}

    @conf_vars({("database", "external_db_managers"): "path.to.manager.TestDBManager"})
    @mock.patch("airflow.cli.commands.db_manager_command.import_string")
    def test_get_db_manager(self, mock_import_string):
        manager_name = "path.to.manager.TestDBManager"
        db_manager = db_manager_command._get_db_manager(manager_name)
        mock_import_string.assert_called_once_with("path.to.manager.TestDBManager")
        assert db_manager is not None

    @conf_vars({("database", "external_db_managers"): "path.to.manager.TestDBManager"})
    @mock.patch("airflow.cli.commands.db_manager_command.import_string")
    def test_get_db_manager_raises(self, mock_import_string):
        manager_name = "NonExistentDBManager"
        with pytest.raises(SystemExit):
            db_manager_command._get_db_manager(manager_name)
