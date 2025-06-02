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

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test


class TestCliDbManager:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    @mock.patch("airflow.cli.commands.db_manager_command._get_db_manager")
    def test_cli_resetdb(self, mock_get_db_manager):
        manager_name = "path.to.TestDBManager"
        db_manager_command.resetdb(self.parser.parse_args(["db-manager", "reset", manager_name, "--yes"]))
        mock_get_db_manager.assert_called_once_with("path.to.TestDBManager")
        mock_get_db_manager.return_value.resetdb.asset_called_once()

    @mock.patch("airflow.cli.commands.db_manager_command._get_db_manager")
    def test_cli_resetdb_skip_init(self, mock_get_db_manager):
        manager_name = "path.to.TestDBManager"
        db_manager_command.resetdb(
            self.parser.parse_args(["db-manager", "reset", manager_name, "--yes", "--skip-init"])
        )
        mock_get_db_manager.assert_called_once_with(manager_name)
        mock_get_db_manager.return_value.resetdb.asset_called_once_with(skip_init=True)

    @mock.patch("airflow.cli.commands.db_manager_command._get_db_manager")
    @mock.patch("airflow.cli.commands.db_manager_command.run_db_migrate_command")
    def test_cli_migrate_db(self, mock_run_db_migrate_cmd, mock_get_db_manager):
        manager_name = "path.to.TestDBManager"
        db_manager_command.migratedb(self.parser.parse_args(["db-manager", "migrate", manager_name]))
        mock_get_db_manager.assert_called_once_with(manager_name)
        mock_run_db_migrate_cmd.assert_called_once()

    @mock.patch("airflow.cli.commands.db_manager_command._get_db_manager")
    @mock.patch("airflow.cli.commands.db_manager_command.run_db_downgrade_command")
    def test_cli_downgrade_db(self, mock_run_db_downgrade_cmd, mock_get_db_manager):
        manager_name = "path.to.TestDBManager"
        db_manager_command.downgrade(self.parser.parse_args(["db-manager", "downgrade", manager_name]))
        mock_get_db_manager.assert_called_once_with(manager_name)
        mock_run_db_downgrade_cmd.assert_called_once()

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
