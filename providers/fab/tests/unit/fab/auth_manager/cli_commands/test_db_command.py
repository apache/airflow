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

from importlib import reload
from unittest import mock

import pytest

from airflow.cli import cli_parser

from tests_common.test_utils.cli import skip_cli_test_marker
from tests_common.test_utils.config import conf_vars

pytestmark = [pytest.mark.db_test]
try:
    from airflow.providers.fab.auth_manager.cli_commands import db_command
    from airflow.providers.fab.auth_manager.models.db import FABDBManager

    @skip_cli_test_marker("airflow.providers.fab.cli.definition", "FAB")
    class TestFABCLiDB:
        @classmethod
        def setup_class(cls):
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
                cls.parser = cli_parser.get_parser()

        @mock.patch.object(FABDBManager, "resetdb")
        def test_cli_resetdb(self, mock_resetdb):
            db_command.resetdb(self.parser.parse_args(["fab-db", "reset", "--yes"]))

            mock_resetdb.assert_called_once_with(skip_init=False)

        @mock.patch.object(FABDBManager, "resetdb")
        def test_cli_resetdb_skip_init(self, mock_resetdb):
            db_command.resetdb(self.parser.parse_args(["fab-db", "reset", "--yes", "--skip-init"]))
            mock_resetdb.assert_called_once_with(skip_init=True)

        @pytest.mark.parametrize(
            ("args", "called_with"),
            [
                (
                    [],
                    dict(
                        to_revision=None,
                        from_revision=None,
                        show_sql_only=False,
                    ),
                ),
                (
                    ["--show-sql-only"],
                    dict(
                        to_revision=None,
                        from_revision=None,
                        show_sql_only=True,
                    ),
                ),
                (
                    ["--to-revision", "abc"],
                    dict(
                        to_revision="abc",
                        from_revision=None,
                        show_sql_only=False,
                    ),
                ),
                (
                    ["--to-revision", "abc", "--show-sql-only"],
                    dict(to_revision="abc", from_revision=None, show_sql_only=True),
                ),
                (
                    ["--to-revision", "abc", "--from-revision", "abc123", "--show-sql-only"],
                    dict(
                        to_revision="abc",
                        from_revision="abc123",
                        show_sql_only=True,
                    ),
                ),
            ],
        )
        @mock.patch.object(FABDBManager, "upgradedb")
        def test_cli_upgrade_success(self, mock_upgradedb, args, called_with):
            db_command.migratedb(self.parser.parse_args(["fab-db", "migrate", *args]))
            mock_upgradedb.assert_called_once_with(**called_with)

        @pytest.mark.parametrize(
            ("args", "pattern"),
            [
                pytest.param(
                    ["--to-revision", "abc", "--to-version", "1.3.0"],
                    "Cannot supply both",
                    id="to both version and revision",
                ),
                pytest.param(
                    ["--from-revision", "abc", "--from-version", "1.3.0"],
                    "Cannot supply both",
                    id="from both version and revision",
                ),
                pytest.param(["--to-version", "1.2.0"], "Unknown version '1.2.0'", id="unknown to version"),
                pytest.param(["--to-version", "abc"], "Invalid version 'abc'", id="invalid to version"),
                pytest.param(
                    ["--to-revision", "abc", "--from-revision", "abc123"],
                    "used with `--show-sql-only`",
                    id="requires offline",
                ),
                pytest.param(
                    ["--to-revision", "abc", "--from-version", "1.3.0"],
                    "used with `--show-sql-only`",
                    id="requires offline",
                ),
                pytest.param(
                    ["--to-revision", "abc", "--from-version", "1.1.25", "--show-sql-only"],
                    "Unknown version '1.1.25'",
                    id="unknown from version",
                ),
                pytest.param(
                    ["--to-revision", "adaf", "--from-version", "abc", "--show-sql-only"],
                    "Invalid version 'abc'",
                    id="invalid from version",
                ),
            ],
        )
        @mock.patch.object(FABDBManager, "upgradedb")
        def test_cli_migratedb_failure(self, mock_upgradedb, args, pattern):
            with pytest.raises(SystemExit, match=pattern):
                db_command.migratedb(self.parser.parse_args(["fab-db", "migrate", *args]))
except (ModuleNotFoundError, ImportError):
    pass
