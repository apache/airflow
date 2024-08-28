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

pytestmark = [pytest.mark.db_test]
try:
    from airflow.providers.fab.auth_manager.cli_commands import db_command
    from airflow.providers.fab.auth_manager.models.db import FABDBManager

    class TestCLiDB:
        @classmethod
        def setup_class(cls):
            cls.parser = cli_parser.get_parser()

        @mock.patch.object(FABDBManager, "resetdb")
        def test_cli_resetdb(self, mock_resetdb):
            db_command.resetdb(self.parser.parse_args(["fab-db", "reset", "--yes"]))

            mock_resetdb.assert_called_once_with(skip_init=False)

        @mock.patch.object(FABDBManager, "resetdb")
        def test_cli_resetdb_skip_init(self, mock_resetdb):
            db_command.resetdb(self.parser.parse_args(["fab-db", "reset", "--yes", "--skip-init"]))
            mock_resetdb.assert_called_once_with(skip_init=True)


except (ModuleNotFoundError, ImportError):
    pass
