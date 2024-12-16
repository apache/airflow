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

import contextlib
from argparse import ArgumentError
from io import StringIO
from unittest.mock import MagicMock

import pytest

from airflow.cli import cli_parser
from airflow.cli.commands.legacy_commands import COMMAND_MAP, check_legacy_command
from airflow.cli.commands.remote_commands import config_command

LEGACY_COMMANDS = [
    "worker",
    "flower",
    "trigger_dag",
    "delete_dag",
    "show_dag",
    "list_dag",
    "dag_status",
    "dags backfill",
    "list_dag_runs",
    "pause",
    "unpause",
    "test",
    "clear",
    "list_tasks",
    "task_failed_deps",
    "task_state",
    "run",
    "render",
    "initdb",
    "resetdb",
    "upgradedb",
    "checkdb",
    "shell",
    "pool",
    "list_users",
    "create_user",
    "delete_user",
]


class TestCliDeprecatedCommandsValue:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    def test_should_display_value(self):
        with pytest.raises(SystemExit) as ctx, contextlib.redirect_stderr(StringIO()) as temp_stderr:
            config_command.get_value(self.parser.parse_args(["worker"]))

        assert ctx.value.code == 2
        assert (
            "Command `airflow worker` has been removed. "
            "Please use `airflow celery worker`" in temp_stderr.getvalue().strip()
        )

    def test_command_map(self):
        for item in LEGACY_COMMANDS:
            assert COMMAND_MAP[item] is not None

    def test_check_legacy_command(self):
        mock_action = MagicMock()
        mock_action._prog_prefix = "airflow"
        with pytest.raises(
            ArgumentError,
            match="argument : Command `airflow list_users` has been removed. "
            "Please use `airflow users list`",
        ):
            check_legacy_command(mock_action, "list_users")
