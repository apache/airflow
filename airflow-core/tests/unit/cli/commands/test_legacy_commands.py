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

from argparse import ArgumentError
from unittest.mock import MagicMock

import pytest

from airflow.cli import cli_parser
from airflow.cli.commands import config_command
from airflow.cli.commands.legacy_commands import check_legacy_command


class TestCliDeprecatedCommandsValue:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    def test_should_display_value(self, stderr_capture):
        with pytest.raises(SystemExit) as ctx, stderr_capture as temp_stderr:
            config_command.get_value(self.parser.parse_args(["webserver"]))

        assert ctx.value.code == 2
        assert (
            "Command `airflow webserver` has been removed. "
            "Please use `airflow api-server`" in temp_stderr.getvalue().strip()
        )

    def test_check_legacy_command(self):
        mock_action = MagicMock()
        mock_action._prog_prefix = "airflow"
        with pytest.raises(
            ArgumentError,
            match="argument : Command `airflow webserver` has been removed. Please use `airflow api-server`",
        ):
            check_legacy_command(mock_action, "webserver")
