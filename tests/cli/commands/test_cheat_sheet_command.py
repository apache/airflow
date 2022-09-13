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
import io
import unittest
from unittest import mock

from airflow.cli import cli_parser
from airflow.cli.cli_parser import ActionCommand, CLICommand, GroupCommand


def noop():
    pass


MOCK_COMMANDS: list[CLICommand] = [
    GroupCommand(
        name='cmd_a',
        help='Help text A',
        subcommands=[
            ActionCommand(
                name='cmd_b',
                help='Help text B',
                func=noop,
                args=(),
            ),
            ActionCommand(
                name='cmd_c',
                help='Help text C',
                func=noop,
                args=(),
            ),
        ],
    ),
    GroupCommand(
        name='cmd_e',
        help='Help text E',
        subcommands=[
            ActionCommand(
                name='cmd_f',
                help='Help text F',
                func=noop,
                args=(),
            ),
            ActionCommand(
                name='cmd_g',
                help='Help text G',
                func=noop,
                args=(),
            ),
        ],
    ),
    ActionCommand(
        name='cmd_b',
        help='Help text D',
        func=noop,
        args=(),
    ),
]

ALL_COMMANDS = """\
airflow cmd_b                             | Help text D
"""

SECTION_A = """\
airflow cmd_a cmd_b                       | Help text B
airflow cmd_a cmd_c                       | Help text C
"""

SECTION_E = """\
airflow cmd_e cmd_f                       | Help text F
airflow cmd_e cmd_g                       | Help text G
"""


class TestCheatSheetCommand(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli_parser.get_parser()

    @mock.patch('airflow.cli.cli_parser.airflow_commands', MOCK_COMMANDS)
    def test_should_display_index(self):
        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            args = self.parser.parse_args(['cheat-sheet'])
            args.func(args)
        output = temp_stdout.getvalue()
        assert ALL_COMMANDS in output
        assert SECTION_A in output
        assert SECTION_E in output
