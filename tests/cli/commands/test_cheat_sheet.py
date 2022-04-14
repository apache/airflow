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

import contextlib
import io
import unittest
from typing import List

import rich_click as click

from airflow.cli.commands.cheat_sheet import display_recursive


@click.group()
def mock_cli():
    """Mock cli"""
    pass


@mock_cli.group()
def cmd_a():
    """Help text A"""
    pass


@cmd_a.command("cmd_b")
def cmd_b():
    """Help text B"""
    pass


@cmd_a.command("cmd_c")
def cmd_c():
    """Help text C"""
    pass


@mock_cli.group()
def cmd_e():
    """Help text E"""
    pass


@cmd_e.command("cmd_f")
def cmd_f():
    """Help text F"""
    pass


@cmd_e.command("cmd_g")
def cmd_g():
    """Help text G"""
    pass


@mock_cli.command()
def cmd_h():
    """Help text H"""
    pass


SECTION_MISC = """\
Miscellaneous commands                                 
airflow cmd-h                             | Help text H
"""

SECTION_A = """\
Help text A                                            
airflow cmd-a cmd_b                       | Help text B
airflow cmd-a cmd_c                       | Help text C
"""

SECTION_E = """\
Help text E                                            
airflow cmd-e cmd_f                       | Help text F
airflow cmd-e cmd_g                       | Help text G
"""


class TestCheatSheet(unittest.TestCase):
    def test_display_recursive_commands(self):
        with contextlib.redirect_stdout(io.StringIO()) as stdout:
            display_recursive(["airflow"], mock_cli)
        output = stdout.getvalue()
        assert SECTION_MISC in output
        assert SECTION_A in output
        assert SECTION_E in output
