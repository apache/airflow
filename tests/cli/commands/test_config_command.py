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
import textwrap
import unittest
from unittest import mock

from airflow.bin import cli
from airflow.cli.commands import config_command


class TestCliConfig(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli.CLIFactory.get_parser()

    @mock.patch("airflow.cli.commands.config_command.conf")
    def test_cli_initdb(self, mock_conf):
        mock_conf.as_dict.return_value = {
            "SECTION1": {"KEY1": "VAL1", "KEY2": "VAL2"},
            "SECTION2": {"KEY3": "VAL3", "KEY4": "VAL4"},
        }
        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            config_command.show_config(self.parser.parse_args(['config']))

        stdout = temp_stdout.getvalue()
        mock_conf.as_dict.assert_called_once_with(display_sensitive=True, raw=True)
        ini_file = textwrap.dedent("""
        [SECTION1]
        KEY1=VAL1
        KEY2=VAL2

        [SECTION2]
        KEY3=VAL3
        KEY4=VAL4
        """).strip()
        self.assertIn(ini_file, stdout)
