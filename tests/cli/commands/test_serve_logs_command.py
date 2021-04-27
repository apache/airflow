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
#
import unittest
from unittest import mock

from airflow.cli import cli_parser
from airflow.cli.commands import serve_logs_command
from airflow.utils.serve_logs import serve_logs


class TestCliServeLogs(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli_parser.get_parser()

    @mock.patch("airflow.cli.commands.serve_logs_command.Process")
    def test_cli_sync_perm(self, mock_process):
        args = self.parser.parse_args(['serve-logs'])
        serve_logs_command.serve_logs(args)
        mock_process.assert_called_once_with(target=serve_logs)
