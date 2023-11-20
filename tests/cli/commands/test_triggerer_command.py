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
from __future__ import annotations

from unittest import mock

import pytest

from airflow.cli import cli_parser
from airflow.cli.commands import triggerer_command

pytestmark = pytest.mark.db_test


class TestTriggererCommand:
    """
    Tests the CLI interface and that it correctly calls the TriggererJobRunner
    """

    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    @mock.patch("airflow.cli.commands.triggerer_command.TriggererJobRunner")
    @mock.patch("airflow.cli.commands.triggerer_command._serve_logs")
    def test_capacity_argument(
        self,
        mock_serve,
        mock_triggerer_job_runner,
    ):
        """Ensure that the capacity argument is passed correctly"""
        mock_triggerer_job_runner.return_value.job_type = "TriggererJob"
        args = self.parser.parse_args(["triggerer", "--capacity=42"])
        triggerer_command.triggerer(args)
        mock_serve.return_value.__enter__.assert_called_once()
        mock_serve.return_value.__exit__.assert_called_once()
        mock_triggerer_job_runner.assert_called_once_with(job=mock.ANY, capacity=42)
