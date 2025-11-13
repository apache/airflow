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

import os
from unittest import mock

import pytest

from airflow.cli import cli_parser
from airflow.cli.commands import dag_processor_command

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test


class TestDagProcessorCommand:
    """
    Tests the CLI interface and that it correctly calls the DagProcessor
    """

    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    @conf_vars({("core", "load_examples"): "False"})
    @mock.patch("airflow.cli.commands.dag_processor_command.DagProcessorJobRunner")
    def test_start_job(self, mock_runner):
        """Ensure that DagProcessorJobRunner is started"""
        mock_runner.return_value.job_type = "DagProcessorJob"
        args = self.parser.parse_args(["dag-processor"])
        dag_processor_command.dag_processor(args)
        mock_runner.return_value._execute.assert_called()

    @conf_vars({("core", "load_examples"): "False"})
    @mock.patch("airflow.cli.commands.dag_processor_command.DagProcessorJobRunner")
    def test_bundle_names_passed(self, mock_runner, configure_testing_dag_bundle):
        mock_runner.return_value.job_type = "DagProcessorJob"
        args = self.parser.parse_args(["dag-processor", "--bundle-name", "testing"])
        with configure_testing_dag_bundle(os.devnull):
            dag_processor_command.dag_processor(args)
        assert mock_runner.call_args.kwargs["processor"].bundle_names_to_parse == ["testing"]

    @mock.patch("airflow.cli.hot_reload.run_with_reloader")
    def test_dag_processor_with_dev_flag(self, mock_reloader):
        """Ensure that dag-processor with --dev flag uses hot-reload"""
        args = self.parser.parse_args(["dag-processor", "--dev"])
        dag_processor_command.dag_processor(args)

        # Verify that run_with_reloader was called
        mock_reloader.assert_called_once()
        # The callback function should be callable
        assert callable(mock_reloader.call_args[0][0])
