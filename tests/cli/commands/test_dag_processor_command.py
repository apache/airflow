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
from airflow.cli.commands import dag_processor_command
from airflow.configuration import conf

from tests_common.test_utils.config import conf_vars

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]


class TestDagProcessorCommand:
    """
    Tests the CLI interface and that it correctly calls the DagProcessor
    """

    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    @conf_vars(
        {
            ("scheduler", "standalone_dag_processor"): "True",
            ("core", "load_examples"): "False",
        }
    )
    @mock.patch("airflow.cli.commands.dag_processor_command.DagProcessorJobRunner")
    @pytest.mark.skipif(
        conf.get_mandatory_value("database", "sql_alchemy_conn")
        .lower()
        .startswith("sqlite"),
        reason="Standalone Dag Processor doesn't support sqlite.",
    )
    def test_start_job(
        self,
        mock_dag_job,
    ):
        """Ensure that DagProcessorJobRunner is started"""
        with conf_vars({("scheduler", "standalone_dag_processor"): "True"}):
            mock_dag_job.return_value.job_type = "DagProcessorJob"
            args = self.parser.parse_args(["dag-processor"])
            dag_processor_command.dag_processor(args)
            mock_dag_job.return_value._execute.assert_called()
