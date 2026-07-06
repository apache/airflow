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

from tests_common.test_utils.config import conf_vars

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
        mock_triggerer_job_runner.assert_called_once_with(
            job=mock.ANY, capacity=42, queues=None, team_name=None
        )

    @conf_vars({("triggerer", "queues_enabled"): "True"})
    @mock.patch("airflow.cli.commands.triggerer_command.TriggererJobRunner")
    @mock.patch("airflow.cli.commands.triggerer_command._serve_logs")
    def test_queues_argument(self, mock_serve, mock_triggerer_job_runner):
        """Ensure that the queues argument is passed correctly"""
        mock_triggerer_job_runner.return_value.job_type = "TriggererJob"
        args = self.parser.parse_args(["triggerer", "--capacity=4", "--queues=my_queue,other_queue"])
        triggerer_command.triggerer(args)
        mock_serve.return_value.__enter__.assert_called_once()
        mock_serve.return_value.__exit__.assert_called_once()
        mock_triggerer_job_runner.assert_called_once_with(
            job=mock.ANY, capacity=4, queues=set(["my_queue", "other_queue"]), team_name=None
        )

    @mock.patch("airflow.cli.commands.triggerer_command.TriggererJobRunner")
    @mock.patch("airflow.cli.commands.triggerer_command.run_job")
    @mock.patch("airflow.cli.commands.triggerer_command.Process")
    def test_trigger_run_serve_logs(self, mock_process, mock_run_job, mock_trigger_job_runner):
        """Ensure that trigger runner and server log functions execute as intended"""
        triggerer_command.triggerer_run(
            skip_serve_logs=False, capacity=1, triggerer_heartrate=10.3, queues=None
        )

        mock_process.assert_called_once()
        mock_run_job.assert_called_once_with(
            job=mock_trigger_job_runner.return_value.job,
            execute_callable=mock_trigger_job_runner.return_value._execute,
        )

    @mock.patch("airflow.cli.hot_reload.run_with_reloader")
    def test_triggerer_with_dev_flag(self, mock_reloader):
        """Ensure that triggerer with --dev flag uses hot-reload"""
        args = self.parser.parse_args(["triggerer", "--dev"])
        triggerer_command.triggerer(args)

        # Verify that run_with_reloader was called
        mock_reloader.assert_called_once()
        # The callback function should be callable
        assert callable(mock_reloader.call_args[0][0])

    @conf_vars({("core", "multi_team"): "False"})
    def test_team_name_rejected_when_multi_team_disabled(self):
        """--team-name should raise when core.multi_team is disabled"""
        from airflow.exceptions import AirflowConfigException

        args = self.parser.parse_args(["triggerer", "--team-name", "team_a"])
        with pytest.raises(AirflowConfigException, match="--team-name.*core.multi_team"):
            triggerer_command.triggerer(args)

    @conf_vars({("core", "multi_team"): "True"})
    @mock.patch("airflow.models.team.Team.get_name_if_exists", return_value=None)
    def test_team_name_rejected_when_team_does_not_exist(self, mock_get_team):
        """--team-name should raise when the specified team doesn't exist in DB"""
        from airflow.exceptions import AirflowConfigException

        args = self.parser.parse_args(["triggerer", "--team-name", "nonexistent_team"])
        with pytest.raises(AirflowConfigException, match="does not exist"):
            triggerer_command.triggerer(args)
        mock_get_team.assert_called_once_with("nonexistent_team")

    @conf_vars({("core", "multi_team"): "True"})
    @mock.patch("airflow.models.team.Team.get_name_if_exists", return_value="team_a")
    @mock.patch("airflow.cli.commands.triggerer_command.TriggererJobRunner")
    @mock.patch("airflow.cli.commands.triggerer_command._serve_logs")
    def test_team_name_passed_through(self, mock_serve, mock_triggerer_job_runner, mock_get_team):
        """--team-name should be passed to TriggererJobRunner when valid"""
        mock_triggerer_job_runner.return_value.job_type = "TriggererJob"
        args = self.parser.parse_args(["triggerer", "--team-name", "team_a"])
        triggerer_command.triggerer(args)
        mock_triggerer_job_runner.assert_called_once_with(
            job=mock.ANY, capacity=mock.ANY, queues=None, team_name="team_a"
        )

    @conf_vars({("core", "multi_team"): "False"})
    @mock.patch("airflow.cli.commands.triggerer_command.TriggererJobRunner")
    @mock.patch("airflow.cli.commands.triggerer_command._serve_logs")
    def test_no_team_name_passes_none(self, mock_serve, mock_triggerer_job_runner):
        """Without --team-name, team_name=None is passed"""
        mock_triggerer_job_runner.return_value.job_type = "TriggererJob"
        args = self.parser.parse_args(["triggerer"])
        triggerer_command.triggerer(args)
        mock_triggerer_job_runner.assert_called_once_with(
            job=mock.ANY, capacity=mock.ANY, queues=None, team_name=None
        )
