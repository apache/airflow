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

from types import SimpleNamespace
from unittest import mock

import pytest

from airflow.cli import cli_parser
from airflow.cli.commands import jobs_command


def _jobs(count: int) -> SimpleNamespace:
    """Stand in for the airflowctl ``JobCollectionResponse`` of ``count`` already-alive jobs."""
    return SimpleNamespace(jobs=[SimpleNamespace(id=i) for i in range(count)])


class TestCliJobsCheck:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    def test_reports_one_alive_job(self, mock_cli_api_client, stdout_capture):
        mock_cli_api_client.jobs.list.return_value = _jobs(1)
        with stdout_capture as temp_stdout:
            jobs_command.check(self.parser.parse_args(["jobs", "check", "--job-type", "SchedulerJob"]))
        assert "Found one alive job." in temp_stdout.getvalue()
        mock_cli_api_client.jobs.list.assert_called_once_with(
            job_type="SchedulerJob", hostname=None, is_alive=True
        )

    def test_forwards_hostname(self, mock_cli_api_client, stdout_capture):
        mock_cli_api_client.jobs.list.return_value = _jobs(1)
        with stdout_capture as temp_stdout:
            jobs_command.check(
                self.parser.parse_args(
                    ["jobs", "check", "--job-type", "SchedulerJob", "--hostname", "HOSTNAME"]
                )
            )
        assert "Found one alive job." in temp_stdout.getvalue()
        mock_cli_api_client.jobs.list.assert_called_once_with(
            job_type="SchedulerJob", hostname="HOSTNAME", is_alive=True
        )

    def test_reports_multiple_with_allow_multiple(self, mock_cli_api_client, stdout_capture):
        mock_cli_api_client.jobs.list.return_value = _jobs(3)
        with stdout_capture as temp_stdout:
            jobs_command.check(
                self.parser.parse_args(
                    ["jobs", "check", "--job-type", "SchedulerJob", "--limit", "100", "--allow-multiple"]
                )
            )
        assert "Found 3 alive jobs." in temp_stdout.getvalue()

    def test_no_alive_jobs(self, mock_cli_api_client):
        mock_cli_api_client.jobs.list.return_value = _jobs(0)
        with pytest.raises(SystemExit, match=r"No alive jobs found."):
            jobs_command.check(self.parser.parse_args(["jobs", "check"]))

    def test_multiple_without_allow_multiple_fails(self, mock_cli_api_client):
        mock_cli_api_client.jobs.list.return_value = _jobs(3)
        with pytest.raises(SystemExit, match=r"Found 3 alive jobs. Expected only one."):
            jobs_command.check(
                self.parser.parse_args(
                    [
                        "jobs",
                        "check",
                        "--job-type",
                        "SchedulerJob",
                        "--limit",
                        "100",
                    ]
                )
            )

    def test_allow_multiple_requires_limit_above_one(self, mock_cli_api_client):
        with pytest.raises(
            SystemExit,
            match=r"To use option --allow-multiple, you must set the limit to a value greater than 1.",
        ):
            jobs_command.check(self.parser.parse_args(["jobs", "check", "--allow-multiple"]))
        mock_cli_api_client.jobs.list.assert_not_called()

    def test_hostname_and_local_are_mutually_exclusive(self, mock_cli_api_client):
        with pytest.raises(SystemExit, match=r"You can't use --hostname and --local at the same time"):
            jobs_command.check(self.parser.parse_args(["jobs", "check", "--hostname", "h", "--local"]))
        mock_cli_api_client.jobs.list.assert_not_called()

    def test_local_resolves_hostname(self, mock_cli_api_client, stdout_capture):
        mock_cli_api_client.jobs.list.return_value = _jobs(1)
        with mock.patch("airflow.cli.commands.jobs_command.get_hostname", return_value="local-host"):
            jobs_command.check(self.parser.parse_args(["jobs", "check", "--local"]))
        mock_cli_api_client.jobs.list.assert_called_once_with(
            job_type=None, hostname="local-host", is_alive=True
        )

    def test_limit_is_applied_client_side(self, mock_cli_api_client, stdout_capture):
        # The API returns all alive jobs (heartbeat desc); --limit trims to the most recent N.
        mock_cli_api_client.jobs.list.return_value = _jobs(5)
        with stdout_capture as stdout:
            jobs_command.check(self.parser.parse_args(["jobs", "check", "--limit", "1"]))
        assert "Found one alive job." in stdout.getvalue()
