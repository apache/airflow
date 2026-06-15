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

from airflowctl.ctl import cli_parser
from airflowctl.ctl.commands import jobs_command


def _jobs(count: int) -> SimpleNamespace:
    """Stand in for the ``JobCollectionResponse`` of ``count`` already-alive jobs."""
    return SimpleNamespace(jobs=[SimpleNamespace(id=i) for i in range(count)])


class TestCliJobsCheck:
    parser = cli_parser.get_parser()

    def test_reports_one_alive_job(self, capsys):
        api_client = mock.MagicMock()
        api_client.jobs.list.return_value = _jobs(1)

        jobs_command.check(
            self.parser.parse_args(["jobs", "check", "--job-type", "SchedulerJob"]),
            api_client=api_client,
        )

        assert "Found one alive job." in capsys.readouterr().out
        api_client.jobs.list.assert_called_once_with(job_type="SchedulerJob", hostname=None, is_alive=True)

    def test_forwards_hostname(self, capsys):
        api_client = mock.MagicMock()
        api_client.jobs.list.return_value = _jobs(1)

        jobs_command.check(
            self.parser.parse_args(["jobs", "check", "--hostname", "HOSTNAME"]),
            api_client=api_client,
        )

        api_client.jobs.list.assert_called_once_with(job_type=None, hostname="HOSTNAME", is_alive=True)

    def test_reports_multiple_with_allow_multiple(self, capsys):
        api_client = mock.MagicMock()
        api_client.jobs.list.return_value = _jobs(3)

        jobs_command.check(
            self.parser.parse_args(["jobs", "check", "--limit", "100", "--allow-multiple"]),
            api_client=api_client,
        )

        assert "Found 3 alive jobs." in capsys.readouterr().out

    def test_no_alive_jobs(self):
        api_client = mock.MagicMock()
        api_client.jobs.list.return_value = _jobs(0)

        with pytest.raises(SystemExit, match=r"No alive jobs found."):
            jobs_command.check(self.parser.parse_args(["jobs", "check"]), api_client=api_client)

    def test_multiple_without_allow_multiple_fails(self):
        api_client = mock.MagicMock()
        api_client.jobs.list.return_value = _jobs(3)

        with pytest.raises(SystemExit, match=r"Found 3 alive jobs. Expected only one."):
            jobs_command.check(
                self.parser.parse_args(["jobs", "check", "--limit", "100"]), api_client=api_client
            )

    def test_allow_multiple_requires_limit_above_one(self):
        api_client = mock.MagicMock()

        with pytest.raises(
            SystemExit,
            match=r"To use option --allow-multiple, you must set the limit to a value greater than 1.",
        ):
            jobs_command.check(
                self.parser.parse_args(["jobs", "check", "--allow-multiple"]), api_client=api_client
            )
        api_client.jobs.list.assert_not_called()

    def test_hostname_and_local_are_mutually_exclusive(self):
        api_client = mock.MagicMock()

        with pytest.raises(SystemExit, match=r"You can't use --hostname and --local at the same time"):
            jobs_command.check(
                self.parser.parse_args(["jobs", "check", "--hostname", "h", "--local"]),
                api_client=api_client,
            )
        api_client.jobs.list.assert_not_called()

    def test_local_resolves_hostname(self):
        api_client = mock.MagicMock()
        api_client.jobs.list.return_value = _jobs(1)

        with mock.patch("airflowctl.ctl.commands.jobs_command.socket.getfqdn", return_value="local-host"):
            jobs_command.check(self.parser.parse_args(["jobs", "check", "--local"]), api_client=api_client)

        api_client.jobs.list.assert_called_once_with(job_type=None, hostname="local-host", is_alive=True)

    def test_limit_is_applied_client_side(self, capsys):
        api_client = mock.MagicMock()
        api_client.jobs.list.return_value = _jobs(5)

        jobs_command.check(self.parser.parse_args(["jobs", "check", "--limit", "1"]), api_client=api_client)

        assert "Found one alive job." in capsys.readouterr().out
