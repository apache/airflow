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


def _make_jobs(count: int) -> SimpleNamespace:
    """Build a stand-in for the ``JobCollectionResponse`` of ``count`` already-alive jobs."""
    return SimpleNamespace(jobs=[SimpleNamespace(id=i) for i in range(count)])


class TestCliJobsCheck:
    parser = cli_parser.get_parser()

    @pytest.mark.parametrize(
        ("argv", "alive_count", "expected_output"),
        [
            (["jobs", "check", "--job-type", "SchedulerJob"], 1, "Found one alive job."),
            (["jobs", "check", "--limit", "100", "--allow-multiple"], 3, "Found 3 alive jobs."),
            (["jobs", "check", "--limit", "0", "--allow-multiple"], 3, "Found 3 alive jobs."),
            (["jobs", "check", "--limit", "1"], 5, "Found one alive job."),
        ],
    )
    def test_reports_alive_jobs(self, capsys, argv, alive_count, expected_output):
        api_client = mock.MagicMock()
        api_client.jobs.list.return_value = _make_jobs(alive_count)

        jobs_command.check(self.parser.parse_args(argv), api_client=api_client)

        assert expected_output in capsys.readouterr().out

    @pytest.mark.parametrize(
        ("argv", "local_hostname", "expected_kwargs"),
        [
            (
                ["jobs", "check", "--job-type", "SchedulerJob"],
                None,
                {"job_type": "SchedulerJob", "hostname": None, "is_alive": True},
            ),
            (
                ["jobs", "check", "--hostname", "HOSTNAME"],
                None,
                {"job_type": None, "hostname": "HOSTNAME", "is_alive": True},
            ),
            (
                ["jobs", "check", "--local"],
                "local-host",
                {"job_type": None, "hostname": "local-host", "is_alive": True},
            ),
        ],
    )
    def test_forwards_filters_to_jobs_list(self, argv, local_hostname, expected_kwargs):
        api_client = mock.MagicMock()
        api_client.jobs.list.return_value = _make_jobs(1)

        with mock.patch("airflowctl.ctl.commands.jobs_command.socket.getfqdn", return_value=local_hostname):
            jobs_command.check(self.parser.parse_args(argv), api_client=api_client)

        api_client.jobs.list.assert_called_once_with(**expected_kwargs)

    @pytest.mark.parametrize(
        ("argv", "alive_count", "expected_error"),
        [
            (["jobs", "check"], 0, r"No alive jobs found."),
            (["jobs", "check", "--limit", "100"], 3, r"Found 3 alive jobs. Expected only one."),
        ],
    )
    def test_exits_when_alive_count_unexpected(self, argv, alive_count, expected_error):
        api_client = mock.MagicMock()
        api_client.jobs.list.return_value = _make_jobs(alive_count)

        with pytest.raises(SystemExit, match=expected_error):
            jobs_command.check(self.parser.parse_args(argv), api_client=api_client)

    @pytest.mark.parametrize(
        ("argv", "expected_error"),
        [
            (
                ["jobs", "check", "--allow-multiple"],
                r"To use option --allow-multiple, you must set the limit to a value greater than 1 "
                r"or 0 to disable it.",
            ),
            (
                ["jobs", "check", "--hostname", "h", "--local"],
                r"You can't use --hostname and --local at the same time",
            ),
        ],
    )
    def test_rejects_invalid_argument_combinations(self, argv, expected_error):
        api_client = mock.MagicMock()

        with pytest.raises(SystemExit, match=expected_error):
            jobs_command.check(self.parser.parse_args(argv), api_client=api_client)
        api_client.jobs.list.assert_not_called()
