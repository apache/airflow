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

import pytest

from airflow.cli import cli_parser
from airflow.cli.commands import jobs_command
from airflow.jobs.scheduler_job import SchedulerJob
from airflow.utils.session import create_session
from airflow.utils.state import State
from tests.test_utils.db import clear_db_jobs


class TestCliConfigList(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli_parser.get_parser()

    def setUp(self) -> None:
        clear_db_jobs()

    def tearDown(self) -> None:
        clear_db_jobs()

    def test_should_report_success_for_one_working_scheduler(self):
        with create_session() as session:
            job = SchedulerJob()
            job.state = State.RUNNING
            session.add(job)
            session.commit()
            job.heartbeat()

        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            jobs_command.check(self.parser.parse_args(['jobs', 'check', '--job-type', 'SchedulerJob']))
        self.assertIn("Found one alive job.", temp_stdout.getvalue())

    def test_should_report_success_for_one_working_scheduler_with_hostname(self):
        with create_session() as session:
            job = SchedulerJob()
            job.state = State.RUNNING
            job.hostname = 'HOSTNAME'
            session.add(job)
            session.commit()
            job.heartbeat()

        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            jobs_command.check(
                self.parser.parse_args(
                    ['jobs', 'check', '--job-type', 'SchedulerJob', '--hostname', 'HOSTNAME']
                )
            )
        self.assertIn("Found one alive job.", temp_stdout.getvalue())

    def test_should_report_success_for_ha_schedulers(self):
        with create_session() as session:
            for _ in range(3):
                job = SchedulerJob()
                job.state = State.RUNNING
                session.add(job)
            session.commit()
            job.heartbeat()

        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            jobs_command.check(
                self.parser.parse_args(
                    ['jobs', 'check', '--job-type', 'SchedulerJob', '--limit', '100', '--allow-multiple']
                )
            )
        self.assertIn("Found 3 alive jobs.", temp_stdout.getvalue())

    def test_should_ignore_not_running_jobs(self):
        with create_session() as session:
            for _ in range(3):
                job = SchedulerJob()
                job.state = State.SHUTDOWN
                session.add(job)
            session.commit()
        # No alive jobs found.
        with pytest.raises(SystemExit, match=r"No alive jobs found."):
            jobs_command.check(self.parser.parse_args(['jobs', 'check']))

    def test_should_raise_exception_for_multiple_scheduler_on_one_host(self):
        with create_session() as session:
            for _ in range(3):
                job = SchedulerJob()
                job.state = State.RUNNING
                job.hostname = 'HOSTNAME'
                session.add(job)
            session.commit()
            job.heartbeat()

        with pytest.raises(SystemExit, match=r"Found 3 alive jobs. Expected only one."):
            jobs_command.check(
                self.parser.parse_args(
                    [
                        'jobs',
                        'check',
                        '--job-type',
                        'SchedulerJob',
                        '--limit',
                        '100',
                    ]
                )
            )

    def test_should_raise_exception_for_allow_multiple_and_limit_1(self):
        with pytest.raises(
            SystemExit,
            match=r"To use option --allow-multiple, you must set the limit to a value greater than 1.",
        ):
            jobs_command.check(self.parser.parse_args(['jobs', 'check', '--allow-multiple']))
