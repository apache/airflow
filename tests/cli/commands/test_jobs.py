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
import unittest

import pytest
from click.testing import CliRunner

from airflow.cli.commands import jobs
from airflow.jobs.scheduler_job import SchedulerJob
from airflow.utils.session import create_session
from airflow.utils.state import State
from tests.test_utils.db import clear_db_jobs


class TestCliConfigList(unittest.TestCase):
    def setUp(self) -> None:
        clear_db_jobs()
        self.scheduler_job = None

    def tearDown(self) -> None:
        if self.scheduler_job and self.scheduler_job.processor_agent:
            self.scheduler_job.processor_agent.end()
        clear_db_jobs()

    def test_should_report_success_for_one_working_scheduler(self):
        with create_session() as session:
            self.scheduler_job = SchedulerJob()
            self.scheduler_job.state = State.RUNNING
            session.add(self.scheduler_job)
            session.commit()
            self.scheduler_job.heartbeat()

        runner = CliRunner()
        result = runner.invoke(jobs.check, ["--job-type", "SchedulerJob"])
        self.assertIn("Found one alive job.", result.output)

    def test_should_report_success_for_one_working_scheduler_with_hostname(self):
        with create_session() as session:
            self.scheduler_job = SchedulerJob()
            self.scheduler_job.state = State.RUNNING
            self.scheduler_job.hostname = 'HOSTNAME'
            session.add(self.scheduler_job)
            session.commit()
            self.scheduler_job.heartbeat()

        runner = CliRunner()
        result = runner.invoke(jobs.check, ["--job-type", "SchedulerJob", "--hostname", "HOSTNAME"])
        self.assertIn("Found one alive job.", result.output)

    def test_should_report_success_for_ha_schedulers(self):
        scheduler_jobs = []
        with create_session() as session:
            for _ in range(3):
                scheduler_job = SchedulerJob()
                scheduler_job.state = State.RUNNING
                session.add(scheduler_job)
                scheduler_jobs.append(scheduler_job)
            session.commit()
            scheduler_job.heartbeat()

        runner = CliRunner()
        result = runner.invoke(
            jobs.check, ["--job-type", "SchedulerJob", "--limit", "100", "--allow-multiple"]
        )
        self.assertIn("Found 3 alive jobs.", result.output)
        for scheduler_job in scheduler_jobs:
            if scheduler_job.processor_agent:
                scheduler_job.processor_agent.end()

    def test_should_ignore_not_running_jobs(self):
        scheduler_jobs = []
        with create_session() as session:
            for _ in range(3):
                scheduler_job = SchedulerJob()
                scheduler_job.state = State.SHUTDOWN
                session.add(scheduler_job)
                scheduler_jobs.append(scheduler_job)
            session.commit()
        # No alive jobs found.
        runner = CliRunner()
        result = runner.invoke(jobs.check)
        assert isinstance(result.exception, SystemExit)
        assert "No alive jobs found." in result.output
        for scheduler_job in scheduler_jobs:
            if scheduler_job.processor_agent:
                scheduler_job.processor_agent.end()

    def test_should_raise_exception_for_multiple_scheduler_on_one_host(self):
        scheduler_jobs = []
        with create_session() as session:
            for _ in range(3):
                scheduler_job = SchedulerJob()
                scheduler_job.state = State.RUNNING
                scheduler_job.hostname = 'HOSTNAME'
                session.add(scheduler_job)
            session.commit()
            scheduler_job.heartbeat()

        runner = CliRunner()
        result = runner.invoke(jobs.check, ["--job-type", "SchedulerJob", "--limit", "100"])
        assert isinstance(result.exception, SystemExit)
        assert "Found 3 alive jobs. Expected only one." in result.output

        for scheduler_job in scheduler_jobs:
            if scheduler_job.processor_agent:
                scheduler_job.processor_agent.end()

    def test_should_raise_exception_for_allow_multiple_and_limit_1(self):
        runner = CliRunner()
        result = runner.invoke(jobs.check, ["--allow-multiple"])
        assert isinstance(result.exception, SystemExit)
        assert (
            "To use option --allow-multiple, you must set the limit to a value greater than 1."
            in result.output
        )
