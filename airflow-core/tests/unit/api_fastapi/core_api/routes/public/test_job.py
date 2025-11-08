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

from typing import TYPE_CHECKING

import pytest

from airflow.jobs.job import Job, JobState
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.utils.session import provide_session
from airflow.utils.state import State

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.db import clear_db_jobs
from tests_common.test_utils.format_datetime import from_datetime_to_zulu

if TYPE_CHECKING:
    from typing import Literal

    TestCase = Literal[
        "should_report_success_for_one_working_scheduler",
        "should_report_success_for_one_working_scheduler_with_hostname",
        "should_report_success_for_ha_schedulers",
        "should_ignore_not_running_jobs",
        "should_raise_exception_for_multiple_scheduler_on_one_host",
    ]

pytestmark = pytest.mark.db_test

TESTCASE_ONE_SCHEDULER = "should_report_success_for_one_working_scheduler"
TESTCASE_ONE_SCHEDULER_WITH_HOSTNAME = "should_report_success_for_one_working_scheduler_with_hostname"
TESTCASE_HA_SCHEDULERS = "should_report_success_for_ha_schedulers"
TESTCASE_IGNORE_NOT_RUNNING = "should_ignore_not_running_jobs"
TESTCASE_MULTIPLE_SCHEDULERS_ON_ONE_HOST = "should_raise_exception_for_multiple_scheduler_on_one_host"
TESTCASE_MULTIPLE_RUNNER = "should_report_success_for_multiple_runners"


class TestJobEndpoint:
    """Common class for /jobs related unit tests."""

    scheduler_jobs: list[Job] | None = None
    job_runners: list[SchedulerJobRunner] | None = None

    def _setup_should_report_success_for_one_working_scheduler(self, session=None):
        scheduler_job = Job()
        job_runner = SchedulerJobRunner(job=scheduler_job)
        scheduler_job.state = State.RUNNING
        session.add(scheduler_job)
        session.commit()
        self.scheduler_jobs.append(scheduler_job)
        self.job_runners.append(job_runner)
        scheduler_job.heartbeat(heartbeat_callback=job_runner.heartbeat_callback)

    def _setup_should_report_success_for_one_working_scheduler_with_hostname(self, session=None):
        scheduler_job = Job()
        job_runner = SchedulerJobRunner(job=scheduler_job)
        scheduler_job.state = State.RUNNING
        scheduler_job.hostname = "HOSTNAME"
        session.add(scheduler_job)
        self.scheduler_jobs.append(scheduler_job)
        self.job_runners.append(job_runner)
        session.commit()
        scheduler_job.heartbeat(heartbeat_callback=job_runner.heartbeat_callback)

    def _setup_should_report_success_for_ha_schedulers(self, session=None):
        for _ in range(3):
            scheduler_job = Job()
            job_runner = SchedulerJobRunner(job=scheduler_job)
            scheduler_job.state = State.RUNNING
            session.add(scheduler_job)
            self.scheduler_jobs.append(scheduler_job)
            self.job_runners.append(job_runner)
        session.commit()
        scheduler_job.heartbeat(heartbeat_callback=job_runner.heartbeat_callback)

    def _setup_should_ignore_not_running_jobs(self, session=None):
        for _ in range(3):
            scheduler_job = Job()
            job_runner = SchedulerJobRunner(job=scheduler_job)
            scheduler_job.state = JobState.FAILED
            session.add(scheduler_job)
            self.scheduler_jobs.append(scheduler_job)
            self.job_runners.append(job_runner)
        session.commit()

    def _setup_should_raise_exception_for_multiple_scheduler_on_one_host(self, session=None):
        for _ in range(3):
            scheduler_job = Job()
            job_runner = SchedulerJobRunner(job=scheduler_job)
            job_runner.job = scheduler_job
            scheduler_job.state = State.RUNNING
            scheduler_job.hostname = "HOSTNAME"
            session.add(scheduler_job)
            self.scheduler_jobs.append(scheduler_job)
            self.job_runners.append(job_runner)
        session.commit()
        scheduler_job.heartbeat(heartbeat_callback=job_runner.heartbeat_callback)

    @provide_session
    def setup(self, testcase: TestCase, session=None) -> None:
        """
        Setup testcase at runtime based on the `testcase` provided by `pytest.mark.parametrize`.
        """
        clear_db_jobs()
        self.scheduler_jobs = []
        self.job_runners = []
        setup_method = getattr(self, f"_setup_{testcase}")
        setup_method(session)

    def teardown_method(self) -> None:
        clear_db_jobs()


class TestGetJobs(TestJobEndpoint):
    @pytest.mark.parametrize(
        ("testcase", "query_params", "expected_status_code", "expected_total_entries"),
        [
            # original testcases refactor from tests/cli/commands/test_jobs_command.py
            (TESTCASE_ONE_SCHEDULER, {}, 200, 1),
            (TESTCASE_ONE_SCHEDULER_WITH_HOSTNAME, {"hostname": "HOSTNAME"}, 200, 1),
            (TESTCASE_HA_SCHEDULERS, {"limit": 100}, 200, 3),
            (TESTCASE_IGNORE_NOT_RUNNING, {}, 200, 0),
            (TESTCASE_MULTIPLE_SCHEDULERS_ON_ONE_HOST, {"limit": 100}, 200, 3),
        ],
    )
    def test_get_jobs(
        self, test_client, testcase, query_params, expected_status_code, expected_total_entries
    ):
        # setup testcase at runtime based on the `testcase` parameter
        self.setup(testcase)
        with assert_queries_count(2):
            response = test_client.get("/jobs", params=query_params)
        assert response.status_code == expected_status_code
        if expected_status_code != 200:
            return
        response_json = response.json()
        assert response_json["total_entries"] == expected_total_entries

        for idx, resp_job in enumerate(response_json["jobs"]):
            expected_job = {
                "id": self.scheduler_jobs[idx].id,
                "dag_display_name": None,
                "dag_id": None,
                "state": "running",
                "job_type": "SchedulerJob",
                "start_date": from_datetime_to_zulu(self.scheduler_jobs[idx].start_date),
                "end_date": None,
                "latest_heartbeat": from_datetime_to_zulu(self.scheduler_jobs[idx].latest_heartbeat),
                "executor_class": None,
                "hostname": self.scheduler_jobs[idx].hostname,
                "unixname": self.scheduler_jobs[idx].unixname,
            }
            assert resp_job == expected_job

    def test_should_raises_401_unauthenticated(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/jobs")
        assert response.status_code == 401

    def test_should_raises_403_unauthorized(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/jobs")
        assert response.status_code == 403
