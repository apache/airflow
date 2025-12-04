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

import datetime
import logging
import sys
from unittest.mock import ANY, Mock, patch

import pytest
from sqlalchemy.exc import OperationalError

from airflow._shared.timezones import timezone
from airflow.executors.local_executor import LocalExecutor
from airflow.jobs.job import Job, most_recent_job, perform_heartbeat, run_job
from airflow.listeners import get_listener_manager
from airflow.utils.session import create_session
from airflow.utils.state import State

from tests_common.test_utils.config import conf_vars
from unit.listeners import lifecycle_listener
from unit.utils.test_helpers import MockJobRunner, SchedulerJobRunner, TriggererJobRunner

pytestmark = pytest.mark.db_test


class TestJob:
    def test_state_success(self):
        job = Job()
        job_runner = MockJobRunner(job=job)
        run_job(job=job, execute_callable=job_runner._execute)

        assert job.state == State.SUCCESS
        assert job.end_date is not None

    def test_state_sysexit(self):
        import sys

        job = Job()
        job_runner = MockJobRunner(job=job, func=lambda: sys.exit(0))
        run_job(job=job, execute_callable=job_runner._execute)

        assert job.state == State.SUCCESS
        assert job.end_date is not None

    def test_base_job_respects_plugin_hooks(self):
        import sys

        job = Job()
        job_runner = MockJobRunner(job=job, func=lambda: sys.exit(0))
        run_job(job=job, execute_callable=job_runner._execute)

        assert job.state == State.SUCCESS
        assert job.end_date is not None

    def test_base_job_respects_plugin_lifecycle(self, dag_maker):
        """
        Test if DagRun is successful, and if Success callbacks is defined, it is sent to DagFileProcessor.
        """
        get_listener_manager().add_listener(lifecycle_listener)

        job = Job()
        job_runner = MockJobRunner(job=job, func=lambda: sys.exit(0))
        run_job(job=job, execute_callable=job_runner._execute)

        assert lifecycle_listener.get_listener_state().started_component is job
        assert lifecycle_listener.get_listener_state().stopped_component is job

    def test_state_failed(self):
        def abort():
            raise RuntimeError("fail")

        job = Job()
        job_runner = MockJobRunner(job=job, func=abort)
        with pytest.raises(RuntimeError):
            run_job(job=job, execute_callable=job_runner._execute)

        assert job.state == State.FAILED
        assert job.end_date is not None

    @pytest.mark.parametrize(
        ("job_runner", "job_type", "job_heartbeat_sec"),
        [(SchedulerJobRunner, "scheduler", "11"), (TriggererJobRunner, "triggerer", "9")],
    )
    def test_heart_rate_after_fetched_from_db(self, job_runner, job_type, job_heartbeat_sec):
        """Ensure heartrate is set correctly after jobs are queried from the DB"""
        if job_type == "scheduler":
            config_name = "scheduler_heartbeat_sec"
        else:
            config_name = "job_heartbeat_sec"

        with create_session() as session, conf_vars({(job_type.lower(), config_name): job_heartbeat_sec}):
            job = Job()
            job_runner(job=job)
            session.add(job)
            session.commit()

            most_recent = most_recent_job(job_runner.job_type, session=session)
            assert most_recent.heartrate == float(job_heartbeat_sec)

            session.rollback()

    @pytest.mark.parametrize(
        ("job_runner", "job_type", "job_heartbeat_sec"),
        [(SchedulerJobRunner, "scheduler", "11"), (TriggererJobRunner, "triggerer", "9")],
    )
    def test_heart_rate_via_constructor_persists(self, job_runner, job_type, job_heartbeat_sec):
        """Ensure heartrate passed via constructor is set correctly"""
        with conf_vars({(job_type.lower(), "job_heartbeat_sec"): job_heartbeat_sec}):
            job = Job(heartrate=12)
            job_runner(job)
            # heartrate should be 12 since we passed that to the constructor directly
            assert job.heartrate == 12

    def _compare_jobs(self, job1: Job, job2: Job):
        """Helper to compare two jobs where one can by Pydantic and the other not."""
        assert job1.id == job2.id
        assert job1.dag_id == job2.dag_id
        assert job1.state == job2.state
        assert job1.job_type == job2.job_type
        assert job1.start_date == job2.start_date
        assert job1.end_date == job2.end_date
        assert job1.latest_heartbeat == job2.latest_heartbeat
        assert job1.executor_class == job2.executor_class
        assert job1.hostname == job2.hostname
        assert job1.unixname == job2.unixname

    def test_most_recent_job(self):
        with create_session() as session:
            old_job = Job(heartrate=10)
            MockJobRunner(job=old_job)
            old_job.latest_heartbeat = old_job.latest_heartbeat - datetime.timedelta(seconds=20)
            job = Job(heartrate=10)
            MockJobRunner(job=job)
            session.add(job)
            session.add(old_job)
            session.commit()

            self._compare_jobs(most_recent_job(MockJobRunner.job_type, session=session), job)
            self._compare_jobs(old_job.most_recent_job(session=session), job)

            session.rollback()

    def test_most_recent_job_running_precedence(self):
        with create_session() as session:
            old_running_state_job = Job(heartrate=10)
            MockJobRunner(job=old_running_state_job)
            old_running_state_job.latest_heartbeat = timezone.utcnow()
            old_running_state_job.state = State.RUNNING
            new_failed_state_job = Job(heartrate=10)
            MockJobRunner(job=new_failed_state_job)
            new_failed_state_job.latest_heartbeat = timezone.utcnow()
            new_failed_state_job.state = State.FAILED
            new_null_state_job = Job(heartrate=10)
            MockJobRunner(job=new_null_state_job)
            new_null_state_job.latest_heartbeat = timezone.utcnow()
            new_null_state_job.state = None
            session.add(old_running_state_job)
            session.add(new_failed_state_job)
            session.add(new_null_state_job)
            session.commit()

            self._compare_jobs(
                most_recent_job(MockJobRunner.job_type, session=session), old_running_state_job
            )

            session.rollback()

    def test_is_alive(self):
        job = Job(heartrate=10, state=State.RUNNING)
        assert job.is_alive() is True

        job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(seconds=20)
        assert job.is_alive() is True

        job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(seconds=21)
        assert job.is_alive() is False

        # test because .seconds was used before instead of total_seconds
        # internal repr of datetime is (days, seconds)
        job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(days=1)
        assert job.is_alive() is False

        job.state = State.SUCCESS
        job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(seconds=10)
        assert job.is_alive() is False, "Completed jobs even with recent heartbeat should not be alive"

    @pytest.mark.parametrize("job_type", ["SchedulerJob", "TriggererJob"])
    def test_is_alive_scheduler(self, job_type):
        job = Job(heartrate=10, state=State.RUNNING, job_type=job_type)
        assert job.is_alive() is True

        job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(seconds=20)
        assert job.is_alive() is True

        # default health-check grace period for scheduler job is not heartrate*2.1 but 30 seconds
        job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(seconds=21)
        assert job.is_alive() is True

        job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(seconds=31)
        assert job.is_alive() is False

        # test because .seconds was used before instead of total_seconds
        # internal repr of datetime is (days, seconds)
        job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(days=1)
        assert job.is_alive() is False

        job.state = State.SUCCESS
        job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(seconds=10)
        assert job.is_alive() is False, "Completed jobs even with recent heartbeat should not be alive"

    @patch("airflow.jobs.job.create_session")
    def test_heartbeat_failed(self, mock_create_session, caplog):
        when = timezone.utcnow() - datetime.timedelta(seconds=60)
        with create_session() as session:
            mock_session = Mock(spec_set=session, name="MockSession")
            mock_create_session.return_value.__enter__.return_value = mock_session
            mock_session.commit.side_effect = OperationalError("Force fail", {}, None)
        job = Job(heartrate=10, state=State.RUNNING)
        job.latest_heartbeat = when
        with caplog.at_level(logging.ERROR):
            job.heartbeat(heartbeat_callback=lambda _: None, session=mock_session)
        assert "heartbeat failed with error" in caplog.text
        assert job.latest_heartbeat == when, "attribute not updated when heartbeat fails"
        assert job.heartbeat_failed

    @conf_vars(
        {
            ("scheduler", "max_tis_per_query"): "100",
        }
    )
    @patch("airflow.jobs.job.ExecutorLoader.get_default_executor")
    @patch("airflow.jobs.job.ExecutorLoader.init_executors")
    @patch("airflow.jobs.job.get_hostname")
    @patch("airflow.jobs.job.getuser")
    def test_essential_attr(self, mock_getuser, mock_hostname, mock_init_executors, mock_default_executor):
        mock_local_executor = LocalExecutor()
        mock_hostname.return_value = "test_hostname"
        mock_getuser.return_value = "testuser"
        mock_default_executor.return_value = mock_local_executor
        mock_init_executors.return_value = [mock_local_executor]

        test_job = Job(heartrate=10, dag_id="example_dag", state=State.RUNNING)
        MockJobRunner(job=test_job)
        assert test_job.heartrate == 10
        assert test_job.dag_id == "example_dag"
        assert test_job.hostname == "test_hostname"
        assert test_job.max_tis_per_query == 100
        assert test_job.unixname == "testuser"
        assert test_job.state == "running"
        assert test_job.executor == mock_local_executor
        assert test_job.executors == [mock_local_executor]

    def test_heartbeat(self, frozen_sleep, monkeypatch):
        monkeypatch.setattr("airflow.jobs.job.sleep", frozen_sleep)
        with create_session() as session:
            job = Job(heartrate=10)
            job.latest_heartbeat = timezone.utcnow()
            session.add(job)
            session.commit()

            hb_callback = Mock()
            job.heartbeat(heartbeat_callback=hb_callback)

            hb_callback.assert_called_once_with(ANY)

            hb_callback.reset_mock()
            perform_heartbeat(job=job, heartbeat_callback=hb_callback, only_if_necessary=True)
            assert hb_callback.called is False
