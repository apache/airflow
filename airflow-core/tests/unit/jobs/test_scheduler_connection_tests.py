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
"""Tests for scheduler connection test dispatch and reaper."""

from __future__ import annotations

import logging
import os
from datetime import timedelta
from unittest import mock

import pytest
import time_machine
from sqlalchemy import delete

from airflow._shared.timezones import timezone
from airflow.executors.base_executor import BaseExecutor
from airflow.executors.local_executor import LocalExecutor
from airflow.jobs.job import Job
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.models.connection_test import ConnectionTest, ConnectionTestState

pytestmark = pytest.mark.db_test


@pytest.fixture
def scheduler_job_runner(session):
    """Create a SchedulerJobRunner with a mock Job and supporting executor."""
    session.execute(delete(ConnectionTest))
    session.commit()

    mock_job = mock.MagicMock(spec=Job)
    mock_job.id = 1
    mock_job.max_tis_per_query = 16
    executor = LocalExecutor()
    executor.queued_connection_tests.clear()
    runner = SchedulerJobRunner.__new__(SchedulerJobRunner)
    runner.job = mock_job
    runner.executors = [executor]
    runner.executor = executor
    runner._log = mock.MagicMock(spec=logging.Logger)
    return runner


class TestDispatchConnectionTests:
    @mock.patch.dict(
        os.environ,
        {
            "AIRFLOW__CORE__MAX_CONNECTION_TEST_CONCURRENCY": "4",
            "AIRFLOW__CORE__CONNECTION_TEST_TIMEOUT": "60",
        },
    )
    def test_dispatch_pending_tests(self, scheduler_job_runner, session):
        """Pending connection tests are dispatched to a supporting executor."""
        ct = ConnectionTest(connection_id="test_conn")
        session.add(ct)
        session.commit()
        assert ct.state == ConnectionTestState.PENDING

        scheduler_job_runner._dispatch_connection_tests(session=session)

        session.expire_all()
        ct = session.get(ConnectionTest, ct.id)
        assert ct.state == ConnectionTestState.QUEUED
        assert len(scheduler_job_runner.executor.queued_connection_tests) == 1

    @mock.patch.dict(
        os.environ,
        {
            "AIRFLOW__CORE__MAX_CONNECTION_TEST_CONCURRENCY": "1",
            "AIRFLOW__CORE__CONNECTION_TEST_TIMEOUT": "60",
        },
    )
    def test_dispatch_respects_concurrency_limit(self, scheduler_job_runner, session):
        """Excess pending tests stay PENDING when concurrency is at capacity."""
        # Create one already-queued test to fill the concurrency budget (limit=1)
        ct_active = ConnectionTest(connection_id="active_conn")
        ct_active.state = ConnectionTestState.QUEUED
        session.add(ct_active)

        ct_pending = ConnectionTest(connection_id="pending_conn")
        session.add(ct_pending)
        session.commit()

        scheduler_job_runner._dispatch_connection_tests(session=session)

        session.expire_all()
        ct_pending = session.get(ConnectionTest, ct_pending.id)
        assert ct_pending.state == ConnectionTestState.PENDING

    @mock.patch.dict(
        os.environ,
        {
            "AIRFLOW__CORE__MAX_CONNECTION_TEST_CONCURRENCY": "4",
            "AIRFLOW__CORE__CONNECTION_TEST_TIMEOUT": "60",
        },
    )
    def test_dispatch_fails_fast_when_no_executor_supports(self, scheduler_job_runner, session):
        """Tests fail immediately when no executor supports connection testing."""
        # Replace executor with one that doesn't support connection tests
        unsupporting_executor = BaseExecutor()
        unsupporting_executor.supports_connection_test = False
        scheduler_job_runner.executors = [unsupporting_executor]
        scheduler_job_runner.executor = unsupporting_executor

        ct = ConnectionTest(connection_id="test_conn")
        session.add(ct)
        session.commit()

        scheduler_job_runner._dispatch_connection_tests(session=session)

        session.expire_all()
        ct = session.get(ConnectionTest, ct.id)
        assert ct.state == ConnectionTestState.FAILED
        assert "No executor supports connection testing" in ct.result_message

    @mock.patch.dict(
        os.environ,
        {
            "AIRFLOW__CORE__MAX_CONNECTION_TEST_CONCURRENCY": "4",
            "AIRFLOW__CORE__CONNECTION_TEST_TIMEOUT": "60",
        },
    )
    def test_dispatch_with_queue_falls_back_to_global_executor(self, scheduler_job_runner, session):
        """Tests with a queue are dispatched to the global executor as fallback."""
        ct = ConnectionTest(connection_id="test_conn", queue="gpu_workers")
        session.add(ct)
        session.commit()

        # Default LocalExecutor has team_name=None — serves as fallback for any queue
        scheduler_job_runner._dispatch_connection_tests(session=session)

        session.expire_all()
        ct = session.get(ConnectionTest, ct.id)
        assert ct.state == ConnectionTestState.QUEUED
        assert len(scheduler_job_runner.executor.queued_connection_tests) == 1


class TestReapStaleConnectionTests:
    @mock.patch.dict(os.environ, {"AIRFLOW__CORE__CONNECTION_TEST_TIMEOUT": "60"})
    def test_reap_stale_queued_test(self, scheduler_job_runner, session):
        """Stale QUEUED tests are marked as FAILED by the reaper."""
        initial_time = timezone.utcnow()

        with time_machine.travel(initial_time, tick=False):
            ct = ConnectionTest(connection_id="test_conn")
            ct.state = ConnectionTestState.QUEUED
            session.add(ct)
            session.commit()

        # Jump forward past timeout (60s) + grace period (30s)
        with time_machine.travel(initial_time + timedelta(seconds=200), tick=False):
            scheduler_job_runner._reap_stale_connection_tests(session=session)

        session.expire_all()
        ct = session.get(ConnectionTest, ct.id)
        assert ct.state == ConnectionTestState.FAILED
        assert "timed out" in ct.result_message

    @mock.patch.dict(os.environ, {"AIRFLOW__CORE__CONNECTION_TEST_TIMEOUT": "60"})
    def test_does_not_reap_fresh_tests(self, scheduler_job_runner, session):
        """Fresh QUEUED tests are not reaped."""
        ct = ConnectionTest(connection_id="test_conn")
        ct.state = ConnectionTestState.QUEUED
        session.add(ct)
        session.commit()

        scheduler_job_runner._reap_stale_connection_tests(session=session)

        session.expire_all()
        ct = session.get(ConnectionTest, ct.id)
        assert ct.state == ConnectionTestState.QUEUED
