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

import os
from datetime import datetime, timedelta
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
import time_machine
from sqlalchemy import delete, select

from airflow.configuration import conf
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.providers.common.compat.sdk import Stats, timezone
from airflow.providers.edge3.executors.edge_executor import EdgeExecutor
from airflow.providers.edge3.models.edge_job import EdgeJobModel
from airflow.providers.edge3.models.edge_worker import EdgeWorkerModel, EdgeWorkerState
from airflow.utils.session import create_session
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_2_PLUS

pytestmark = pytest.mark.db_test


class TestEdgeExecutor:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self):
        with create_session() as session:
            session.execute(delete(EdgeJobModel))

    def get_test_executor(self, pool_slots=1):
        key = TaskInstanceKey(
            dag_id="test_dag", run_id="test_run", task_id="test_task", map_index=-1, try_number=1
        )
        ti = MagicMock()
        ti.pool_slots = pool_slots
        ti.dag_run.dag_id = key.dag_id
        ti.dag_run.run_id = key.run_id
        ti.dag_run.start_date = datetime(2021, 1, 1)
        executor = EdgeExecutor()
        executor.queued_tasks = {key: [None, None, None, ti]}

        return (executor, key)

    @patch(f"{Stats.__module__}.Stats.incr")
    def test_sync_orphaned_tasks(self, mock_stats_incr):
        executor = EdgeExecutor()

        delta_to_purge = timedelta(minutes=conf.getint("edge", "job_fail_purge") + 1)
        delta_to_orphaned_config_name = "task_instance_heartbeat_timeout"

        delta_to_orphaned = timedelta(seconds=conf.getint("scheduler", delta_to_orphaned_config_name) + 1)

        with create_session() as session:
            for task_id, state, last_update in [
                (
                    "started_running_orphaned",
                    TaskInstanceState.RUNNING,
                    timezone.utcnow() - delta_to_orphaned,
                ),
                ("started_removed", TaskInstanceState.REMOVED, timezone.utcnow() - delta_to_purge),
            ]:
                session.add(
                    EdgeJobModel(
                        dag_id="test_dag",
                        task_id=task_id,
                        run_id="test_run",
                        map_index=-1,
                        try_number=1,
                        state=state,
                        queue="default",
                        command="mock",
                        concurrency_slots=1,
                        last_update=last_update,
                    )
                )
                session.commit()

        executor.sync()

        mock_stats_incr.assert_called_with(
            "edge_worker.ti.finish",
            tags={
                "dag_id": "test_dag",
                "queue": "default",
                "state": "failed",
                "task_id": "started_running_orphaned",
            },
        )
        mock_stats_incr.call_count == 2

        with create_session() as session:
            jobs = session.scalars(select(EdgeJobModel)).all()
            assert len(jobs) == 1
            assert jobs[0].task_id == "started_running_orphaned"
            assert jobs[0].state == TaskInstanceState.REMOVED

    @patch("airflow.providers.edge3.executors.edge_executor.EdgeExecutor.running_state")
    @patch("airflow.providers.edge3.executors.edge_executor.EdgeExecutor.success")
    @patch("airflow.providers.edge3.executors.edge_executor.EdgeExecutor.fail")
    def test_sync(self, mock_fail, mock_success, mock_running_state):
        executor = EdgeExecutor()

        def remove_from_running(key: TaskInstanceKey):
            executor.running.remove(key)

        mock_success.side_effect = remove_from_running
        mock_fail.side_effect = remove_from_running

        delta_to_purge = timedelta(minutes=conf.getint("edge", "job_fail_purge") + 1)

        # Prepare some data
        with create_session() as session:
            for task_id, state, last_update in [
                ("started_running", TaskInstanceState.RUNNING, timezone.utcnow()),
                ("started_success", TaskInstanceState.SUCCESS, timezone.utcnow() - delta_to_purge),
                ("started_failed", TaskInstanceState.FAILED, timezone.utcnow() - delta_to_purge),
            ]:
                session.add(
                    EdgeJobModel(
                        dag_id="test_dag",
                        task_id=task_id,
                        run_id="test_run",
                        map_index=-1,
                        try_number=1,
                        state=state,
                        queue="default",
                        concurrency_slots=1,
                        command="mock",
                        last_update=last_update,
                    )
                )
                key = TaskInstanceKey(
                    dag_id="test_dag", run_id="test_run", task_id=task_id, map_index=-1, try_number=1
                )
                executor.running.add(key)
                session.commit()
        assert len(executor.running) == 3

        executor.sync()

        with create_session() as session:
            jobs = session.scalars(select(EdgeJobModel)).all()
            assert len(session.scalars(select(EdgeJobModel)).all()) == 1
            assert jobs[0].task_id == "started_running"
            assert jobs[0].state == TaskInstanceState.RUNNING

        assert len(executor.running) == 1
        mock_running_state.assert_called_once()
        mock_success.assert_called_once()
        mock_fail.assert_called_once()

        # Any test another round with one new run
        mock_running_state.reset_mock()
        mock_success.reset_mock()
        mock_fail.reset_mock()

        with create_session() as session:
            task_id = "started_running2"
            state = TaskInstanceState.RUNNING
            session.add(
                EdgeJobModel(
                    dag_id="test_dag",
                    task_id=task_id,
                    run_id="test_run",
                    map_index=-1,
                    try_number=1,
                    state=state,
                    concurrency_slots=1,
                    queue="default",
                    command="mock",
                    last_update=timezone.utcnow(),
                )
            )
            key = TaskInstanceKey(
                dag_id="test_dag", run_id="test_run", task_id=task_id, map_index=-1, try_number=1
            )
            executor.running.add(key)
            session.commit()
        assert len(executor.running) == 2

        executor.sync()

        assert len(executor.running) == 2
        mock_running_state.assert_called_once()  # because we reported already first run, new run calling
        mock_success.assert_not_called()  # because we reported already, not running anymore
        mock_fail.assert_not_called()  # because we reported already, not running anymore
        mock_running_state.reset_mock()

        executor.sync()

        assert len(executor.running) == 2
        # Now none is called as we called before already
        mock_running_state.assert_not_called()
        mock_success.assert_not_called()
        mock_fail.assert_not_called()

    def test_sync_active_worker(self):
        executor = EdgeExecutor()

        # Prepare some data
        with create_session() as session:
            # Clear existing workers to avoid unique constraint violation
            session.execute(delete(EdgeWorkerModel))
            session.commit()

            # Add workers with different states
            for worker_name, state, last_heartbeat in [
                (
                    "inactive_timed_out_worker",
                    EdgeWorkerState.IDLE,
                    datetime(2023, 1, 1, 0, 59, 0, tzinfo=timezone.utc),
                ),
                ("active_worker", EdgeWorkerState.IDLE, datetime(2023, 1, 1, 0, 59, 10, tzinfo=timezone.utc)),
                (
                    "offline_worker",
                    EdgeWorkerState.OFFLINE,
                    datetime(2023, 1, 1, 0, 59, 10, tzinfo=timezone.utc),
                ),
                (
                    "offline_maintenance_worker",
                    EdgeWorkerState.OFFLINE_MAINTENANCE,
                    datetime(2023, 1, 1, 0, 59, 10, tzinfo=timezone.utc),
                ),
            ]:
                session.add(
                    EdgeWorkerModel(
                        worker_name=worker_name,
                        state=state,
                        last_update=last_heartbeat,
                        queues="",
                        first_online=timezone.utcnow(),
                    )
                )
                session.commit()

        with time_machine.travel(datetime(2023, 1, 1, 1, 0, 0, tzinfo=timezone.utc), tick=False):
            with conf_vars({("edge", "heartbeat_interval"): "10"}):
                executor.sync()

        with create_session() as session:
            for worker in session.scalars(select(EdgeWorkerModel)).all():
                print(worker.worker_name)
                if "maintenance_" in worker.worker_name:
                    EdgeWorkerState.OFFLINE_MAINTENANCE
                elif "offline_" in worker.worker_name:
                    assert worker.state == EdgeWorkerState.OFFLINE
                elif "inactive_" in worker.worker_name:
                    assert worker.state == EdgeWorkerState.UNKNOWN
                else:
                    assert worker.state == EdgeWorkerState.IDLE

    def test_revoke_task(self):
        """Test that revoke_task removes task from executor and database."""
        executor = EdgeExecutor()
        key = TaskInstanceKey(
            dag_id="test_dag", run_id="test_run", task_id="test_task", map_index=-1, try_number=1
        )

        # Create a mock task instance
        ti = MagicMock()
        ti.key = key
        ti.dag_id = "test_dag"
        ti.task_id = "test_task"
        ti.run_id = "test_run"
        ti.map_index = -1
        ti.try_number = 1

        # Add task to executor's internal state
        executor.running.add(key)
        executor.queued_tasks[key] = [None, None, None, ti]
        executor.last_reported_state[key] = TaskInstanceState.QUEUED

        # Add corresponding job to database
        with create_session() as session:
            session.add(
                EdgeJobModel(
                    dag_id="test_dag",
                    task_id="test_task",
                    run_id="test_run",
                    map_index=-1,
                    try_number=1,
                    state=TaskInstanceState.QUEUED,
                    queue="default",
                    command="mock",
                    concurrency_slots=1,
                )
            )
            session.commit()

        # Verify job exists before revoke
        with create_session() as session:
            jobs = session.scalars(select(EdgeJobModel)).all()
            assert len(jobs) == 1

        # Revoke the task
        executor.revoke_task(ti=ti)

        # Verify task is removed from executor's internal state
        assert key not in executor.running
        assert key not in executor.queued_tasks
        assert key not in executor.last_reported_state

        # Verify job is removed from database
        with create_session() as session:
            jobs = session.scalars(select(EdgeJobModel)).all()
            assert len(jobs) == 0

    def test_revoke_task_nonexistent(self):
        """Test that revoke_task handles non-existent tasks gracefully."""
        executor = EdgeExecutor()
        key = TaskInstanceKey(
            dag_id="nonexistent_dag",
            run_id="nonexistent_run",
            task_id="nonexistent_task",
            map_index=-1,
            try_number=1,
        )

        # Create a mock task instance
        ti = MagicMock()
        ti.key = key
        ti.dag_id = "nonexistent_dag"
        ti.task_id = "nonexistent_task"
        ti.run_id = "nonexistent_run"
        ti.map_index = -1
        ti.try_number = 1

        # Revoke a task that doesn't exist (should not raise error)
        executor.revoke_task(ti=ti)

        # Verify nothing breaks
        assert key not in executor.running
        assert key not in executor.queued_tasks


class TestEdgeExecutorMultiTeam:
    """Tests for multi-team (AIP-67) support in EdgeExecutor."""

    @pytest.fixture(autouse=True)
    def setup_test_cases(self):
        with create_session() as session:
            session.execute(delete(EdgeJobModel))
            session.execute(delete(EdgeWorkerModel))
            session.commit()

    def test_global_executor_without_team_name(self):
        """Test that global executor (no team) works correctly."""
        executor = EdgeExecutor()

        assert hasattr(executor, "conf")
        assert executor.team_name is None
        if AIRFLOW_V_3_2_PLUS:
            assert executor.conf.team_name is None

    @pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="The tests should be skipped for Airflow < 3.2")
    def test_executor_with_team_name(self):
        """Test that executor with team_name has correct conf setup."""
        team_name = "test_team"
        executor = EdgeExecutor(team_name=team_name)

        assert hasattr(executor, "conf")
        assert executor.team_name == team_name
        assert executor.conf.team_name == team_name

    @pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="The tests should be skipped for Airflow < 3.2")
    def test_multiple_team_executors_isolation(self):
        """Test that multiple team executors can coexist with isolated resources."""
        team_a_executor = EdgeExecutor(parallelism=2, team_name="team_a")
        team_b_executor = EdgeExecutor(parallelism=3, team_name="team_b")

        assert team_a_executor.running is not team_b_executor.running
        assert team_a_executor.queued_tasks is not team_b_executor.queued_tasks
        assert team_a_executor.last_reported_state is not team_b_executor.last_reported_state

        if AIRFLOW_V_3_2_PLUS:
            assert team_a_executor.conf.team_name == "team_a"
            assert team_b_executor.conf.team_name == "team_b"

        assert team_a_executor.parallelism == 2
        assert team_b_executor.parallelism == 3

    @pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="The tests should be skipped for Airflow < 3.2")
    def test_team_config_used_in_check_worker_liveness(self):
        """Test that _check_worker_liveness reads config from self.conf, not global conf."""
        team_name = "test_team"
        executor = EdgeExecutor(team_name=team_name)

        team_env_key_prefix = f"AIRFLOW__{team_name.upper()}___EDGE__"
        test_key_values = [
            "heartbeat_interval",
            "task_instance_heartbeat_timeout",
            "job_success_purge",
            "job_fail_purge",
        ]
        for test_key_value in test_key_values:
            with mock.patch.dict(os.environ, {f"{team_env_key_prefix}{test_key_value.upper()}": "100"}):
                value = executor.conf.getint("edge", test_key_value)
                assert value == 100

    @pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="The tests should be skipped for Airflow < 3.2")
    def test_purge_jobs_filters_by_team_name(self):
        """Test that _purge_jobs only purges jobs belonging to its team."""
        executor_a = EdgeExecutor(team_name="team_a")

        delta_to_purge = timedelta(minutes=conf.getint("edge", "job_fail_purge") + 1)

        with create_session() as session:
            for team in ["team_a", "team_b"]:
                session.add(
                    EdgeJobModel(
                        dag_id="test_dag",
                        task_id=f"task_{team}",
                        run_id="test_run",
                        map_index=-1,
                        try_number=1,
                        state=TaskInstanceState.FAILED,
                        queue="default",
                        command="mock",
                        concurrency_slots=1,
                        last_update=timezone.utcnow() - delta_to_purge,
                        team_name=team,
                    )
                )
            session.commit()

        with create_session() as session:
            executor_a._purge_jobs(session)
            session.commit()

        with create_session() as session:
            remaining_jobs = session.scalars(select(EdgeJobModel)).all()
            assert len(remaining_jobs) == 1
            assert remaining_jobs[0].team_name == "team_b"

    @pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="The tests should be skipped for Airflow < 3.2")
    def test_update_orphaned_jobs_filters_by_team_name(self):
        """Test that _update_orphaned_jobs only checks jobs belonging to its team."""
        executor_a = EdgeExecutor(team_name="team_a")

        heartbeat_timeout = conf.getint("scheduler", "task_instance_heartbeat_timeout")
        delta_to_orphaned = timedelta(seconds=heartbeat_timeout + 1)

        with create_session() as session:
            for team in ["team_a", "team_b"]:
                session.add(
                    EdgeJobModel(
                        dag_id="test_dag",
                        task_id=f"task_{team}",
                        run_id="test_run",
                        map_index=-1,
                        try_number=1,
                        state=TaskInstanceState.RUNNING,
                        queue="default",
                        command="mock",
                        concurrency_slots=1,
                        last_update=timezone.utcnow() - delta_to_orphaned,
                        team_name=team,
                    )
                )
            session.commit()

        with create_session() as session:
            executor_a._update_orphaned_jobs(session)
            session.commit()

        with create_session() as session:
            jobs = session.scalars(select(EdgeJobModel)).all()
            jobs_by_team = {job.team_name: job for job in jobs}
            assert jobs_by_team["team_a"].state != TaskInstanceState.RUNNING
            assert jobs_by_team["team_b"].state == TaskInstanceState.RUNNING

    @pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="The tests should be skipped for Airflow < 3.2")
    def test_check_worker_liveness_filters_by_team_name(self):
        """Test that _check_worker_liveness only resets workers belonging to its team."""
        executor_a = EdgeExecutor(team_name="team_a")

        with create_session() as session:
            for worker_name, team in [
                ("worker_team_a", "team_a"),
                ("worker_team_b", "team_b"),
            ]:
                session.add(
                    EdgeWorkerModel(
                        worker_name=worker_name,
                        state=EdgeWorkerState.IDLE,
                        last_update=datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                        queues=["default"],
                        first_online=timezone.utcnow(),
                        team_name=team,
                    )
                )
            session.commit()

        with time_machine.travel(datetime(2023, 1, 1, 1, 0, 0, tzinfo=timezone.utc), tick=False):
            with conf_vars({("edge", "heartbeat_interval"): "10"}):
                with create_session() as session:
                    executor_a._check_worker_liveness(session)
                    session.commit()

        with create_session() as session:
            workers = {w.worker_name: w for w in session.scalars(select(EdgeWorkerModel)).all()}
            assert workers["worker_team_a"].state == EdgeWorkerState.UNKNOWN
            assert workers["worker_team_b"].state == EdgeWorkerState.IDLE

    @pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="The tests should be skipped for Airflow < 3.2")
    def test_no_team_executor_processes_all_jobs(self):
        """Test that an executor without team_name processes only job has no team_name."""
        executor = EdgeExecutor()

        delta_to_purge = timedelta(minutes=conf.getint("edge", "job_fail_purge") + 1)

        with create_session() as session:
            for team in ["team_a", "team_b", None]:
                session.add(
                    EdgeJobModel(
                        dag_id="test_dag",
                        task_id=f"task_{team}",
                        run_id="test_run",
                        map_index=-1,
                        try_number=1,
                        state=TaskInstanceState.FAILED,
                        queue="default",
                        command="mock",
                        concurrency_slots=1,
                        last_update=timezone.utcnow() - delta_to_purge,
                        team_name=team,
                    )
                )
            session.commit()

        with create_session() as session:
            executor._purge_jobs(session)
            session.commit()

        with create_session() as session:
            remaining_jobs = session.scalars(select(EdgeJobModel)).all()
            assert len(remaining_jobs) == 2
