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

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
import time_machine

from airflow.configuration import conf
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.providers.common.compat.sdk import Stats, timezone
from airflow.providers.edge3.executors.edge_executor import EdgeExecutor
from airflow.providers.edge3.models.edge_job import EdgeJobModel
from airflow.providers.edge3.models.edge_worker import EdgeWorkerModel, EdgeWorkerState
from airflow.utils.session import create_session
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test


class TestEdgeExecutor:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self):
        with create_session() as session:
            session.query(EdgeJobModel).delete()

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
            jobs = session.query(EdgeJobModel).all()
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
            jobs = session.query(EdgeJobModel).all()
            assert len(session.query(EdgeJobModel).all()) == 1
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
            session.query(EdgeWorkerModel).delete()
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
            for worker in session.query(EdgeWorkerModel).all():
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
            jobs = session.query(EdgeJobModel).all()
            assert len(jobs) == 1

        # Revoke the task
        executor.revoke_task(ti=ti)

        # Verify task is removed from executor's internal state
        assert key not in executor.running
        assert key not in executor.queued_tasks
        assert key not in executor.last_reported_state

        # Verify job is removed from database
        with create_session() as session:
            jobs = session.query(EdgeJobModel).all()
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
