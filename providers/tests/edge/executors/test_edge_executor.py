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

from airflow.configuration import conf
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.providers.edge.executors.edge_executor import EdgeExecutor
from airflow.providers.edge.models.edge_job import EdgeJobModel
from airflow.providers.edge.models.edge_worker import EdgeWorkerModel, EdgeWorkerState
from airflow.utils import timezone
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

    def test__process_tasks_bad_command(self):
        executor, key = self.get_test_executor()
        task_tuple = (key, ["hello", "world"], None, None)
        with pytest.raises(ValueError):
            executor._process_tasks([task_tuple])

    @pytest.mark.parametrize(
        "pool_slots, expected_concurrency",
        [
            pytest.param(1, 1, id="default_pool_size"),
            pytest.param(5, 5, id="increased_pool_size"),
        ],
    )
    def test__process_tasks_ok_command(self, pool_slots, expected_concurrency):
        executor, key = self.get_test_executor(pool_slots=pool_slots)
        task_tuple = (key, ["airflow", "tasks", "run", "hello", "world"], None, None)
        executor._process_tasks([task_tuple])

        with create_session() as session:
            jobs: list[EdgeJobModel] = session.query(EdgeJobModel).all()
        assert len(jobs) == 1
        assert jobs[0].dag_id == "test_dag"
        assert jobs[0].run_id == "test_run"
        assert jobs[0].task_id == "test_task"
        assert jobs[0].concurrency_slots == expected_concurrency

    def test_sync_orphaned_tasks(self):
        executor = EdgeExecutor()

        delta_to_purge = timedelta(minutes=conf.getint("edge", "job_fail_purge") + 1)
        delta_to_orphaned = timedelta(seconds=conf.getint("scheduler", "scheduler_zombie_task_threshold") + 1)

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
                        command="dummy",
                        concurrency_slots=1,
                        last_update=last_update,
                    )
                )
                session.commit()

        executor.sync()

        with create_session() as session:
            jobs = session.query(EdgeJobModel).all()
            assert len(jobs) == 1
            assert jobs[0].task_id == "started_running_orphaned"
            assert jobs[0].state == TaskInstanceState.REMOVED

    @patch("airflow.providers.edge.executors.edge_executor.EdgeExecutor.running_state")
    @patch("airflow.providers.edge.executors.edge_executor.EdgeExecutor.success")
    @patch("airflow.providers.edge.executors.edge_executor.EdgeExecutor.fail")
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
                        command="dummy",
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
                    command="dummy",
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

        with patch(
            "airflow.utils.timezone.utcnow", return_value=datetime(2023, 1, 1, 1, 0, 0, tzinfo=timezone.utc)
        ):
            with conf_vars({("edge", "heartbeat_interval"): "10"}):
                executor.sync()

        with create_session() as session:
            for worker in session.query(EdgeWorkerModel).all():
                print(worker.worker_name)
                if "offline_" in worker.worker_name:
                    assert worker.state == EdgeWorkerState.OFFLINE
                elif "inactive_" in worker.worker_name:
                    assert worker.state == EdgeWorkerState.UNKNOWN
                else:
                    assert worker.state == EdgeWorkerState.IDLE
