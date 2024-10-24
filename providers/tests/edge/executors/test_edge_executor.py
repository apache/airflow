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

from datetime import datetime
from unittest.mock import patch

import pytest

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

    def test_execute_async_bad_command(self):
        executor = EdgeExecutor()
        with pytest.raises(ValueError):
            executor.execute_async(
                TaskInstanceKey(
                    dag_id="test_dag", run_id="test_run", task_id="test_task", map_index=-1, try_number=1
                ),
                command=["hello", "world"],
            )

    def test_execute_async_ok_command(self):
        executor = EdgeExecutor()
        executor.execute_async(
            TaskInstanceKey(
                dag_id="test_dag", run_id="test_run", task_id="test_task", map_index=-1, try_number=1
            ),
            command=["airflow", "tasks", "run", "hello", "world"],
        )
        with create_session() as session:
            jobs: list[EdgeJobModel] = session.query(EdgeJobModel).all()
        assert len(jobs) == 1
        assert jobs[0].dag_id == "test_dag"
        assert jobs[0].run_id == "test_run"
        assert jobs[0].task_id == "test_task"

    @patch("airflow.providers.edge.executors.edge_executor.EdgeExecutor.running_state")
    @patch("airflow.providers.edge.executors.edge_executor.EdgeExecutor.success")
    @patch("airflow.providers.edge.executors.edge_executor.EdgeExecutor.fail")
    def test_sync(self, mock_fail, mock_success, mock_running_state):
        executor = EdgeExecutor()

        def remove_from_running(key: TaskInstanceKey):
            executor.running.remove(key)

        mock_success.side_effect = remove_from_running
        mock_fail.side_effect = remove_from_running

        # Prepare some data
        with create_session() as session:
            for task_id, state in [
                ("started_running", TaskInstanceState.RUNNING),
                ("started_success", TaskInstanceState.SUCCESS),
                ("started_failed", TaskInstanceState.FAILED),
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
                        last_update=timezone.utcnow(),
                    )
                )
                key = TaskInstanceKey(
                    dag_id="test_dag", run_id="test_run", task_id=task_id, map_index=-1, try_number=1
                )
                executor.running.add(key)
                session.commit()
        assert len(executor.running) == 3

        executor.sync()

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
