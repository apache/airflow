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

from unittest import mock
from unittest.mock import MagicMock

from airflow.executors.debug_executor import DebugExecutor
from airflow.utils.state import State


class TestDebugExecutor:
    @mock.patch("airflow.executors.debug_executor.DebugExecutor._run_task")
    def test_sync(self, run_task_mock):
        run_task_mock.return_value = True

        executor = DebugExecutor()

        ti1 = MagicMock(key="t1")
        ti2 = MagicMock(key="t2")
        executor.tasks_to_run = [ti1, ti2]

        executor.sync()
        assert not executor.tasks_to_run
        run_task_mock.assert_has_calls([mock.call(ti1), mock.call(ti2)])

    @mock.patch("airflow.executors.debug_executor.TaskInstance")
    def test_run_task(self, task_instance_mock):
        ti_key = "key"
        job_id = " job_id"
        task_instance_mock.key = ti_key
        task_instance_mock.job_id = job_id

        executor = DebugExecutor()
        executor.running = set([ti_key])
        succeeded = executor._run_task(task_instance_mock)

        assert succeeded
        task_instance_mock._run_raw_task.assert_called_once_with(job_id=job_id)

    def test_queue_task_instance(self):
        key = "ti_key"
        ti = MagicMock(key=key)

        executor = DebugExecutor()
        executor.queue_task_instance(task_instance=ti, mark_success=True, pool="pool")

        assert key in executor.queued_tasks
        assert key in executor.tasks_params
        assert executor.tasks_params[key] == {
            "mark_success": True,
            "pool": "pool",
        }

    def test_trigger_tasks(self):
        execute_async_mock = MagicMock()
        executor = DebugExecutor()
        executor.execute_async = execute_async_mock

        executor.queued_tasks = {
            "t1": (None, 1, None, MagicMock(key="t1")),
            "t2": (None, 2, None, MagicMock(key="t2")),
        }

        executor.trigger_tasks(open_slots=4)
        assert not executor.queued_tasks
        assert len(executor.running) == 2
        assert len(executor.tasks_to_run) == 2
        assert not execute_async_mock.called

    def test_end(self):
        ti = MagicMock(key="ti_key")

        executor = DebugExecutor()
        executor.tasks_to_run = [ti]
        executor.running = set([ti.key])
        executor.end()

        ti.set_state.assert_called_once_with(State.UPSTREAM_FAILED)
        assert not executor.running

    @mock.patch("airflow.executors.debug_executor.DebugExecutor.change_state")
    def test_fail_fast(self, change_state_mock):
        with mock.patch.dict("os.environ", {"AIRFLOW__DEBUG__FAIL_FAST": "True"}):
            executor = DebugExecutor()

        ti1 = MagicMock(key="t1")
        ti2 = MagicMock(key="t2")

        ti1._run_raw_task.side_effect = Exception

        executor.tasks_to_run = [ti1, ti2]

        executor.sync()

        assert executor.fail_fast
        assert not executor.tasks_to_run
        change_state_mock.assert_has_calls(
            [
                mock.call(ti1.key, State.FAILED),
                mock.call(ti2.key, State.UPSTREAM_FAILED),
            ]
        )
