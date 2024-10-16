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

from collections import defaultdict
from unittest.mock import MagicMock, Mock

from airflow.executors.base_executor import BaseExecutor
from airflow.executors.executor_utils import ExecutorName
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.utils.session import create_session
from airflow.utils.state import State


class MockExecutor(BaseExecutor):
    """TestExecutor is used for unit testing purposes."""

    supports_pickling = False
    mock_module_path = "mock.executor.path"
    mock_alias = "mock_executor"

    def __init__(self, do_update=True, *args, **kwargs):
        self.do_update = do_update
        self.callback_sink = MagicMock()

        # A list of "batches" of tasks
        self.history = []
        # All the tasks, in a stable sort order
        self.sorted_tasks = []

        self.name = ExecutorName(module_path=self.mock_module_path, alias=self.mock_alias)

        # If multiprocessing runs in spawn mode,
        # arguments are to be pickled but lambda is not picclable.
        # So we should pass self.success instead of lambda.
        self.mock_task_results = defaultdict(self.success)

        super().__init__(*args, **kwargs)

        self.task_context_logger = Mock()

    def success(self):
        return State.SUCCESS

    def heartbeat(self):
        if not self.do_update:
            return

        with create_session() as session:
            self.history.append(list(self.queued_tasks.values()))

            # Create a stable/predictable sort order for events in self.history
            # for tests!
            def sort_by(item):
                key, val = item
                (dag_id, task_id, date, try_number, map_index) = key
                (_, prio, _, _) = val
                # Sort by priority (DESC), then date,task, try
                return -prio, date, dag_id, task_id, map_index, try_number

            open_slots = self.parallelism - len(self.running)
            sorted_queue = sorted(self.queued_tasks.items(), key=sort_by)
            for key, (_, _, _, ti) in sorted_queue[:open_slots]:
                self.queued_tasks.pop(key)
                state = self.mock_task_results[key]
                ti.set_state(state, session=session)
                self.change_state(key, state)

    def terminate(self):
        pass

    def end(self):
        self.sync()

    def change_state(self, key, state, info=None, remove_running=False):
        super().change_state(key, state, info=info, remove_running=remove_running)
        # The normal event buffer is cleared after reading, we want to keep
        # a list of all events for testing
        self.sorted_tasks.append((key, (state, info)))

    def mock_task_fail(self, dag_id, task_id, run_id: str, try_number=1):
        """
        Mock for test failures.

        Set the mock outcome of running this particular task instances to
        FAILED.

        If the task identified by the tuple ``(dag_id, task_id, date,
        try_number)`` is run by this executor its state will be FAILED.
        """
        assert isinstance(run_id, str)
        self.mock_task_results[TaskInstanceKey(dag_id, task_id, run_id, try_number)] = State.FAILED

    def get_mock_loader_side_effect(self):
        return lambda *x: {
            (None,): self,
            (self.mock_module_path,): self,
            (self.mock_alias,): self,
        }[x]
