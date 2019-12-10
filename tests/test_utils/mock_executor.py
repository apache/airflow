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

from collections import defaultdict

from airflow.executors.base_executor import BaseExecutor
from airflow.utils.db import create_session
from airflow.utils.state import State


class MockExecutor(BaseExecutor):
    """
    TestExecutor is used for unit testing purposes.
    """

    def __init__(self, do_update=True, *args, **kwargs):
        self.do_update = do_update
        self._running = []

        # A list of "batches" of tasks
        self.history = []
        # All the tasks, in a stable sort order
        self.sorted_tasks = []
        self.mock_task_results = defaultdict(lambda: State.SUCCESS)

        super().__init__(*args, **kwargs)

    def heartbeat(self):
        if not self.do_update:
            return

        with create_session() as session:
            self.history.append(list(self.queued_tasks.values()))

            # Create a stable/predictable sort order for events in self.history
            # for tests!
            def sort_by(item):
                key, val = item
                (dag_id, task_id, date, try_number) = key
                (_, prio, _, _) = val
                # Sort by priority (DESC), then date,task, try
                return -prio, date, dag_id, task_id, try_number

            open_slots = self.parallelism - len(self.running)
            sorted_queue = sorted(self.queued_tasks.items(), key=sort_by)
            for index in range(min((open_slots, len(sorted_queue)))):
                (key, (_, _, _, simple_ti)) = sorted_queue[index]
                self.queued_tasks.pop(key)
                state = self.mock_task_results[key]
                ti = simple_ti.construct_task_instance(session=session, lock_for_update=True)
                ti.set_state(state, session=session)
                self.change_state(key, state)

    def terminate(self):
        pass

    def end(self):
        self.sync()

    def change_state(self, key, state):
        super().change_state(key, state)
        # The normal event buffer is cleared after reading, we want to keep
        # a list of all events for testing
        self.sorted_tasks.append((key, state))

    def mock_task_fail(self, dag_id, task_id, date, try_number=1):
        """
        Set the mock outcome of running this particular task instances to
        FAILED.

        If the task identified by the tuple ``(dag_id, task_id, date,
        try_number)`` is run by this executor it's state will be FAILED.
        """
        self.mock_task_results[(dag_id, task_id, date, try_number)] = State.FAILED
