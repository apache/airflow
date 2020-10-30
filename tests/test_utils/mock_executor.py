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
from heapq import heappop

from airflow.executors.base_executor import BaseExecutor
from airflow.models.taskinstance import TaskInstanceKey
from airflow.utils.session import create_session
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

        # If multiprocessing runs in spawn mode,
        # arguments are to be pickled but lambda is not picclable.
        # So we should pass self.success instead of lambda.
        self.mock_task_results = defaultdict(self.success)

        super().__init__(*args, **kwargs)

    def success(self):
        return State.SUCCESS

    def heartbeat(self):
        if not self.do_update:
            return

        with create_session() as session:
            self.history.append([x[1] for x in self.queued_tasks_priority_queue])

            open_slots = self.parallelism - len(self.running)
            for _ in range(min((open_slots, len(self.queued_tasks)))):
                self.log.info("XDDEBUG-pos1: ")
                self.log.info(self.queued_tasks)
                self.log.info(self.queued_tasks_priority_queue)
                _, (_, _, _, ti) = heappop(self.queued_tasks_priority_queue)
                key = ti.key
                self.log.info("XDDEBUG-pos2: ")
                self.log.info(self.queued_tasks)
                self.log.info(self.queued_tasks_priority_queue)
                self.queued_tasks.remove(key)
                state = self.mock_task_results[key]
                ti.set_state(state, session=session)
                self.change_state(key, state)

    def terminate(self):
        pass

    def end(self):
        self.sync()

    def change_state(self, key, state, info=None):
        super().change_state(key, state, info=info)
        # The normal event buffer is cleared after reading, we want to keep
        # a list of all events for testing
        self.sorted_tasks.append((key, (state, info)))

    def mock_task_fail(self, dag_id, task_id, date, try_number=1):
        """
        Set the mock outcome of running this particular task instances to
        FAILED.

        If the task identified by the tuple ``(dag_id, task_id, date,
        try_number)`` is run by this executor it's state will be FAILED.
        """
        self.mock_task_results[TaskInstanceKey(dag_id, task_id, date, try_number)] = State.FAILED
