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
"""
SequentialExecutor.

.. seealso::
    For more information on how the SequentialExecutor works, take a look at the guide:
    :ref:`executor:SequentialExecutor`
"""

from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING, Any

from airflow.executors.base_executor import BaseExecutor
from airflow.traces.tracer import Trace, add_span

if TYPE_CHECKING:
    from airflow.executors.base_executor import CommandType
    from airflow.models.taskinstancekey import TaskInstanceKey


class SequentialExecutor(BaseExecutor):
    """
    This executor will only run one task instance at a time.

    It can be used for debugging. It is also the only executor
    that can be used with sqlite since sqlite doesn't support
    multiple connections.

    Since we want airflow to work out of the box, it defaults to this
    SequentialExecutor alongside sqlite as you first install it.
    """

    supports_pickling: bool = False

    is_local: bool = True
    is_single_threaded: bool = True
    is_production: bool = False

    serve_logs: bool = True

    def __init__(self):
        super().__init__()
        self.commands_to_run = []

    @add_span
    def execute_async(
        self,
        key: TaskInstanceKey,
        command: CommandType,
        queue: str | None = None,
        executor_config: Any | None = None,
    ) -> None:
        self.validate_airflow_tasks_run_command(command)
        self.commands_to_run.append((key, command))

        span = Trace.get_current_span()
        if span.is_recording():
            span.set_attribute("dag_id", key.dag_id)
            span.set_attribute("run_id", key.run_id)
            span.set_attribute("task_id", key.task_id)
            span.set_attribute("try_number", key.try_number - 1)
            span.set_attribute("commands_to_run", str(self.commands_to_run))

    def sync(self) -> None:
        for key, command in self.commands_to_run:
            self.log.info("Executing command: %s", command)

            try:
                subprocess.check_call(command, close_fds=True)
                self.success(key)
            except subprocess.CalledProcessError as e:
                self.fail(key)
                self.log.error("Failed to execute task %s.", e)

        self.commands_to_run = []

    def end(self):
        """End the executor."""
        self.heartbeat()

    def terminate(self):
        """Terminate the executor is not doing anything."""
