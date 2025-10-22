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

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger as Logger

    from airflow.sdk import Context
    from airflow.sdk.execution_time.comms import ToSupervisor
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
    from airflow.sdk.types import DagRunProtocol, RuntimeTaskInstanceProtocol, TaskInstanceState

    RunReturn = tuple[TaskInstanceState, ToSupervisor | None, BaseException | None]

    class Run(Protocol):
        def __call__(self, ti: RuntimeTaskInstance, context: Context, log: Logger) -> RunReturn: ...


class NoopSentry:
    """Blank class for Sentry."""

    def add_tagging(self, dag_run: DagRunProtocol, task_instance: RuntimeTaskInstanceProtocol) -> None:
        """Blank function for tagging."""

    def add_breadcrumbs(self, task_instance: RuntimeTaskInstanceProtocol) -> None:
        """Blank function for breadcrumbs."""

    def enrich_errors(self, run: Run) -> Run:
        """Blank function for formatting a TaskInstance._run_raw_task."""
        return run

    def flush(self) -> None:
        """Blank function for flushing errors."""
