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
Python runtime coordinator.

Unlike foreign language coordinators, this starts the ordinary Task SDK
:class:`~airflow.sdk.execution_time.supervisor.ActivitySubprocess`. It is used
by default when a queue is not mapped to any other coordinator.

The coordinator can also be configured explicitly in ``[sdk] coordinators`` and
``[sdk] queue_to_coordinator`` to carry ``task_handler_bundle_name``. This is
useful for YAML Dags, where ``run:`` tasks can point to a task handler defined
in Python. The value is read by the parse and execution paths to import handler
modules; see :func:`~airflow.sdk.task_handlers.task_handler`.
"""

from __future__ import annotations

import contextlib
import os
import signal
from typing import TYPE_CHECKING, Any

import attrs
import structlog

from airflow.sdk.execution_time.coordinator import BaseCoordinator

if TYPE_CHECKING:
    from collections.abc import Generator
    from os import PathLike

    from structlog.typing import FilteringBoundLogger

    from airflow.sdk.api.client import Client
    from airflow.sdk.api.datamodels._generated import TaskInstance
    from airflow.sdk.task_handlers import TaskHandlerDeclaration

log: FilteringBoundLogger = structlog.get_logger(logger_name="coordinators.python")


@contextlib.contextmanager
def _warm_shutdown_signals() -> Generator[None, None, None]:
    """
    Install SIGTERM/SIGINT warm-shutdown handlers during task supervision.

    While supervising a task the supervisor must not be torn down by a
    termination signal; instead it keeps running so the task can finish (or be
    shut down gracefully) and its terminal state and logs are reported. The
    handlers are installed around BOTH ``start()`` (which transitions the TI to
    RUNNING) and ``wait()`` (which runs the task and then reports the terminal
    state / uploads logs), so there is no window where Python's default SIGTERM
    disposition could kill the supervisor and tear the just-started task down
    with it.

    The previous dispositions are restored on exit so a long-lived supervisor
    process (e.g. a reused Celery prefork worker) does not leak the handler into
    later tasks or clobber the worker's own signal handling.
    """

    def _warm_shutdown(signum, frame):
        log.info(
            "Received signal; warm shutdown in progress, waiting for the running task to complete.",
            signal=signal.Signals(signum).name,
            pid=os.getpid(),
        )

    prev_sigterm = signal.getsignal(signal.SIGTERM)
    prev_sigint = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGTERM, _warm_shutdown)
    signal.signal(signal.SIGINT, _warm_shutdown)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, prev_sigterm)
        signal.signal(signal.SIGINT, prev_sigint)


@attrs.define(kw_only=True)
class PythonCoordinator(BaseCoordinator):
    """
    Coordinator that executes Python tasks in an in-process Task SDK subprocess.

    This is automatically used as the default fallback when a queue is not
    mapped to another coordinator. It may also be configured explicitly in to
    carry ``task_handler_bundle_name`` via ``[sdk] coordinators`` for specific
    queues in ``[sdk] queue_to_coordinator``.

    :param task_handler_bundle_name: Name of the Dag bundle holding the Python
        ``@task_handler`` modules for stub tasks routed to the coordinator. This
        is validated against ``[dag_processor] dag_bundle_config_list`` when
        configuration loads.
    """

    task_handler_bundle_name: str | None = None

    def execute_task(
        self,
        *,
        what: TaskInstance,
        dag_rel_path: str | PathLike[str],
        bundle_info,
        client: Client,
        logger: FilteringBoundLogger | None = None,
        sentry_integration: str = "",
        subprocess_logs_to_stdout: bool,
        **kwargs: Any,
    ) -> BaseCoordinator.ExecutionResult:
        # TODO: Importing this at the top causes circular imports.
        # ActivitySubprocess and WatchedSubprocess should be moved out of the
        # supervisor, and maybe with additional refactoring to abstract out
        # process handling.
        from airflow.sdk.execution_time.supervisor import ActivitySubprocess

        # Keep the warm-shutdown handlers installed across both start() (which
        # transitions the TI to RUNNING) and wait() (which runs the task and
        # reports its terminal state / uploads logs) so a SIGTERM at any point
        # in this window can't kill the supervisor and tear the task down.
        with _warm_shutdown_signals():
            process = ActivitySubprocess.start(
                dag_rel_path=dag_rel_path,
                what=what,
                client=client,
                logger=logger,
                bundle_info=bundle_info,
                subprocess_logs_to_stdout=subprocess_logs_to_stdout,
                sentry_integration=sentry_integration,
            )
            exit_code = process.wait()
            return self.ExecutionResult(exit_code, process.final_state)

    def describe_task_handler(self, dag_id: str, task_id: str) -> TaskHandlerDeclaration | None:
        """
        Describe the registered handler for the specified task.

        Since Python task handlers are registered in-process, they are simply
        imported and introspected.

        The returned declaration carries both the signature (for validation) and
        the resolved callable (for execution).
        """
        from airflow.sdk.task_handlers import declare_handler, get_handler, load_handler_bundle

        load_handler_bundle(self.task_handler_bundle_name)
        handler = get_handler(dag_id, task_id)
        return None if handler is None else declare_handler(task_id, handler)
