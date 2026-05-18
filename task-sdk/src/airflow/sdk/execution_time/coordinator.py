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
Runtime coordinator for non-Python DAG file processing and task execution.

Provides :class:`BaseCoordinator`, the base class for
SDK-specific coordinators that bridge subprocess I/O between the
Airflow supervisor and an external-SDK runtime (Java, Go, Rust, etc.),
and :class:`CoordinatorManager`, the registry that loads coordinator
instances from the ``[sdk] coordinators`` configuration.

The coordinator's :meth:`~BaseCoordinator.run_task_execution` handles the full
lifecycle:

1. Creates TCP servers for comm and logs channels, and a socketpair for stderr.
2. Calls :meth:`~BaseCoordinator.task_execution_cmd` (provided by the subclass)
   to obtain the subprocess command.
3. Spawns the subprocess and accepts TCP connections from it.
4. Runs a selector-based bridge that transparently forwards bytes
   between fd 0 (supervisor) and the subprocess comm socket, and
   re-emits the subprocess's log and stderr output through structlog.

I/O multiplexing uses the same selector-based loop as
:class:`~airflow.sdk.execution_time.supervisor.WatchedSubprocess`,
driven by :func:`~airflow.sdk.execution_time.selector_loop.service_selector`.
"""

from __future__ import annotations

import functools
from typing import TYPE_CHECKING, Any

import attrs

from airflow.sdk._shared.module_loading import import_string

if TYPE_CHECKING:
    from collections.abc import Mapping
    from os import PathLike

    from structlog.typing import FilteringBoundLogger
    from typing_extensions import Self

    from airflow.sdk.api.client import Client
    from airflow.sdk.execution_time.workloads.task import TaskInstanceDTO


class BaseCoordinator:
    """
    Base coordinator for runtime-specific DAG file processing and task execution.

    Coordinators are instantiated from the ``[sdk] coordinators`` configuration
    (see :class:`CoordinatorManager`) — each entry's ``classpath`` is resolved
    via :func:`~airflow.sdk._shared.module_loading.import_string` and
    constructed with the entry's ``kwargs``.
    """

    @attrs.define(slots=True)
    class ExecutionResult:
        """Return value for :meth:`BaseCoordinator.execute_task`."""

        exit_code: Any
        final_state: str

    def execute_task(
        self,
        *,
        what: TaskInstanceDTO,
        dag_rel_path: str | PathLike[str],
        bundle_info,
        client: Client,
        logger: FilteringBoundLogger | None = None,
        sentry_integration: str = "",
        subprocess_logs_to_stdout: bool,
        **kwargs,
    ) -> ExecutionResult:
        """
        Start task execution.

        This should execute the task and return a result.
        """
        raise NotImplementedError


class _PythonCoordinator(BaseCoordinator):
    """
    Coordinator implementation to execute Python tasks.

    This is not supposed to be specified by users directly, but the fallback
    used by default when nothing is specified.
    """

    def execute_task(
        self,
        *,
        what: TaskInstanceDTO,
        dag_rel_path: str | PathLike[str],
        bundle_info,
        client: Client,
        logger: FilteringBoundLogger | None = None,
        sentry_integration: str = "",
        subprocess_logs_to_stdout: bool,
        **kwargs,
    ) -> BaseCoordinator.ExecutionResult:
        # TODO: Importing this at the top causes circular imports.
        # ActivitySubprocess and WatchedSubprocess should be moved out of the
        # supervisor, and maybe with additional refactoring to abstract out
        # process handling.
        from airflow.sdk.execution_time.supervisor import ActivitySubprocess

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


@functools.cache
def _build_python_coordinator() -> _PythonCoordinator:
    return _PythonCoordinator()


@attrs.define
class CoordinatorManager:
    """
    Registry of coordinator instances loaded from the ``[sdk] coordinators`` config.

    Each entry in the JSON list takes the form::

        {
            "name": "jdk-11",
            "classpath": "airflow.sdk.coordinators.java.JavaCoordinator",
            "kwargs": {"java_executable": "/usr/lib/jvm/jdk-11/bin/java", ...}
        }

    The ``classpath`` is resolved via
    :func:`~airflow.sdk._shared.module_loading.import_string` (no
    :class:`ProvidersManager` involvement) and constructed with ``kwargs``.

    The ``[sdk] queue_to_coordinator`` config maps queue names to a coordinator
    ``name`` from that list, which lets users reuse existing queue assignments
    to route tasks to a specific coordinator instance (for example, a
    ``"legacy-java"`` queue routed to a JDK 11 coordinator and a
    ``"modern-java"`` queue routed to a JDK 17 coordinator).

    :meta private:
    """

    _queue_to_coordinator: Mapping[str, BaseCoordinator]

    @classmethod
    def from_config(cls) -> Self:
        """Load coordinator instances from the ``[sdk]`` configuration."""
        from airflow.sdk.configuration import conf

        coordinator_entry_list = conf.getjson("sdk", "coordinators", fallback=[])
        if not isinstance(coordinator_entry_list, list):
            coordinator_entries = {}
        else:
            coordinator_entries = {d["name"]: d for d in coordinator_entry_list if "name" in d}

        queue_mapping = conf.getjson("sdk", "queue_to_coordinator", fallback={})
        if not isinstance(queue_mapping, dict):
            queue_mapping = {}

        def _build_coordinator(key: str) -> BaseCoordinator:
            entry = coordinator_entries[key]
            coordinator_cls = import_string(entry["classpath"])
            return coordinator_cls(**entry["kwargs"])

        queue_to_coordinator = {
            queue: _build_coordinator(coordinator_key)
            for queue, coordinator_key in queue_mapping.items()
            if coordinator_key in coordinator_entries
        }
        return cls(queue_to_coordinator)

    def for_queue(self, queue: str) -> BaseCoordinator:
        """
        Find the coordinator for *queue*.

        If an entry is not registered, a Python coordinator is returned.
        """
        return self._queue_to_coordinator.get(queue) or _build_python_coordinator()


@functools.cache
def get_coordinator_manager() -> CoordinatorManager:
    """Return the process-wide :class:`CoordinatorManager`, loaded from config on first use."""
    return CoordinatorManager.from_config()


def reset_coordinator_manager() -> None:
    """Clear the cached :class:`CoordinatorManager` (test helper)."""
    get_coordinator_manager.cache_clear()


__all__ = [
    "BaseCoordinator",
    "CoordinatorManager",
    "get_coordinator_manager",
    "reset_coordinator_manager",
]
