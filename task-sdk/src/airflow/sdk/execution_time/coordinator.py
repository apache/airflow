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
"""

from __future__ import annotations

import contextlib
import functools
from typing import TYPE_CHECKING, Any

import attrs
import pydantic
import structlog

from airflow.sdk._shared.module_loading import import_string
from airflow.sdk.configuration import conf

if TYPE_CHECKING:
    from collections.abc import Mapping
    from os import PathLike

    from structlog.typing import FilteringBoundLogger
    from typing_extensions import Self

    from airflow.sdk.api.client import Client
    from airflow.sdk.api.datamodels._generated import TaskInstance

__all__ = [
    "BaseCoordinator",
    "CoordinatorManager",
    "get_coordinator_manager",
    "reset_coordinator_manager",
]

log = structlog.get_logger(__name__)


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
        what: TaskInstance,
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


class _CoordinatorSpec(pydantic.BaseModel):
    classpath: str
    kwargs: dict[str, Any] = pydantic.Field(default_factory=dict)
    # Optional metadata read by other components; kept separate from ``kwargs``
    # so it is never passed to the coordinator constructor.
    extra: dict[str, Any] | None = None


class _PythonCoordinator(BaseCoordinator):
    """
    Coordinator implementation to execute Python tasks.

    This is not supposed to be specified by users directly, but the fallback
    used by default when nothing is specified.
    """

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


class InvalidCoordinatorError(ValueError):
    """Raised for an invalid coordinator configuration."""


@attrs.define(kw_only=True)
class CoordinatorManager:
    """
    Registry of coordinator instances loaded from ``[sdk]`` configurations.

    The ``[sdk] coordinators`` value is a JSON object keyed by coordinator name::

        {
            "jdk-11": {
                "classpath": "airflow.sdk.coordinators.java.JavaCoordinator",
                "kwargs": {"java_executable": "/usr/lib/jvm/jdk-11/bin/java", ...},
            }
        }

    The ``classpath`` is resolved via
    :func:`~airflow.sdk._shared.module_loading.import_string` and constructed
    with ``kwargs`` on first use. A coordinator entry that is never looked up
    incurs no startup cost.

    The ``[sdk] queue_to_coordinator`` config maps queue names to a key in the
    object, which lets users reuse existing queue assignments to route tasks to
    a specific coordinator instance (for example, a ``"legacy-java"`` queue
    routed to a JDK 11 coordinator, and a ``"modern-java"`` queue routed to a
    JDK 17 coordinator).

    A coordinator entry may also carry an optional ``extra`` mapping: metadata
    that other components read as needed. It is kept separate from ``kwargs`` and
    never passed to the coordinator constructor::

        {
            "java": {
                "classpath": "airflow.sdk.coordinators.java.JavaCoordinator",
                "kwargs": {...},
                "extra": {"pod_template_file": "/opt/airflow/pod_templates/java.yaml"},
            }
        }

    :meta private:
    """

    _coordinator_specs: Mapping[str, _CoordinatorSpec]
    _queue_to_coordinator: Mapping[str, str]

    _created_coordinators: dict[str, BaseCoordinator] = attrs.field(init=False, factory=dict)

    @classmethod
    def from_config(cls) -> Self:
        """Load coordinator specs from configuration without initialization."""
        coordinator_specs = {
            k: _CoordinatorSpec.model_validate(v)
            for k, v in conf.getjson("sdk", "coordinators", fallback={}).items()
        }
        queue_to_coordinator = conf.getjson("sdk", "queue_to_coordinator", fallback={})
        for key in queue_to_coordinator.values():
            if key not in coordinator_specs:
                raise ValueError(f"[sdk] queue_to_coordinator references invalid coordinator key: {key!r}")
        return cls(coordinator_specs=coordinator_specs, queue_to_coordinator=queue_to_coordinator)

    def _find_queue(self, key: str) -> BaseCoordinator:
        with contextlib.suppress(KeyError):
            return self._created_coordinators[key]
        spec = self._coordinator_specs[key]
        coordinator = self._created_coordinators[key] = import_string(spec.classpath)(**spec.kwargs)
        return coordinator

    def for_queue(self, queue: str) -> BaseCoordinator:
        """
        Find the coordinator for *queue*.

        If an entry is not registered, a Python coordinator is returned.
        """
        try:
            key = self._queue_to_coordinator[queue]
        except KeyError:
            log.debug("Queue not configured to a coordinator; defaulting to Python", queue=queue)
            return _build_python_coordinator()
        try:
            coordinator = self._find_queue(key)
        except KeyError:
            raise InvalidCoordinatorError(f"Queue {queue!r} configured to nonexistent coordinator")
        except ImportError:
            raise InvalidCoordinatorError(f"Cannot import coordinator {key!r}")
        except TypeError:
            raise InvalidCoordinatorError(f"Cannot instantiate coordinator {key!r}")
        log.debug("Coordinator found for queue", coordinator=coordinator, queue=queue)
        return coordinator

    def extra_for_queue(self, queue: str) -> dict[str, Any] | None:
        """
        Return the optional ``extra`` mapping configured for *queue*'s coordinator.

        Returns ``None`` when the queue is not routed to a coordinator or its
        coordinator declares no ``extra``. Only the declarative spec is read; the
        coordinator is never instantiated.
        """
        if (key := self._queue_to_coordinator.get(queue)) is None:
            return None
        if (spec := self._coordinator_specs.get(key)) is None:
            return None
        return spec.extra


@functools.cache
def get_coordinator_manager() -> CoordinatorManager:
    """Return the process-wide :class:`CoordinatorManager`, loaded from config on first use."""
    return CoordinatorManager.from_config()


def reset_coordinator_manager() -> None:
    """Clear the cached :class:`CoordinatorManager` (test helper)."""
    get_coordinator_manager.cache_clear()
