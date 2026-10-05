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
Runtime coordinators for non-Python Dag file processing and task execution.

Provides :class:`BaseCoordinator`, the base class for SDK-specific coordinators
that run an external-SDK runtime (Java, Go, TypeScript, etc.) for the Airflow
supervisor, and :class:`CoordinatorManager`, the registry that loads coordinator
instances from the ``[sdk] coordinators`` configuration.

A coordinator executes a task through :meth:`~BaseCoordinator.execute_task`.
:meth:`CoordinatorManager.get_dag_parsing_coordinator_key` picks the coordinator
that parses the native Dag files of a Dag bundle.
"""

from __future__ import annotations

import contextlib
import functools
import os
import signal
from typing import TYPE_CHECKING, Any

import attrs
import pydantic
import structlog

from airflow.dag_processing.bundles.manager import DagBundlesManager  # noqa: SDK002
from airflow.sdk._shared.module_loading import import_string
from airflow.sdk.configuration import conf
from airflow.sdk.exceptions import AirflowConfigException

if TYPE_CHECKING:
    from collections.abc import Generator, Mapping
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


@contextlib.contextmanager
def _warm_shutdown_signals() -> Generator[None, None, None]:
    """
    Install SIGTERM/SIGINT warm-shutdown handlers for the duration of task supervision.

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
    with ``kwargs`` on first use. A coordinator is built only when a task routed
    to its queue, or a Dag file it parses, needs it.

    The ``[sdk] queue_to_coordinator`` config maps queue names to a key in the
    object, which lets users reuse existing queue assignments to route tasks to
    a specific coordinator instance (for example, a ``"legacy-java"`` queue
    routed to a JDK 11 coordinator, and a ``"modern-java"`` queue routed to a
    JDK 17 coordinator).

    The ``[sdk] dag_bundle_to_coordinator`` config maps a Dag bundle name to one
    key in the object. It picks the coordinator that parses the bundle's native
    Dag files when several coordinators of that class are configured, and tasks
    never read it.

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
    _coordinator_classes: dict[str, type | None] = attrs.field(init=False, factory=dict)
    _dag_bundle_to_coordinator: dict[str, str] | None = attrs.field(init=False, default=None)

    @classmethod
    def from_config(cls) -> Self:
        """
        Load coordinator specs from configuration without initialization.

        Every ``queue_to_coordinator`` key and the ``task_handler_bundle_name`` of
        every routed coordinator are validated here, so a typo fails at config load
        rather than on the first task routed to the coordinator.
        """
        coordinator_specs = {
            k: _CoordinatorSpec.model_validate(v)
            for k, v in conf.getjson("sdk", "coordinators", fallback={}).items()
        }
        queue_to_coordinator = conf.getjson("sdk", "queue_to_coordinator", fallback={})
        for key in set(queue_to_coordinator.values()):
            if (spec := coordinator_specs.get(key)) is None:
                raise ValueError(f"[sdk] queue_to_coordinator references invalid coordinator key: {key!r}")
            bundle_name = spec.kwargs.get("task_handler_bundle_name")
            if bundle_name is not None and (
                not isinstance(bundle_name, str) or not DagBundlesManager.is_bundle_configured(bundle_name)
            ):
                raise InvalidCoordinatorError(
                    f"[sdk] coordinators {key!r} sets task_handler_bundle_name={bundle_name!r}, "
                    f"which is not a bundle in [dag_processor] dag_bundle_config_list"
                )
        return cls(coordinator_specs=coordinator_specs, queue_to_coordinator=queue_to_coordinator)

    def has_coordinators(self) -> bool:
        """Return whether ``[sdk] coordinators`` configures any coordinator."""
        return bool(self._coordinator_specs)

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
        if key not in self._coordinator_specs:
            raise InvalidCoordinatorError(f"Queue {queue!r} configured to nonexistent coordinator")
        coordinator = self.get_coordinator(key)
        log.debug("Coordinator found for queue", coordinator=coordinator, queue=queue)
        return coordinator

    def get_coordinator(self, key: str) -> BaseCoordinator:
        """
        Return the coordinator configured under *key* in ``[sdk] coordinators``, building it on first use.

        :raises InvalidCoordinatorError: when *key* is not configured, or its class cannot be imported or
            called with its kwargs. Other errors from building the coordinator propagate.
        """
        with contextlib.suppress(KeyError):
            return self._created_coordinators[key]
        try:
            spec = self._coordinator_specs[key]
        except KeyError:
            raise InvalidCoordinatorError(f"No coordinator {key!r} in [sdk] coordinators")
        try:
            coordinator = import_string(spec.classpath)(**spec.kwargs)
        except ImportError:
            raise InvalidCoordinatorError(f"Cannot import coordinator {key!r}")
        except TypeError:
            raise InvalidCoordinatorError(f"Cannot instantiate coordinator {key!r}")
        self._created_coordinators[key] = coordinator
        return coordinator

    def _get_coordinator_class(self, key: str) -> type | None:
        """
        Return the class of the coordinator under *key* without building it.

        ``None`` means no class can be found: *key* is not configured, its classpath cannot be
        imported or fails while importing, or it is not a class. The reason is logged once.
        """
        with contextlib.suppress(KeyError):
            return self._coordinator_classes[key]
        coordinator_class: type | None = None
        if (spec := self._coordinator_specs.get(key)) is None:
            log.error("No coordinator in [sdk] coordinators", coordinator=key)
        else:
            try:
                resolved = import_string(spec.classpath)
            except Exception:
                log.exception("Cannot import coordinator", coordinator=key, classpath=spec.classpath)
            else:
                if isinstance(resolved, type):
                    coordinator_class = resolved
                else:
                    log.error(
                        "Coordinator classpath is not a class", coordinator=key, classpath=spec.classpath
                    )
        self._coordinator_classes[key] = coordinator_class
        return coordinator_class

    def get_coordinator_keys_for_class(self, coordinator_classpath: str) -> list[str]:
        """
        Return, in config order, the keys of the coordinators of the class at *coordinator_classpath*.

        A coordinator is of the class when its class is that class or a subclass of it. Nothing is built.
        """
        target = import_string(coordinator_classpath)
        return [
            key
            for key in self._coordinator_specs
            if (coordinator_class := self._get_coordinator_class(key)) is not None
            and issubclass(coordinator_class, target)
        ]

    def _get_dag_bundle_to_coordinator(self) -> dict[str, str]:
        if self._dag_bundle_to_coordinator is None:
            try:
                mapping = conf.getjson("sdk", "dag_bundle_to_coordinator", fallback={})
            except AirflowConfigException as e:
                raise InvalidCoordinatorError(str(e)) from e
            if not isinstance(mapping, dict) or not all(isinstance(key, str) for key in mapping.values()):
                raise InvalidCoordinatorError(
                    "[sdk] dag_bundle_to_coordinator must be a JSON object that maps Dag bundle names "
                    "to coordinator keys"
                )
            self._dag_bundle_to_coordinator = mapping
        return self._dag_bundle_to_coordinator

    def get_dag_parsing_coordinator_key(self, coordinator_classpath: str, bundle_name: str) -> str:
        """
        Return the key of the coordinator that parses the Dag files of its class in *bundle_name*.

        With one coordinator of the class at *coordinator_classpath*, that one parses them in every
        Dag bundle. With several, ``[sdk] dag_bundle_to_coordinator`` must map *bundle_name* to one of
        them.

        :raises InvalidCoordinatorError: when no single coordinator of the class can parse the bundle.
        """
        keys = self.get_coordinator_keys_for_class(coordinator_classpath)
        if len(keys) == 1:
            return keys[0]
        kind = coordinator_classpath.rsplit(".", 1)[-1]
        if not keys:
            raise InvalidCoordinatorError(f"[sdk] coordinators has no {kind}")
        mapped = self._get_dag_bundle_to_coordinator().get(bundle_name)
        if mapped in keys:
            return mapped
        if mapped is None:
            fix = "Map the bundle to one of them in [sdk] dag_bundle_to_coordinator."
        elif self._get_coordinator_class(mapped) is None:
            fix = f"[sdk] dag_bundle_to_coordinator maps it to {mapped!r}, which cannot be loaded."
        else:
            fix = (
                f"[sdk] dag_bundle_to_coordinator maps it to {mapped!r}, a coordinator of another class. "
                f"Move these files to another Dag bundle, or keep one {kind}."
            )
        raise InvalidCoordinatorError(
            f"Dag bundle {bundle_name!r} has {len(keys)} {kind} coordinators ({', '.join(keys)}). {fix}"
        )

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
    """
    Clear the cached :class:`CoordinatorManager` (test helper).

    The Dag importers a registry holds depend on the configured coordinators, so the cached
    registries are cleared too.
    """
    # circular: importers.base imports this module at load time
    from airflow.sdk.importers.base import reset_importer_registry

    reset_importer_registry()
