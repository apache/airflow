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

A coordinator executes a task through :meth:`~BaseCoordinator.execute_task`. A
coordinator that also parses native Dags hands out a Dag importer through
:meth:`~BaseCoordinator.get_dag_importer`, and :meth:`CoordinatorManager.for_bundle`
selects the coordinators whose importers parse a Dag bundle.
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

from airflow.sdk._shared.module_loading import import_string
from airflow.sdk.configuration import conf

if TYPE_CHECKING:
    from collections.abc import Generator, Mapping
    from os import PathLike

    from structlog.typing import FilteringBoundLogger
    from typing_extensions import Self

    from airflow.sdk.api.client import Client
    from airflow.sdk.api.datamodels._generated import TaskInstance
    from airflow.sdk.importers import AbstractDagImporter

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

    @classmethod
    def get_dag_importer_class(cls) -> type[AbstractDagImporter] | None:
        """
        Return the class of the Dag importer that parses this coordinator's native Dag files.

        ``None``, the default, means the coordinator parses no native Dags.
        """
        return None

    def get_dag_importer(self) -> AbstractDagImporter | None:
        """Return the Dag importer that parses this coordinator's native Dag files, if any."""
        return None

    @classmethod
    def get_parsed_bundles(cls, kwargs: Mapping[str, Any]) -> frozenset[str] | None:
        """
        Return the Dag bundles whose files a coordinator built with *kwargs* parses.

        ``None`` means every bundle. It agrees with :meth:`serves_bundle`, so the configuration can be
        checked without building the coordinator.
        """
        return frozenset()

    def serves_bundle(self, bundle_name: str) -> bool:
        """Return whether this coordinator's Dag importer parses Dag files in *bundle_name*."""
        return False


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
        cls._check_dag_file_claims(coordinator_specs)
        return cls(coordinator_specs=coordinator_specs, queue_to_coordinator=queue_to_coordinator)

    @staticmethod
    def _check_dag_file_claims(coordinator_specs: Mapping[str, _CoordinatorSpec]) -> None:
        """
        Reject two coordinators whose Dag importers parse the same extension in the same Dag bundle.

        Only one runtime can parse a file. The check reads the coordinator classes and their specs
        and builds no coordinator. A class that cannot be imported is skipped, since using it reports
        the error.

        :raises InvalidCoordinatorError: if two coordinators claim the same extension in a bundle.
        """
        # circular: importers.base imports this module at load time
        from airflow.sdk.importers.base import normalize_extensions

        claims: list[tuple[str, frozenset[str] | None, frozenset[str]]] = []
        for key, spec in coordinator_specs.items():
            try:
                coordinator_cls: type[BaseCoordinator] = import_string(spec.classpath)
            except ImportError:
                continue
            if (importer_cls := coordinator_cls.get_dag_importer_class()) is None:
                continue
            bundles = coordinator_cls.get_parsed_bundles(spec.kwargs)
            if bundles is not None and not bundles:
                continue
            extensions = frozenset(normalize_extensions(getattr(importer_cls, "supported_extensions", ())))
            for other_key, other_bundles, other_extensions in claims:
                shared = extensions & other_extensions
                if shared and (bundles is None or other_bundles is None or bundles & other_bundles):
                    raise InvalidCoordinatorError(
                        f"Coordinators {other_key!r} and {key!r} both parse {', '.join(sorted(shared))} "
                        "files in the same Dag bundle. Give each coordinator its own 'dag_bundle_name'."
                    )
            claims.append((key, bundles, extensions))

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

    def for_bundle(self, bundle_name: str) -> dict[str, BaseCoordinator]:
        """
        Return the coordinators that parse Dag files in *bundle_name*, keyed by their config key.

        Every configured coordinator is built to learn whether it serves the bundle, in config
        order. One that cannot be built is logged and skipped.
        """
        coordinators: dict[str, BaseCoordinator] = {}
        for key in self._coordinator_specs:
            try:
                coordinator = self._find_queue(key)
            except Exception:
                log.exception("Cannot load coordinator; skipping it for Dag parsing", coordinator=key)
                continue
            if coordinator.serves_bundle(bundle_name):
                coordinators[key] = coordinator
        return coordinators

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

    The cached Dag importer registries hold importers bound to its coordinators, so they are
    cleared too.
    """
    # circular: importers.base imports this module at load time
    from airflow.sdk.importers.base import reset_importer_registry

    reset_importer_registry()
