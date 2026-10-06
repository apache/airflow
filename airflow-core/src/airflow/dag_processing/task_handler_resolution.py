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
"""Resolve a Dag file's stub tasks to the task handler artifacts their coordinators would run."""

from __future__ import annotations

import os
import time
from collections import defaultdict
from typing import TYPE_CHECKING

from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.dag_processing.dagbag import _get_bundle_team_name
from airflow.dag_processing.task_handler_validation import (
    TaskHandlerAnswer,
    TaskHandlerProblem,
    format_import_errors,
    match_task_handlers,
)
from airflow.sdk.coordinators._subprocess import (  # noqa: SDK001
    SubprocessCoordinator,
    supports_task_handler_parsing,
)

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from structlog.typing import FilteringBoundLogger

    from airflow.dag_processing.task_handler_validation import StubTask, StubTaskWarning
    from airflow.sdk.execution_time.coordinator import CoordinatorManager  # noqa: SDK001


class _NotChecked(Exception):
    """Why one (coordinator, Dag) group's stub tasks were not checked against their task handlers."""

    def __init__(self, reason: str, *, level: str = "warning") -> None:
        super().__init__(reason)
        self.level = level
        """``"warning"``, ``"info"`` or ``"debug"``: how loudly the group is logged as unchecked."""


class _LazyDagBundlesManager:
    """Builds a ``DagBundlesManager`` at most once, the first time a named bundle must be read."""

    def __init__(self) -> None:
        self._manager: DagBundlesManager | None = None

    def get(self) -> DagBundlesManager:
        if self._manager is None:
            self._manager = DagBundlesManager()
        return self._manager


def resolve_task_handlers(
    stub_tasks: Sequence[StubTask],
    *,
    dag_bundle_name: str,
    dag_bundle_path: Path,
    deadline: float,
    log: FilteringBoundLogger,
) -> dict[str, str]:
    """
    Check *stub_tasks* against the task handlers of the artifact each one's worker would run.

    Stub tasks are grouped by the coordinator their queue routes to and by their Dag; a stub task on
    an unrouted queue is left to a worker outside Airflow's coordinators. Each group's artifact is
    found with the coordinator's own scan, in the Dag bundle named by its ``task_handler_bundle_name``
    or in the Dag's own bundle, and each distinct artifact is probed once, until *deadline*, a
    :func:`time.monotonic` value. Anything that leaves a group unchecked is logged, never an import
    error. The import errors returned are keyed by Dag file.
    """
    from airflow.sdk.execution_time.coordinator import get_coordinator_manager  # noqa: SDK001

    try:
        manager = get_coordinator_manager()
    except Exception:
        log.warning("Cannot load [sdk] coordinators, so no stub task is checked", exc_info=True)
        return {}

    groups: defaultdict[tuple[str, str], list[StubTask]] = defaultdict(list)
    for stub in stub_tasks:
        key = manager.get_coordinator_key(stub.queue)
        if key is not None:
            groups[key, stub.dag_id].append(stub)
    if not groups:
        return {}

    coordinators: dict[str, SubprocessCoordinator | _NotChecked] = {}
    bundles: dict[str, tuple[str, Path] | _NotChecked] = {}
    probes: dict[tuple[str, str, str], TaskHandlerAnswer | _NotChecked] = {}
    dag_bundles_manager = _LazyDagBundlesManager()

    problems: list[TaskHandlerProblem] = []
    for key, dag_id in sorted(groups):
        stubs = groups[key, dag_id]
        try:
            if time.monotonic() >= deadline:
                raise _NotChecked(
                    "the parse ran out of [dag_processor] dag_file_processor_timeout before checking "
                    f"coordinator {key!r}'s task handlers for Dag {dag_id!r}"
                )
            coordinator = _get_coordinator(coordinators, manager, key)
            bundle_name, root = _get_bundle(
                bundles,
                coordinator,
                dag_bundle_name=dag_bundle_name,
                dag_bundle_path=dag_bundle_path,
                dag_bundles_manager=dag_bundles_manager,
            )
            rel_path = _find_artifact(coordinator, key=key, dag_id=dag_id, bundle_name=bundle_name, root=root)
            answer = _probe(
                probes,
                key=key,
                bundle_name=bundle_name,
                rel_path=rel_path,
                root=root,
                deadline=deadline,
                log=log,
            )
        except _NotChecked as e:
            getattr(log, e.level)(
                "Not checking a Dag's stub tasks against their task handlers",
                reason=str(e),
                coordinator=key,
                dag_id=dag_id,
            )
            continue
        match = match_task_handlers(stubs, answer)
        problems.extend(match.problems)
        for warning in match.warnings:
            _log_name_mismatch(warning, log)
    return format_import_errors(problems)


def _does_not_probe_message(coordinator: object, key: str) -> str:
    classpath = f"{type(coordinator).__module__}.{type(coordinator).__qualname__}"
    return f"coordinator {key!r} ({classpath}) does not probe task handlers"


def _get_coordinator(
    cache: dict[str, SubprocessCoordinator | _NotChecked], manager: CoordinatorManager, key: str
) -> SubprocessCoordinator:
    if key not in cache:
        try:
            built = manager.get_coordinator(key)
        except Exception as e:
            cache[key] = _NotChecked(f"coordinator {key!r} cannot be built: {_describe(e)}")
        else:
            if isinstance(built, SubprocessCoordinator):
                cache[key] = built
            else:
                cache[key] = _NotChecked(_does_not_probe_message(built, key), level="debug")
    cached = cache[key]
    if isinstance(cached, _NotChecked):
        raise cached
    return cached


def _get_bundle(
    cache: dict[str, tuple[str, Path] | _NotChecked],
    coordinator: SubprocessCoordinator,
    *,
    dag_bundle_name: str,
    dag_bundle_path: Path,
    dag_bundles_manager: _LazyDagBundlesManager,
) -> tuple[str, Path]:
    name = coordinator.task_handler_bundle_name
    if name is None or name == dag_bundle_name:
        return dag_bundle_name, dag_bundle_path.resolve()
    if name not in cache:
        try:
            cache[name] = _resolve_named_bundle(
                name, dag_bundle_name=dag_bundle_name, dag_bundles_manager=dag_bundles_manager
            )
        except _NotChecked as e:
            cache[name] = e
    cached = cache[name]
    if isinstance(cached, _NotChecked):
        raise cached
    return cached


def _resolve_named_bundle(
    name: str, *, dag_bundle_name: str, dag_bundles_manager: _LazyDagBundlesManager
) -> tuple[str, Path]:
    if not DagBundlesManager.is_bundle_configured(name):
        raise _NotChecked(f"Dag bundle {name!r} is not configured on this Dag processor")
    try:
        bundle_team = _get_bundle_team_name(name) or None
        dag_team = _get_bundle_team_name(dag_bundle_name) or None
    except Exception as e:
        raise _NotChecked(
            f"cannot read the teams of Dag bundles {name!r} and {dag_bundle_name!r}: {_describe(e)}"
        ) from e
    if bundle_team != dag_team:
        raise _NotChecked(
            f"Dag bundle {name!r} belongs to {_describe_team(bundle_team)}, "
            f"but Dag bundle {dag_bundle_name!r} belongs to {_describe_team(dag_team)}"
        )
    try:
        root = dag_bundles_manager.get().get_bundle(name).path.resolve()
    except Exception as e:
        raise _NotChecked(f"cannot read Dag bundle {name!r}: {_describe(e)}") from e
    try:
        os.scandir(root).close()
    except (FileNotFoundError, NotADirectoryError):
        raise _NotChecked(
            f"Dag bundle {name!r} resolved to {root}, which does not exist on this Dag processor"
        ) from None
    except OSError as e:
        raise _NotChecked(
            f"Dag bundle {name!r} at {root} cannot be read on this Dag processor: {_describe(e)}"
        ) from e
    return name, root


def _find_artifact(
    coordinator: SubprocessCoordinator, *, key: str, dag_id: str, bundle_name: str, root: Path
) -> str:
    try:
        resolved = coordinator._find_task_handler_artifact(bundle_path=root, dag_id=dag_id)
    except NotImplementedError:
        raise _NotChecked(_does_not_probe_message(coordinator, key), level="debug") from None
    except Exception as e:
        raise _NotChecked(
            f"no task handler artifact for Dag {dag_id!r} in Dag bundle {bundle_name!r}: {_describe(e)}"
        ) from e
    try:
        rel_path = os.fspath(resolved.path.relative_to(root))
    except ValueError:
        raise _NotChecked(
            f"the task handler artifact {resolved.path} of Dag {dag_id!r} is not inside Dag bundle "
            f"{bundle_name!r} at {root}"
        ) from None
    if not supports_task_handler_parsing(resolved.schema_version):
        raise _NotChecked(
            f"the task handler artifact {rel_path!r} in Dag bundle {bundle_name!r} uses supervisor "
            f"schema {resolved.schema_version}, which cannot answer a task handler parse request",
            level="info",
        )
    return rel_path


def _probe(
    cache: dict[tuple[str, str, str], TaskHandlerAnswer | _NotChecked],
    *,
    key: str,
    bundle_name: str,
    rel_path: str,
    root: Path,
    deadline: float,
    log: FilteringBoundLogger,
) -> TaskHandlerAnswer:
    cache_key = (key, bundle_name, rel_path)
    if cache_key not in cache:
        try:
            cache[cache_key] = _run_probe(
                key=key, bundle_name=bundle_name, rel_path=rel_path, root=root, deadline=deadline, log=log
            )
        except _NotChecked as e:
            cache[cache_key] = e
    cached = cache[cache_key]
    if isinstance(cached, _NotChecked):
        raise cached
    return cached


def _run_probe(
    *, key: str, bundle_name: str, rel_path: str, root: Path, deadline: float, log: FilteringBoundLogger
) -> TaskHandlerAnswer:
    from airflow.dag_processing.task_handler_processor import LangSDKTaskHandlerProcessorProcess

    if time.monotonic() >= deadline:
        raise _NotChecked(
            "the parse ran out of [dag_processor] dag_file_processor_timeout before probing "
            f"{rel_path!r} in Dag bundle {bundle_name!r}"
        )
    context = {"coordinator": key, "bundle_name": bundle_name, "path": rel_path}
    log.info("Probing a task handler artifact", **context)
    started = time.monotonic()
    try:
        result = LangSDKTaskHandlerProcessorProcess.run(
            coordinator=key,
            path=root / rel_path,
            bundle_path=root,
            bundle_name=bundle_name,
            artifact_rel_path=rel_path,
            logger=log,
            deadline=deadline,
        )
    except Exception as e:
        raise _NotChecked(f"probing {rel_path!r} in Dag bundle {bundle_name!r} raised: {_describe(e)}") from e
    for warning in result.warnings or ():
        log.warning("The task handler runtime reported a warning", warning=warning, **context)
    if result.import_errors:
        raise _NotChecked(
            f"probing {rel_path!r} in Dag bundle {bundle_name!r} gave no answer: "
            f"{'; '.join(result.import_errors.values())}"
        )
    log.info("Probed a task handler artifact", seconds=round(time.monotonic() - started, 3), **context)
    return TaskHandlerAnswer(bundle_name=bundle_name, rel_path=rel_path, task_handlers=result.task_handlers)


def _log_name_mismatch(warning: StubTaskWarning, log: FilteringBoundLogger) -> None:
    context = {
        "dag_id": warning.dag_id,
        "task_id": warning.task_id,
        "artifact_bundle_name": warning.artifact_bundle_name,
        "artifact_rel_path": warning.artifact_rel_path,
    }
    if warning.passed_not_declared:
        log.warning(
            "Dag's call passed argument(s) the task handler does not declare",
            passed_not_declared=warning.passed_not_declared,
            **context,
        )
    if warning.declared_not_passed:
        log.warning(
            "Task handler declares argument(s) the Dag's call did not pass",
            declared_not_passed=warning.declared_not_passed,
            **context,
        )


def _describe(error: BaseException) -> str:
    """Return *error* with its type, and the error it was raised from, if any."""
    description = f"{type(error).__name__}: {error}"
    cause = error.__cause__ or (None if error.__suppress_context__ else error.__context__)
    if cause is not None:
        description = f"{description} ({type(cause).__name__}: {cause})"
    return description


def _describe_team(team_name: str | None) -> str:
    return "no team" if team_name is None else f"team {team_name!r}"
