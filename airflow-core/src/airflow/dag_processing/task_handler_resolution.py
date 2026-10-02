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
"""Resolve a Dag file's stub tasks to the task handler artifacts of the coordinators their queues route to."""

from __future__ import annotations

import os
import time
from collections import defaultdict
from typing import TYPE_CHECKING

import attrs
from pydantic import ValidationError

from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.dag_processing.processor import TaskHandlerArtifact
from airflow.dag_processing.task_handler_fast_path import plan_task_handler_probes
from airflow.dag_processing.task_handler_validation import (
    TaskHandlerProblem,
    format_import_errors,
    match_task_handlers,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence
    from pathlib import Path

    from structlog.typing import FilteringBoundLogger

    from airflow.dag_processing.processor import TaskHandlerBinding
    from airflow.dag_processing.task_handler_validation import StubTask, StubTaskWarning
    from airflow.sdk.execution_time.coordinator import (  # noqa: SDK001
        CoordinatorManager,
        TaskHandlerCandidate,
    )

_OUT_OF_TIME = "the parse ran out of [dag_processor] dag_file_processor_timeout"


@attrs.frozen(kw_only=True)
class TaskHandlerResolution:
    """What a Dag file's parse reports about the task handlers of its stub tasks."""

    bindings: list[TaskHandlerBinding] | None
    """A binding for each stub task checked; ``None`` when they were not checked, or a check failed."""

    probed_artifacts: list[TaskHandlerArtifact]
    """Every artifact probed with an answer, kept even when a check failed."""

    import_errors: dict[str, str]
    """The problems found, as one import error per Dag file."""

    @classmethod
    def failed(cls, stub_tasks: Iterable[StubTask], message: str) -> TaskHandlerResolution:
        """Return a resolution that reports *message* as a problem of each Dag file with a stub task."""
        return cls(
            bindings=None,
            probed_artifacts=[],
            import_errors=format_import_errors(_make_problems(stub_tasks, message)),
        )


@attrs.frozen(kw_only=True)
class _CoordinatorArtifacts:
    """The candidates one coordinator lists in its Dag bundle, sorted by what the parse does with them."""

    key: str
    bundle_name: str
    bundle_path: Path
    cached: list[TaskHandlerArtifact]
    probe: list[TaskHandlerCandidate]
    broken: dict[str, str]
    """Candidates the coordinator cannot probe, with why."""


class _CoordinatorProblem(Exception):
    """Why a coordinator's stub tasks cannot be checked."""


@attrs.frozen(kw_only=True)
class _Probes:
    """What probing the candidates gave."""

    answers: dict[tuple[str, str], TaskHandlerArtifact | str]
    """
    By Dag bundle name and path: the answer, or why the deadline left the candidate unprobed.

    Every coordinator that lists the candidate uses the answer. A skip stands for a coordinator only when
    its own probe did not fail first, so a skip left by a later coordinator hides no earlier failure.
    """

    failures: dict[tuple[str, str, str], str]
    """Why a probe gave no answer, by coordinator key, Dag bundle name and path."""

    def get(self, coordinator: str, bundle_name: str, rel_path: str) -> TaskHandlerArtifact | str:
        """Return *coordinator*'s answer for the candidate, or why it has none."""
        answer = self.answers.get((bundle_name, rel_path))
        if isinstance(answer, TaskHandlerArtifact):
            return answer
        if (failure := self.failures.get((coordinator, bundle_name, rel_path))) is not None:
            return failure
        return self.answers[bundle_name, rel_path]


class _DagBundles:
    """Where the Dag bundles the coordinators name are, looked up once each in a parse."""

    def __init__(self, dag_bundle_name: str) -> None:
        self._dag_bundle_name = dag_bundle_name
        self._manager: DagBundlesManager | None = None
        self._paths: dict[str, Path | str] = {}

    def get_path(self, name: str) -> Path:
        """Return where the Dag bundle *name* is, if it belongs to the team of the Dag's bundle."""
        if name not in self._paths:
            try:
                self._paths[name] = self._find_path(name)
            except _CoordinatorProblem as e:
                self._paths[name] = str(e)
        path = self._paths[name]
        if isinstance(path, str):
            raise _CoordinatorProblem(path)
        return path

    def _find_path(self, name: str) -> Path:
        try:
            if self._manager is None:
                self._manager = DagBundlesManager()
            team_name = self._manager.get_bundle_team_name(name)
            dag_team_name = self._manager.get_bundle_team_name(self._dag_bundle_name)
        except Exception as e:
            raise _CoordinatorProblem(
                f"cannot read the teams of Dag bundles {name!r} and {self._dag_bundle_name!r}: {_describe(e)}"
            ) from e
        if team_name != dag_team_name:
            raise _CoordinatorProblem(
                f"Dag bundle {name!r} belongs to {_describe_team(team_name)}, "
                f"but Dag bundle {self._dag_bundle_name!r} belongs to {_describe_team(dag_team_name)}"
            )
        try:
            return self._manager.get_bundle(name).path
        except Exception as e:
            raise _CoordinatorProblem(f"cannot read Dag bundle {name!r}: {_describe(e)}") from e


def resolve_task_handlers(
    stub_tasks: Sequence[StubTask],
    *,
    dag_bundle_name: str,
    dag_bundle_path: Path,
    known_artifacts: Sequence[TaskHandlerArtifact],
    deadline: float,
    log: FilteringBoundLogger,
) -> TaskHandlerResolution:
    """
    Check *stub_tasks* against the task handlers of the coordinators their queues route to.

    A coordinator lists its artifacts in the Dag bundle named by its ``task_handler_bundle_name``, which must
    belong to the team of the Dag bundle *dag_bundle_name*, or in that Dag bundle at *dag_bundle_path* when it
    names none. A candidate whose answer in *known_artifacts* still holds is not probed. The others are
    probed one at a time until *deadline*, a :func:`time.monotonic` value. An answer is shared by every
    coordinator that lists the artifact, and a failed probe is retried under the next coordinator that
    lists it. A candidate that cannot be probed, whose probe fails, or that the deadline leaves unprobed
    has no answer, and is named only when a stub task finds no task handler. A stub task on a queue routed
    to no coordinator is not checked. A coordinator that cannot be evaluated is a problem for each Dag
    file with stub tasks on it, and those stub tasks are not checked.
    """
    from airflow.sdk.execution_time.coordinator import get_coordinator_manager  # noqa: SDK001

    try:
        manager = get_coordinator_manager()
    except Exception as e:
        return TaskHandlerResolution.failed(stub_tasks, f"Cannot load [sdk] coordinators: {_describe(e)}")

    stubs_by_coordinator: defaultdict[str, list[StubTask]] = defaultdict(list)
    for stub in stub_tasks:
        if (key := manager.get_coordinator_key(stub.queue)) is not None:
            stubs_by_coordinator[key].append(stub)

    problems: list[TaskHandlerProblem] = []
    coordinators: list[_CoordinatorArtifacts] = []
    bundle_names = manager.get_task_handler_bundle_names()
    bundles = _DagBundles(dag_bundle_name)
    for key in sorted(stubs_by_coordinator):
        try:
            coordinators.append(
                _list_artifacts(
                    manager,
                    key,
                    bundle_name=bundle_names[key],
                    bundles=bundles,
                    dag_bundle_name=dag_bundle_name,
                    dag_bundle_path=dag_bundle_path,
                    known_artifacts=known_artifacts,
                    log=log,
                )
            )
        except _CoordinatorProblem as e:
            problems.extend(_make_problems(stubs_by_coordinator[key], f"Coordinator {key!r}: {e}"))
        except Exception as e:
            log.warning(
                "Cannot list the task handler artifacts of a coordinator", coordinator=key, exc_info=True
            )
            problems.extend(
                _make_problems(
                    stubs_by_coordinator[key],
                    f"Coordinator {key!r}: cannot find its task handler artifacts: {_describe(e)}",
                )
            )

    probes = _probe_candidates(coordinators, deadline=deadline, log=log)

    bindings: list[TaskHandlerBinding] = []
    for coordinator in coordinators:
        coordinator_answers = list(coordinator.cached)
        broken = dict(coordinator.broken)
        for candidate in coordinator.probe:
            answer = probes.get(coordinator.key, coordinator.bundle_name, candidate.rel_path)
            if isinstance(answer, TaskHandlerArtifact):
                coordinator_answers.append(answer)
            else:
                broken[candidate.rel_path] = answer
        match = match_task_handlers(
            stubs_by_coordinator[coordinator.key],
            coordinator_answers,
            bundle_name=coordinator.bundle_name,
            broken_candidates=broken,
        )
        bindings.extend(match.bindings)
        problems.extend(match.problems)
        for warning in match.warnings:
            _log_name_mismatch(warning, log)

    probed_artifacts = [
        answer for answer in probes.answers.values() if isinstance(answer, TaskHandlerArtifact)
    ]
    if problems:
        return TaskHandlerResolution(
            bindings=None, probed_artifacts=probed_artifacts, import_errors=format_import_errors(problems)
        )
    return TaskHandlerResolution(bindings=bindings, probed_artifacts=probed_artifacts, import_errors={})


def _list_artifacts(
    manager: CoordinatorManager,
    key: str,
    *,
    bundle_name: str | None,
    bundles: _DagBundles,
    dag_bundle_name: str,
    dag_bundle_path: Path,
    known_artifacts: Sequence[TaskHandlerArtifact],
    log: FilteringBoundLogger,
) -> _CoordinatorArtifacts:
    if bundle_name is None:
        bundle_name, bundle_path = dag_bundle_name, dag_bundle_path
    else:
        bundle_path = bundles.get_path(bundle_name)
    try:
        os.scandir(bundle_path).close()
    except (FileNotFoundError, NotADirectoryError):
        raise _CoordinatorProblem(
            f"Dag bundle {bundle_name!r} resolved to {bundle_path}, which does not exist on this Dag processor"
        ) from None
    except OSError as e:
        raise _CoordinatorProblem(
            f"Dag bundle {bundle_name!r} at {bundle_path} cannot be read on this Dag processor: {_describe(e)}"
        ) from e
    try:
        coordinator = manager.get_coordinator(key)
    except Exception as e:
        raise _CoordinatorProblem(f"cannot be built: {_describe(e)}") from e
    try:
        candidates = coordinator.list_task_handler_candidates(bundle_path)
    except NotImplementedError:
        classpath = f"{type(coordinator).__module__}.{type(coordinator).__qualname__}"
        raise _CoordinatorProblem(
            f"{classpath} cannot list task handler artifacts, so its stub tasks cannot be bound"
        ) from None
    except Exception as e:
        raise _CoordinatorProblem(
            f"cannot list the task handler artifacts of Dag bundle {bundle_name!r}: {_describe(e)}"
        ) from e

    plan = plan_task_handler_probes(
        bundle_name=bundle_name, candidates=candidates, known_artifacts=known_artifacts
    )
    broken: dict[str, str] = {}
    for candidate in plan.rejected:
        log.warning(
            "Ignoring a task handler artifact its coordinator cannot probe",
            coordinator=key,
            bundle_name=bundle_name,
            path=candidate.rel_path,
            error=candidate.error,
        )
        broken[candidate.rel_path] = str(candidate.error)
    for artifact in plan.cached:
        log.debug(
            "Using the recorded answer of a task handler artifact",
            coordinator=key,
            bundle_name=bundle_name,
            path=artifact.relative_fileloc,
        )
    return _CoordinatorArtifacts(
        key=key,
        bundle_name=bundle_name,
        bundle_path=bundle_path,
        cached=plan.cached,
        probe=plan.probe,
        broken=broken,
    )


def _probe_candidates(
    coordinators: Iterable[_CoordinatorArtifacts], *, deadline: float, log: FilteringBoundLogger
) -> _Probes:
    """Probe each candidate under each coordinator that lists it, until one has an answer."""
    probes = _Probes(answers={}, failures={})
    for coordinator in coordinators:
        for candidate in coordinator.probe:
            if (coordinator.bundle_name, candidate.rel_path) in probes.answers:
                continue
            context = {
                "coordinator": coordinator.key,
                "bundle_name": coordinator.bundle_name,
                "path": candidate.rel_path,
            }
            if time.monotonic() >= deadline:
                reason = f"not probed: {_OUT_OF_TIME}"
                log.warning("Not probing a task handler artifact", reason=reason, **context)
                probes.answers[coordinator.bundle_name, candidate.rel_path] = reason
                continue
            outcome = _probe_candidate(coordinator, candidate, deadline=deadline, log=log, context=context)
            if isinstance(outcome, TaskHandlerArtifact):
                probes.answers[coordinator.bundle_name, candidate.rel_path] = outcome
            else:
                probes.failures[coordinator.key, coordinator.bundle_name, candidate.rel_path] = outcome
    return probes


def _probe_candidate(
    coordinator: _CoordinatorArtifacts,
    candidate: TaskHandlerCandidate,
    *,
    deadline: float,
    log: FilteringBoundLogger,
    context: dict[str, str],
) -> TaskHandlerArtifact | str:
    """Probe *candidate* under *coordinator*, and return its answer or why it has none."""
    from airflow.dag_processing.task_handler_processor import (
        LangSDKTaskHandlerProcessorProcess,
        TaskHandlerProbeStopped,
    )

    log.info("Probing a task handler artifact", **context)
    started = time.monotonic()
    try:
        result = LangSDKTaskHandlerProcessorProcess.run(
            coordinator=coordinator.key,
            path=coordinator.bundle_path / candidate.rel_path,
            bundle_path=coordinator.bundle_path,
            bundle_name=coordinator.bundle_name,
            artifact_rel_path=candidate.rel_path,
            logger=log,
            deadline=deadline,
        )
    except Exception as e:
        log.warning(
            "Probing a task handler artifact raised",
            seconds=round(time.monotonic() - started, 3),
            exc_info=True,
            **context,
        )
        return f"probe failed: {_describe(e)}"
    seconds = round(time.monotonic() - started, 3)
    for warning in result.warnings or ():
        log.warning("The task handler runtime reported a warning", warning=warning, **context)
    if result.import_errors:
        error = "; ".join(result.import_errors.values())
        log.warning(
            "Probed a task handler artifact without an answer", error=error, seconds=seconds, **context
        )
        if isinstance(result, TaskHandlerProbeStopped):
            return f"probe stopped: {_OUT_OF_TIME}"
        return f"probe failed: {error}"
    try:
        answer = TaskHandlerArtifact(
            bundle_name=coordinator.bundle_name,
            relative_fileloc=candidate.rel_path,
            size_bytes=candidate.size_bytes,
            cache_digest=candidate.cache_digest,
            task_handlers=result.task_handlers,
        )
    except ValidationError as e:
        errors = "; ".join(f"{'.'.join(map(str, error['loc']))}: {error['msg']}" for error in e.errors())
        log.warning("Probed a task handler artifact with an invalid answer", error=errors, **context)
        return f"invalid answer: {errors}"
    log.info("Probed a task handler artifact", seconds=seconds, **context)
    return answer


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


def _make_problems(stub_tasks: Iterable[StubTask], message: str) -> list[TaskHandlerProblem]:
    """Return *message* as a problem of each Dag file with one of *stub_tasks*."""
    return [
        TaskHandlerProblem(relative_fileloc=relative_fileloc, message=message)
        for relative_fileloc in sorted({stub.relative_fileloc for stub in stub_tasks})
    ]


def _describe(error: BaseException) -> str:
    """Return *error* with its type, and the error it was raised from, if any."""
    description = f"{type(error).__name__}: {error}"
    cause = error.__cause__ or (None if error.__suppress_context__ else error.__context__)
    if cause is not None:
        description = f"{description} ({type(cause).__name__}: {cause})"
    return description


def _describe_team(team_name: str | None) -> str:
    return "no team" if team_name is None else f"team {team_name!r}"
