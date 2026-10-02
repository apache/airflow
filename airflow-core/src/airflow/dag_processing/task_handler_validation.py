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
"""Check a Dag file's stub tasks against the task handlers that their coordinator's artifacts register."""

from __future__ import annotations

import json
from collections import defaultdict
from typing import TYPE_CHECKING

import attrs

from airflow.api_fastapi.execution_api.datamodels.task_arg_binding import (
    LiteralArgBinding,
    get_arg_bindings_adapter,
)
from airflow.dag_processing.processor import TaskHandlerBinding
from airflow.sdk.definitions._internal.abstractoperator import DEFAULT_QUEUE  # noqa: SDK001
from airflow.serialization.enums import Encoding

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

    from airflow.api_fastapi.execution_api.datamodels.task_arg_binding import ArgValueSchema, TaskArgBinding
    from airflow.dag_processing.processor import TaskHandlerArtifact, TaskHandlerDeclaration, TaskHandlerParam
    from airflow.sdk.definitions.dag import DAG  # noqa: SDK001
    from airflow.serialization.serialized_objects import LazyDeserializedDAG

# Every top-level JSON type, in the order messages list them.
_JSON_TYPES = ("string", "integer", "number", "boolean", "array", "object", "null")


@attrs.frozen(kw_only=True)
class StubTask:
    """A ``@task.stub`` task of a Dag that serialized, as the worker that runs it will see it."""

    dag_id: str
    task_id: str
    queue: str
    relative_fileloc: str
    """The Dag's file, which keys its import error."""

    arg_bindings: list[TaskArgBinding]
    """The arguments of the call; ``[]`` for an argless call."""

    is_mapped: bool
    """A mapped stub task has no arguments captured at parse time, so only its handler's presence is checked."""


@attrs.frozen(kw_only=True)
class ArgumentCheck:
    """How a stub task's arguments bind to one task handler's parameters."""

    errors: list[str]
    """Each makes the Dag file fail to import."""

    passed_not_declared: list[str]
    """Under ``named`` binding, passed arguments no parameter takes; logged, never an error."""

    declared_not_passed: list[str]
    """Under ``named`` binding, parameters no argument fills; logged, never an error."""


@attrs.frozen(kw_only=True)
class TaskHandlerProblem:
    """One line of a Dag file's import error."""

    relative_fileloc: str
    message: str
    dag_id: str | None = None
    """``None`` for a problem of no single stub task, which is listed first."""

    task_id: str | None = None


@attrs.frozen(kw_only=True)
class StubTaskWarning:
    """A name mismatch between a stub task and its ``named`` task handler, for the parse log."""

    dag_id: str
    task_id: str
    artifact_bundle_name: str
    artifact_rel_path: str
    passed_not_declared: list[str]
    declared_not_passed: list[str]


@attrs.frozen(kw_only=True)
class TaskHandlerMatch:
    """The outcome of checking stub tasks against one coordinator's answers."""

    bindings: list[TaskHandlerBinding]
    """One per stub task with exactly one handler, even when its arguments do not match."""

    problems: list[TaskHandlerProblem]
    warnings: list[StubTaskWarning]


def collect_stub_tasks(dags: Iterable[DAG], serialized_dags: Iterable[LazyDeserializedDAG]) -> list[StubTask]:
    """
    Return the stub tasks of every Dag in *serialized_dags*.

    The arguments are read from the serialized Dag as JSON, so they are the ones the worker gets: a tuple
    becomes a list, and a mapping key becomes a string. A stub task with no queue of its own runs on the
    default queue.
    """
    dags_by_id = {dag.dag_id: dag for dag in dags}
    stub_tasks: list[StubTask] = []
    for serialized_dag in serialized_dags:
        dag = dags_by_id[serialized_dag.dag_id]
        encoded_tasks = {
            task[Encoding.VAR]["task_id"]: task[Encoding.VAR] for task in serialized_dag.data["dag"]["tasks"]
        }
        stub_tasks.extend(
            StubTask(
                dag_id=dag.dag_id,
                task_id=task.task_id,
                queue=DEFAULT_QUEUE if task.queue is None else task.queue,
                relative_fileloc=dag.relative_fileloc or dag.fileloc,
                arg_bindings=get_arg_bindings_adapter().validate_json(
                    json.dumps(encoded_tasks[task.task_id].get("_arg_bindings") or [])
                ),
                is_mapped=task.is_mapped,
            )
            for task in dag.task_dict.values()
            if task.is_stub
        )
    return stub_tasks


def check_task_handler_arguments(
    arg_bindings: Sequence[TaskArgBinding], declaration: TaskHandlerDeclaration
) -> ArgumentCheck:
    """
    Check how *arg_bindings* bind to the parameters of *declaration*, as its runtime binds them.

    A defaulted argument, one filled from the stub signature's default, is type-checked when it binds and is
    never reported as unmatched.

    - ``positional``: the count matches when all arguments, or those left after dropping the defaulted
      ones, number the parameters; any other count is an error. Each argument must have a value type the
      parameter at its position accepts.
    - ``named``: a parameter takes the argument of its exact name, else, unless ``exact_name`` is set, the one
      whose name folds to its own (lower case, underscores removed); a folded name two arguments share
      matches neither. Each bound argument must have a value type its parameter accepts. A passed argument
      no parameter takes and a parameter no argument fills are reported for the parse log. They are not
      when the argument may be the whole value: no parameter matched, exactly one argument was passed, the
      handler has parameters and none of them has ``exact_name`` set, and the argument may be an object.
    - ``params`` is ``None``: nothing is checked.
    """
    params = declaration.params
    if params is None:
        return ArgumentCheck(errors=[], passed_not_declared=[], declared_not_passed=[])
    passed = [arg for arg in arg_bindings if not _is_defaulted(arg)]
    if declaration.binding == "positional":
        return _check_positional_arguments(arg_bindings, passed, params)
    return _check_named_arguments(arg_bindings, passed, params)


def check_value_schema(
    stub_schema: ArgValueSchema | None, handler_schema: ArgValueSchema | None
) -> str | None:
    """
    Return why a value the stub's schema allows may be one the handler's schema rejects, or ``None``.

    Only the top-level JSON types are compared, as the Go runtime compares them. A stub type of ``null``
    needs a handler type of ``null``, and at least one of the stub's other types must be a handler type,
    with ``integer`` accepted by ``number``. The types come from ``type``, the union over ``anyOf`` or
    ``oneOf``, or the values of ``const`` or ``enum``. A side with no schema, or a schema of any other shape
    such as ``$ref`` or ``allOf``, is not compared. ``format``, ranges and nested items are never compared.
    """
    if stub_schema is None or handler_schema is None:
        return None
    stub_types = _get_json_types(stub_schema)
    handler_types = _get_json_types(handler_schema)
    if stub_types is None or handler_types is None:
        return None
    accepted = (handler_types | {"integer"}) if "number" in handler_types else handler_types
    non_null_types = stub_types - {"null"}
    if ("null" not in stub_types or "null" in handler_types) and (
        not non_null_types or non_null_types & accepted
    ):
        return None
    return f"{_join_types(stub_types)}, the task handler takes {_join_types(handler_types)}"


def match_task_handlers(
    stub_tasks: Sequence[StubTask],
    answers: Sequence[TaskHandlerArtifact],
    *,
    bundle_name: str,
    broken_candidates: Mapping[str, str],
) -> TaskHandlerMatch:
    """
    Find the one task handler of each stub task among the *answers* of the coordinator it routes to.

    A stub task that no artifact registers is a problem naming each of *broken_candidates* (artifact path to
    why it has no answer), and so is one that two or more artifacts register. A handler with no stub task is
    not a problem. A stub task with exactly one handler is bound to its artifact, and its arguments are
    checked unless it is mapped or the handler's ``params`` are ``None``.
    """
    claims: defaultdict[tuple[str, str], list[tuple[TaskHandlerArtifact, TaskHandlerDeclaration]]] = (
        defaultdict(list)
    )
    for artifact in answers:
        for dag_id, declarations in artifact.task_handlers.items():
            for declaration in declarations:
                claims[dag_id, declaration.task_id].append((artifact, declaration))

    bindings: list[TaskHandlerBinding] = []
    problems: list[TaskHandlerProblem] = []
    warnings: list[StubTaskWarning] = []
    for stub in stub_tasks:
        stub_claims = claims[stub.dag_id, stub.task_id]
        prefix = f"Dag {stub.dag_id!r}, task {stub.task_id!r}"
        if len(stub_claims) != 1:
            if stub_claims:
                paths = _join_quoted(sorted(artifact.relative_fileloc for artifact, _ in stub_claims))
                message = f"{prefix}: registered by {paths} in Dag bundle {bundle_name!r}"
            else:
                message = f"{prefix}: no artifact in Dag bundle {bundle_name!r} registers it"
                if broken_candidates:
                    unanswered = ", ".join(
                        f"{path!r} ({why})" for path, why in sorted(broken_candidates.items())
                    )
                    message = f"{message}; no answer from {unanswered}"
            problems.append(_make_problem(stub, message))
            continue

        [(artifact, declaration)] = stub_claims
        bindings.append(
            TaskHandlerBinding(
                dag_id=stub.dag_id,
                task_id=stub.task_id,
                artifact_bundle_name=artifact.bundle_name,
                artifact_rel_path=artifact.relative_fileloc,
            )
        )
        if stub.is_mapped:
            continue
        check = check_task_handler_arguments(stub.arg_bindings, declaration)
        located = f"{prefix} ({artifact.relative_fileloc!r} in Dag bundle {bundle_name!r})"
        problems.extend(_make_problem(stub, f"{located}: {error}") for error in check.errors)
        if check.passed_not_declared or check.declared_not_passed:
            warnings.append(
                StubTaskWarning(
                    dag_id=stub.dag_id,
                    task_id=stub.task_id,
                    artifact_bundle_name=artifact.bundle_name,
                    artifact_rel_path=artifact.relative_fileloc,
                    passed_not_declared=check.passed_not_declared,
                    declared_not_passed=check.declared_not_passed,
                )
            )
    return TaskHandlerMatch(bindings=bindings, problems=problems, warnings=warnings)


def format_import_errors(problems: Iterable[TaskHandlerProblem]) -> dict[str, str]:
    """
    Build one import error per Dag file from its *problems*.

    Problems of no single stub task come first, in the order given, then the others by Dag and task id.
    """
    by_file: defaultdict[str, list[TaskHandlerProblem]] = defaultdict(list)
    for problem in problems:
        by_file[problem.relative_fileloc].append(problem)
    import_errors: dict[str, str] = {}
    for relative_fileloc in sorted(by_file):
        ordered = sorted(
            by_file[relative_fileloc],
            key=lambda problem: (problem.dag_id is not None, problem.dag_id or "", problem.task_id or ""),
        )
        lines = [f"Stub tasks in {relative_fileloc} do not match their task handlers:"]
        lines.extend(f"- {problem.message}" for problem in ordered)
        import_errors[relative_fileloc] = "\n".join(lines)
    return import_errors


def _check_positional_arguments(
    arg_bindings: Sequence[TaskArgBinding],
    passed: Sequence[TaskArgBinding],
    params: Sequence[TaskHandlerParam],
) -> ArgumentCheck:
    bound = arg_bindings if len(arg_bindings) == len(params) else passed
    if len(bound) == len(params):
        errors = _check_values(zip(bound, params))
    else:
        count = f"{len(arg_bindings)} argument{'' if len(arg_bindings) == 1 else 's'}"
        if len(passed) != len(arg_bindings):
            count = f"{count} ({len(passed)} without defaults)"
        errors = [f"passes {count}, the task handler takes {len(params)}"]
    return ArgumentCheck(errors=errors, passed_not_declared=[], declared_not_passed=[])


def _check_named_arguments(
    arg_bindings: Sequence[TaskArgBinding],
    passed: Sequence[TaskArgBinding],
    params: Sequence[TaskHandlerParam],
) -> ArgumentCheck:
    by_name = {binding.name: binding for binding in arg_bindings}
    by_folded_name: defaultdict[str, list[TaskArgBinding]] = defaultdict(list)
    for binding in arg_bindings:
        by_folded_name[_fold_name(binding.name)].append(binding)
    matched: list[tuple[TaskArgBinding, TaskHandlerParam]] = []
    declared_not_passed: list[str] = []
    for index, param in enumerate(params):
        if param.name is None:
            declared_not_passed.append(f"#{index}")
            continue
        arg: TaskArgBinding | None = by_name.get(param.name)
        if arg is None and not param.exact_name:
            candidates = by_folded_name[_fold_name(param.name)]
            arg = candidates[0] if len(candidates) == 1 else None
        if arg is None:
            declared_not_passed.append(param.name)
        else:
            matched.append((arg, param))
    if not matched and _may_be_whole_value(passed, params):
        return ArgumentCheck(errors=[], passed_not_declared=[], declared_not_passed=[])
    claimed = {arg.name for arg, _ in matched}
    return ArgumentCheck(
        errors=_check_values(matched),
        passed_not_declared=[arg.name for arg in passed if arg.name not in claimed],
        declared_not_passed=declared_not_passed,
    )


def _may_be_whole_value(passed: Sequence[TaskArgBinding], params: Sequence[TaskHandlerParam]) -> bool:
    if len(passed) != 1 or not params or any(param.exact_name for param in params):
        return False
    schema = passed[0].value_schema
    types = None if schema is None else _get_json_types(schema)
    return types is None or "object" in types


def _is_defaulted(arg: TaskArgBinding) -> bool:
    return isinstance(arg, LiteralArgBinding) and arg.from_default


def _fold_name(name: str) -> str:
    return name.replace("_", "").lower()


def _check_values(pairs: Iterable[tuple[TaskArgBinding, TaskHandlerParam]]) -> list[str]:
    return [
        f"argument {arg.name!r} is {mismatch}"
        for arg, param in pairs
        if (mismatch := check_value_schema(arg.value_schema, param.value_schema)) is not None
    ]


def _get_json_types(schema: Mapping[str, object]) -> frozenset[str] | None:
    if "type" in schema:
        names = schema["type"]
        if isinstance(names, str):
            names = [names]
        if not isinstance(names, list) or not names or not all(name in _JSON_TYPES for name in names):
            return None
        return frozenset(names)
    for keyword in ("anyOf", "oneOf"):
        if keyword in schema:
            branches = schema[keyword]
            if not isinstance(branches, list) or not branches:
                return None
            union: set[str] = set()
            for branch in branches:
                if not isinstance(branch, dict) or (types := _get_json_types(branch)) is None:
                    return None
                union |= types
            return frozenset(union)
    if "const" in schema:
        return frozenset([_get_value_type(schema["const"])])
    if isinstance(values := schema.get("enum"), list) and values:
        return frozenset(_get_value_type(value) for value in values)
    return None


def _get_value_type(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    return "object"


def _join_types(types: frozenset[str]) -> str:
    return _join([name for name in _JSON_TYPES if name in types], "or")


def _join_quoted(values: Sequence[str]) -> str:
    return _join([repr(value) for value in values], "and")


def _join(values: Sequence[str], conjunction: str) -> str:
    if len(values) == 1:
        return values[0]
    return f"{', '.join(values[:-1])} {conjunction} {values[-1]}"


def _make_problem(stub: StubTask, message: str) -> TaskHandlerProblem:
    return TaskHandlerProblem(
        relative_fileloc=stub.relative_fileloc, message=message, dag_id=stub.dag_id, task_id=stub.task_id
    )
