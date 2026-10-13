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
Build a validated :class:`~.models.DagDocument` into a :class:`~airflow.sdk.DAG`.

The parser produces a validated document; this turns it into real DAG + operator
objects. Ready-made operators are instantiated from their ``uses`` import path;
tasks with ``run`` are built into stub operators and handled by AIP-108 task
handlers at execution time.

Templating is explicit, realized importer-side (a la AIP-80). The format is
literal by default; only values the author marked render as templates. Value
grammar markers are only accepted on fields declared in ``template_fields``.

Tasks are built in topological order so a task referenced by an upstream's XCom
already exists when the downstream operator is constructed.
"""

from __future__ import annotations

import datetime
import functools
import graphlib
import inspect
import json
import keyword
import linecache
import re
from typing import TYPE_CHECKING, Any

import attrs

from airflow.sdk import DAG, BaseOperator, XComArg, literal, task as task_decorators
from airflow.sdk._shared.module_loading import import_string
from airflow.sdk.definitions.context import KNOWN_CONTEXT_KEYS
from airflow.sdk.importers.yaml_importer.models import (
    CONST_KEY,
    FILE_KEYS,
    TEMPLATE_KEYS,
    XCOM_KEYS,
    TimetableSchedule,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from airflow.dag_processing.bundles.base import BaseDagBundle  # noqa: SDK002
    from airflow.sdk.importers.base import DagDefinition
    from airflow.sdk.importers.yaml_importer.models import DagDocument, Task, Value, XComRef


class YamlDagBuildError(ValueError):
    """A validated document could not be built into a DAG."""


_BASE_TASK_ARGS = frozenset(BaseOperator.__init__._BaseOperatorMeta__param_names)  # type: ignore[attr-defined]
_UNSUPPORTED_TASK_ARGS = frozenset({"task_id", "dag", "task_group", "default_args"})
_MARKER_KEYS = frozenset({*XCOM_KEYS, *TEMPLATE_KEYS, *FILE_KEYS, CONST_KEY})


@attrs.define(kw_only=True)
class DagBuilder:
    """
    Build a :class:`DagDocument` into a :class:`DAG`.

    This holds the state a single build shares. The state belongs to one build
    and is discarded with the instance.
    """

    _definition: DagDefinition
    _bundle: BaseDagBundle
    _operators: dict[str, BaseOperator] = attrs.field(factory=dict, init=False)
    _stub_callables: dict[tuple[str, ...], Callable[..., None]] = attrs.field(factory=dict, init=False)

    def build(self, doc: DagDocument) -> DAG:
        tasks = {t.id_: t for t in doc.tasks}
        _reject_reserved_extra_keys(doc.model_extra or {})
        extras = _coerce_temporal(doc.model_extra or {}, DAG)
        with DAG(dag_id=doc.dag_id, schedule=_build_schedule(doc.schedule), **extras) as dag:
            for task_id in _topological_order(doc.tasks):
                self._operators[task_id] = self._build_operator(tasks[task_id])

            for task in doc.tasks:
                for upstream in sorted(task.get_dependency_ids()):
                    self._operators[upstream] >> self._operators[task.id_]
        dag.bundle_name = self._bundle.name
        dag.fileloc = repr(self._definition)
        dag.relative_fileloc = self._definition.get_relative_loc(self._bundle.path)
        return dag

    def _build_stub_task(self, task: Task, args: dict[str, Value]) -> BaseOperator:
        """
        Build a stub from a "run:" task.

        The stub is fed a callable whose parameters are the run argument names,
        so serialization can build and later validate (AIP-108) the handler's
        argument spec.

        The "real" task body should be a registered SDK task handler resolved at
        run time; the run mapping binds as that handler's arguments. Only a
        top-level ``$x`` (an upstream return value) crosses the binding; every
        other argument must be a JSON-serializable literal.
        """
        if task.with_:
            raise YamlDagBuildError(
                f"task {task.id_!r}: a run: task takes its inputs under 'run:', not 'with:'"
            )
        run_kwargs: dict[str, Any] = {}
        for name, value in args.items():
            if not name.isidentifier() or keyword.iskeyword(name):
                raise YamlDagBuildError(f"task {task.id_!r}: run argument {name!r} is not a valid identifier")
            if name in KNOWN_CONTEXT_KEYS:
                raise YamlDagBuildError(f"task {task.id_!r}: run argument cannot be named {name!r}")
            if error := value.get_run_binding_error():
                raise YamlDagBuildError(f"task {task.id_!r}: {error}")
            if not isinstance(op_arg := value.as_operator_argument(self, rendered=False), XComArg):
                try:
                    json.dumps(op_arg, allow_nan=False)
                except (TypeError, ValueError):
                    raise YamlDagBuildError(
                        f"task {task.id_!r}: run argument {name!r} must be JSON-serializable, "
                        f"not a {type(op_arg).__name__}"
                    ) from None
            run_kwargs[name] = op_arg
        decorator = task_decorators.stub(
            task_id=task.id_, **_coerce_temporal(task.model_extra or {}, BaseOperator)
        )
        return decorator(self._stub_callable(tuple(run_kwargs)))(**run_kwargs).operator

    def _build_operator(self, task: Task) -> BaseOperator:
        """Instantiate *task* as an operator."""
        if reserved := sorted(
            k for k in (*task.with_, *(task.model_extra or {})) if k in _UNSUPPORTED_TASK_ARGS
        ):
            raise YamlDagBuildError(
                f"task {task.id_!r}: {reserved} cannot be set on a task; the task id comes from 'id', and "
                "dag/task_group/default_args are managed by the importer"
            )
        if misplaced := sorted(k for k in task.with_ if k in _BASE_TASK_ARGS):
            raise YamlDagBuildError(
                f"task {task.id_!r}: {misplaced} are task-level arguments; set them at task level, not under 'with'"
            )
        if unknown := sorted(k for k in (task.model_extra or {}) if k not in _BASE_TASK_ARGS):
            raise YamlDagBuildError(
                f"task {task.id_!r}: {unknown} are not task-level (BaseOperator) arguments; "
                "operator-specific arguments belong under 'with'"
            )
        _reject_reserved_extra_keys(task.model_extra or {})

        if (args := task.run) is not None:
            return self._build_stub_task(task, args)

        op_cls: Any
        if task.uses is None:  # uses: null (or omitted) becomes EmptyOperator.
            from airflow.providers.standard.operators.empty import EmptyOperator as op_cls  # noqa: SDK002
        else:
            op_cls = import_string(task.uses)

        template_fields = set(getattr(op_cls, "template_fields", ()))
        temporal = _temporal_fields(op_cls)
        kwargs: dict[str, Any] = {}
        render_fields: list[str] = []
        off_template: list[str] = []
        for name, value in task.with_.items():
            flags = value.build_marker_flags()
            op_arg = value.as_operator_argument(self, rendered=flags.needs_render)
            if name in temporal and not flags.has_marker:  # a literal date/ISO string for a datetime arg
                op_arg = _to_datetime(op_arg)
            kwargs[name] = op_arg
            if flags.has_marker and name not in template_fields:
                off_template.append(name)
            if flags.needs_render:
                render_fields.append(name)
        if off_template:
            raise YamlDagBuildError(
                f"task {task.id_!r}: value grammar markers are only allowed on templated fields, not {off_template}"
            )
        if task.model_extra:  # Merge in task-level arguments (raw, never templated).
            kwargs.update(_coerce_temporal(task.model_extra, BaseOperator))

        return _restrict_template_fields(op_cls(task_id=task.id_, **kwargs), task.with_, render_fields)

    def build_xcom_arg(self, ref: XComRef) -> XComArg:
        """
        Build an :class:`XComArg` from an :class:`XComRef`.

        This looks up the upstream operator among the already-built ones.
        """
        target = ref.target
        if isinstance(target, str):
            return XComArg(self._operators[target])
        if target.key is None:
            return XComArg(self._operators[target.task])
        return XComArg(self._operators[target.task], key=target.key)

    def read_file(self, path: str) -> str:
        """Read a ``$f`` at build time, relative to the document."""
        try:
            return self._definition.sibling(path).read_bytes().decode("utf-8")
        except (OSError, ValueError) as exc:
            raise YamlDagBuildError(f"cannot read $f {path!r}: {exc}") from exc

    def ensure_literal(self, value: Any, *, rendered: bool) -> Any:
        return literal(value) if rendered else value

    def _stub_callable(self, param_names: tuple[str, ...]) -> Callable[..., None]:
        """
        Build a placeholder callable to feed to ``@task.stub``.

        The stub needs a real callable with a meaningful signature; core
        serialization reads it to build the TaskFlow arg-binding spec a lang-SDK
        handler is validated against (AIP-108), and ``@task.stub`` reads the
        callable's source to check the body is empty.

        The callable declares exactly the run arguments as a fixed, ordered
        parameter list with an empty body, and its source is registered with
        :mod:`linecache` so :func:`inspect.getsource` can read it back.
        """
        if (cached := self._stub_callables.get(param_names)) is not None:
            return cached
        source = f"def _stub({', '.join(param_names)}):\n    ...\n"
        filename = f"<yaml-dag stub ({', '.join(param_names)})>"
        linecache.cache[filename] = (len(source), None, source.splitlines(keepends=True), filename)
        namespace: dict[str, Any] = {}
        exec(compile(source, filename, "exec"), namespace)
        self._stub_callables[param_names] = stub = namespace["_stub"]
        return stub


def _restrict_template_fields(
    op: BaseOperator, with_: dict[str, Any], render_fields: list[str]
) -> BaseOperator:
    """
    Make *op* render literal-by-default, per instance.

    Only fields the author set in ``with:`` without a marker are dropped from
    ``template_fields``; fields the operator templates by default or marked as
    templates (e.g. uses ``$t``) are kept, so an operator's own defaults (e.g.
    ``ExternalTaskMarker.logical_date``) still render.

    ``template_ext`` is emptied: a file is read only when asked for explicitly
    (``$f``), never by a rendered value's suffix.
    """
    op.template_fields = tuple(f for f in op.template_fields if f not in with_ or f in render_fields)
    op.template_ext = ()
    return op


def _reject_reserved_extra_keys(extras: Any) -> None:
    """
    Reject a marker key ($x/$t/$f/$const) nested in Dag- or task-level arguments.

    These arguments pass straight to ``DAG`` or the operator, never through the
    value grammar, so a marker written there (e.g. ``params: {p: {$f: x}}``)
    would be taken as a literal dict rather than flagged. Only marker keys are
    rejected, so a genuine ``$`` key such as a JSON Schema ``$ref`` in a param
    default is kept.
    """
    if isinstance(extras, dict):
        for key, val in extras.items():
            if key in _MARKER_KEYS:
                raise YamlDagBuildError(f"marker key {key!r} may only be used in 'with:' or 'run:'")
            _reject_reserved_extra_keys(val)
    elif isinstance(extras, list):
        for item in extras:
            _reject_reserved_extra_keys(item)


def _coerce_temporal(mapping: dict[str, Any], target: type) -> dict[str, Any]:
    """
    Promote YAML-native dates and ISO date/datetime strings to datetimes.

    Some fields on a Dag or operator are datetimes, but YAML has distinct date
    and date-time formats, so a bare ``2024-01-01`` or a quoted ISO string would
    otherwise reach the datetime field unconverted. A date becomes midnight; an
    ISO string is parsed (which also helps JSON compatibility).

    Only values bound to a *target* argument annotated as ``datetime`` are
    coerced. A non-temporal argument is left untouched, so a string that merely
    looks like a date keeps its type.
    """
    temporal = _temporal_fields(target)
    return {key: _to_datetime(value) if key in temporal else value for key, value in mapping.items()}


def _to_datetime(value: Any) -> Any:
    if isinstance(value, datetime.datetime):
        return value
    if isinstance(value, datetime.date):
        return datetime.datetime.combine(value, datetime.time.min)
    if isinstance(value, str):
        try:
            return datetime.datetime.fromisoformat(value)
        except ValueError:
            return value
    return value


# Airflow annotates arguments with either stdlib datetime or pendulum DateTime.
# Matched by name (see _temporal_fields) rather than resolved to avoid having
# to evaluate dynamically.
_TEMPORAL_TYPE_NAMES = frozenset({"datetime", "DateTime"})
_TYPE_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_.]*")


@functools.cache
def _temporal_fields(target: type) -> frozenset[str]:
    """
    Names of *target*'s constructor arguments annotated as a datetime type.

    The annotation text is matched against the datetime type names Airflow uses;
    it is never resolved, so one referring to a ``TYPE_CHECKING``-only name does
    not need importing. The constructor signature is read -- not class
    attributes -- so attrs field aliases and operator-specific arguments are
    covered. This is not perfect (a datetime type reached under another alias is
    missed); we'll fix edge cases as we go.
    """
    try:
        params = inspect.signature(target).parameters
    except (TypeError, ValueError):
        return frozenset()
    return frozenset(name for name, p in params.items() if _is_temporal_annotation(str(p.annotation)))


def _is_temporal_annotation(annotation: str) -> bool:
    """Return True if *annotation* names a datetime type, ignoring any module prefix."""
    return any(ref.rsplit(".", 1)[-1] in _TEMPORAL_TYPE_NAMES for ref in _TYPE_NAME_RE.findall(annotation))


def _build_schedule(schedule: Any) -> Any:
    """Build *schedule* into a schedule construct for :class:`DAG`."""
    if isinstance(schedule, TimetableSchedule):
        return import_string(schedule.uses)(**schedule.with_)
    return schedule


def _topological_order(tasks: list[Task]) -> list[str]:
    """Return task ids ordered so every task follows the tasks it depends on."""
    graph = {task.id_: task.get_dependency_ids() for task in tasks}
    try:
        return list(graphlib.TopologicalSorter(graph).static_order())
    except graphlib.CycleError as exc:
        raise YamlDagBuildError(f"dependency cycle among tasks: {exc.args[1]}") from exc
