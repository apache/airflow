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
import graphlib
import keyword
import linecache
from typing import TYPE_CHECKING, Any, cast

import attrs

from airflow.sdk import DAG, BaseOperator, XComArg, literal, task as task_decorators
from airflow.sdk._shared.module_loading import import_string
from airflow.sdk.importers.yaml_importer.models import (
    ConstRef,
    FileRef,
    Literal,
    TemplateRef,
    TimetableSchedule,
    XComRef,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from airflow.dag_processing.bundles.base import BaseDagBundle  # noqa: SDK002
    from airflow.sdk.importers.base import DagDefinition
    from airflow.sdk.importers.yaml_importer.models import DagDocument, Task, Value


class YamlDagBuildError(ValueError):
    """A validated document could not be built into a DAG."""


_BASE_TASK_ARGS = frozenset(BaseOperator.__init__._BaseOperatorMeta__param_names)  # type: ignore[attr-defined]


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
        extras = _coerce_temporal(doc.model_extra or {})
        with DAG(dag_id=doc.dag_id, schedule=_build_schedule(doc.schedule), **extras) as dag:
            # A task depended by another task must be built first.
            for task_id in _topological_order(doc.tasks):
                self._operators[task_id] = self._build_operator(tasks[task_id])

            # Wire dependency edges explicitly.
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

        The task body is a registered SDK task handler resolved at run time; the
        run mapping binds as that handler's arguments, where XCom references
        resolve natively. The stub is fed a callable whose parameters are the
        run argument names, so serialization can build and later validate
        (AIP-108) the handler's argument spec.
        """
        run_kwargs: dict[str, Any] = {}
        for name, value in args.items():
            if not name.isidentifier() or keyword.iskeyword(name):
                raise YamlDagBuildError(f"task {task.id_!r}: run argument {name!r} is not a valid identifier")
            _reject_unbindable_run_marker(task.id_, value)
            run_kwargs[name] = self._lower(value, protect=False)
        decorator = task_decorators.stub(task_id=task.id_, **_coerce_temporal(task.model_extra or {}))
        return decorator(self._stub_callable(tuple(run_kwargs)))(**run_kwargs).operator

    def _build_operator(self, task: Task) -> BaseOperator:
        """Instantiate *task* as an operator."""
        if misplaced := sorted(k for k in task.with_ if k in _BASE_TASK_ARGS):
            raise YamlDagBuildError(
                f"task {task.id_!r}: {misplaced} are task-level arguments; set them at task level, not under 'with'"
            )
        if unknown := sorted(k for k in (task.model_extra or {}) if k not in _BASE_TASK_ARGS):
            raise YamlDagBuildError(
                f"task {task.id_!r}: {unknown} are not task-level (BaseOperator) arguments; "
                "operator-specific arguments belong under 'with'"
            )

        if (args := task.run) is not None:
            return self._build_stub_task(task, args)

        op_cls: Any
        if task.uses is None:  # uses: null (or omitted) becomes EmptyOperator.
            from airflow.providers.standard.operators.empty import EmptyOperator as op_cls  # noqa: SDK002
        else:
            op_cls = import_string(task.uses)

        # Validate template fields.
        template_fields = set(getattr(op_cls, "template_fields", ()))
        kwargs: dict[str, Any] = {}
        render_fields: list[str] = []
        off_template: list[str] = []
        for name, value in task.with_.items():
            needs_render, has_marker = _field_markers(value)
            kwargs[name] = self._lower(value, protect=needs_render)
            if has_marker and name not in template_fields:
                off_template.append(name)
            if needs_render:
                render_fields.append(name)
        if off_template:
            raise YamlDagBuildError(f"task {task.id_!r}: value grammar only allowed on template_fields")
        if task.model_extra:  # Merge in task-level arguments (raw, never templated).
            kwargs.update(_coerce_temporal(task.model_extra))

        return _restrict_template_fields(op_cls(task_id=task.id_, **kwargs), task.with_, render_fields)

    def _lower(self, value: Any, *, protect: bool) -> Any:
        """
        Lower a resolved value-grammar node into an operator argument.

        :param protect: This should be set when the enclosing field renders to
            avoid double-rendering. :class:`LiteralValue` is used to wrap
            rendered values so they stay literal.
        """
        if isinstance(value, XComRef):
            return self._xcom_arg(value)  # resolves at render time; never wrapped
        if isinstance(value, TemplateRef):
            # $t over a string renders that string; $t over $f is a template file (read, then render).
            if isinstance(value.source, FileRef):
                # A constructed FileRef has a str path (its validator rejects a templated filename).
                return self._read_file(cast("str", value.source.path))
            return value.source
        if isinstance(value, FileRef):
            return self._protect(self._read_file(cast("str", value.path)), protect)
        if isinstance(value, ConstRef):
            return self._protect(value.value, protect)
        if isinstance(value, Literal):
            return self._lower(value.root, protect=protect)
        if isinstance(value, dict):
            return {k: self._lower(v, protect=protect) for k, v in value.items()}
        if isinstance(value, list):
            return [self._lower(v, protect=protect) for v in value]
        return self._protect(value, protect)

    @staticmethod
    def _protect(value: Any, protect: bool) -> Any:
        return literal(value) if protect else value

    def _xcom_arg(self, value: XComRef) -> XComArg:
        """Resolve an :class:`XComRef` against the already-built operators."""
        target = value.target
        if isinstance(target, str):
            return XComArg(self._operators[target])
        if target.key is None:
            return XComArg(self._operators[target.task])
        return XComArg(self._operators[target.task], key=target.key)

    def _read_file(self, rel_path: str) -> str:
        """Read a ``$f`` at build time, relative to the document."""
        try:
            return self._definition.sibling(rel_path).read_bytes().decode("utf-8")
        except (OSError, ValueError) as exc:
            raise YamlDagBuildError(f"cannot read $f {rel_path!r}: {exc}") from exc

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


def _reject_unbindable_run_marker(task_id: str, value: Any) -> None:
    """
    Reject value-grammar a task handler's argument binding cannot carry.

    The binding carries only literals and an upstream task's return value, so a
    template (``$t``) or a keyed XCom (``$x`` with a ``key``) anywhere in a run
    argument is an error. A plain ``$x`` (return value), ``$f``, ``$const`` and
    literals are fine.
    """
    if isinstance(value, TemplateRef):
        raise YamlDagBuildError(
            f"task {task_id!r}: a template ($t) reference is not supported in a run: task; the task "
            "handler binding carries literal values and XCom return values, not rendered templates"
        )
    if isinstance(value, XComRef):
        target = value.target
        if not isinstance(target, str) and target.key is not None:
            raise YamlDagBuildError(
                f"task {task_id!r}: a keyed XCom ($x with 'key') is not supported in a run: task; "
                "only a task's return value crosses the handler boundary"
            )
        return
    if isinstance(value, Literal):
        _reject_unbindable_run_marker(task_id, value.root)
    elif isinstance(value, dict):
        for item in value.values():
            _reject_unbindable_run_marker(task_id, item)
    elif isinstance(value, list):
        for item in value:
            _reject_unbindable_run_marker(task_id, item)


def _coerce_temporal(mapping: dict[str, Any]) -> dict[str, Any]:
    """
    Promote YAML-native dates and ISO date/datetime strings to datetimes.

    Dag/operator arguments such as ``start_date`` are datetimes, but YAML has
    distinct date and date-time formats. Convert dates to midnight times.

    Strings are also parsed automatically to datetimes if they LOOK like the ISO
    8601 format. This helps with JSON compatibility.
    """
    return {key: _to_datetime(value) for key, value in mapping.items()}


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


def _build_schedule(schedule: Any) -> Any:
    """Build *schedule* into a schedule construct for :class:`DAG`."""
    if isinstance(schedule, TimetableSchedule):
        return import_string(schedule.uses)(**schedule.with_)
    return schedule


def _field_markers(value: Any) -> tuple[bool, bool]:
    """
    Scan a value-grammar node for ``(needs_render, has_marker)``.

    ``needs_render`` is True when it carries a ``$t`` or ``$x`` anywhere (the field must render);
    ``has_marker`` is True when it carries any of ``$x``/``$t``/``$f`` (only allowed on a templated
    field). ``$const`` is a force-literal escape, not a marker.
    """
    if isinstance(value, XComRef):
        return True, True
    if isinstance(value, TemplateRef):
        return True, True
    if isinstance(value, FileRef):
        return False, True
    if isinstance(value, ConstRef):
        return False, False
    if isinstance(value, Literal):
        return _field_markers(value.root)
    if isinstance(value, dict):
        flags = [_field_markers(v) for v in value.values()]
        return any(r for r, _ in flags), any(m for _, m in flags)
    if isinstance(value, list):
        flags = [_field_markers(v) for v in value]
        return any(r for r, _ in flags), any(m for _, m in flags)
    return False, False


def _topological_order(tasks: list[Task]) -> list[str]:
    """Return task ids ordered so every task follows the tasks it depends on."""
    graph = {task.id_: task.get_dependency_ids() for task in tasks}
    try:
        return list(graphlib.TopologicalSorter(graph).static_order())
    except graphlib.CycleError as exc:
        raise YamlDagBuildError(f"dependency cycle among tasks: {exc.args[1]}") from exc
