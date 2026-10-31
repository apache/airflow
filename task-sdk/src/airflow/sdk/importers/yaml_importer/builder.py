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
from typing import TYPE_CHECKING, Any, NamedTuple, cast

import attrs
import pendulum
import structlog
from dateutil.relativedelta import relativedelta

from airflow.sdk import DAG, BaseOperator, XComArg, literal, task as task_decorators
from airflow.sdk._shared.module_loading import import_string
from airflow.sdk.definitions.context import KNOWN_CONTEXT_KEYS
from airflow.sdk.importers.yaml_importer.models import (
    CONST_KEY,
    FILE_KEYS,
    TEMPLATE_KEYS,
    XCOM_KEYS,
    ConstRef,
    FileRef,
    Literal,
    TemplateRef,
    TimetableSchedule,
    XComRef,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from airflow.dag_processing.bundles.base import BaseDagBundle  # noqa: SDK002
    from airflow.sdk.importers.base import DagDefinition
    from airflow.sdk.importers.yaml_importer.models import DagDocument, Task, Value


class YamlDagBuildError(ValueError):
    """A validated document could not be built into a DAG."""


_BASE_TASK_ARGS = frozenset(BaseOperator.__init__._BaseOperatorMeta__param_names)  # type: ignore[attr-defined]
_UNSUPPORTED_TASK_ARGS = frozenset({"task_id", "dag", "task_group", "default_args"})
_MARKER_KEYS = frozenset({*XCOM_KEYS, *TEMPLATE_KEYS, *FILE_KEYS, CONST_KEY})

log = structlog.get_logger(__name__)


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
        self._validate_stub_handlers(doc)
        dag.bundle_name = self._bundle.name
        dag.fileloc = repr(self._definition)
        dag.relative_fileloc = self._definition.get_relative_loc(self._bundle.path)
        return dag

    def _validate_stub_handlers(self, doc: DagDocument) -> None:
        """
        Check each ``run:`` task's arguments against its handler.

        This implements the interface defined in AIP-108 and Language SDK
        ADR-0011. The routed coordinator is queried to obtain the handler's
        signature and compared against the ``run:`` arguments. Mismatches are
        collected and raised together as a :class:`YamlDagBuildError`,
        eventually recorded as an import error.

        Validation is skipped when no declaration is available (no handler
        bundle or an unregistered handler), or resolving one fails (an
        uninitializable bundle, an ImportError in it, or a malformed handler).

        This never fails parsing, so building a Dag does not depend on handler
        availability. A real problem surfaces when the task runs.
        """
        if not (stub_tasks := [task for task in doc.tasks if self._operators[task.id_].is_stub]):
            return
        if problems := "; ".join(self._iter_stub_handler_collect_problems(doc, stub_tasks)):
            raise YamlDagBuildError(problems)

    def _iter_stub_handler_collect_problems(self, doc: DagDocument, stub_tasks: list[Task]) -> Iterator[str]:
        from airflow.sdk.execution_time.coordinator import get_coordinator_manager

        manager = get_coordinator_manager()
        for task in stub_tasks:
            coordinator = manager.for_queue(self._operators[task.id_].queue)
            try:
                declaration = coordinator.parse_task_handler(
                    dag_id=doc.dag_id,
                    task_id=task.id_,
                    bundle=self._bundle,
                )
            except Exception:
                log.warning(
                    "Could not resolve task handler; skipping parse-time validation",
                    dag_id=doc.dag_id,
                    task_id=task.id_,
                    exc_info=True,
                )
                continue
            if declaration is None:
                continue  # Best-effort: nothing to match against; defer to execution.
            run_names = set(task.run) if task.run else set()
            if missing := sorted({p.name for p in declaration.params if p.required} - run_names):
                yield f"task {task.id_!r}: missing required handler argument(s) {missing}"
            if unknown := sorted(run_names - {p.name for p in declaration.params}):
                yield f"task {task.id_!r}: run argument(s) {unknown} are not parameters of its task handler"
            # Only names and arity are checked here. Type validation will follow
            # once Language SDK ADR-0011 is settled.

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
            _reject_unbindable_run_marker(task.id_, value)
            if not isinstance(lowered := self._lower(value, protect=False), XComArg):
                try:
                    json.dumps(lowered, allow_nan=False)
                except (TypeError, ValueError):
                    raise YamlDagBuildError(
                        f"task {task.id_!r}: run argument {name!r} must be JSON-serializable, "
                        f"not a {type(lowered).__name__}"
                    ) from None
            run_kwargs[name] = lowered
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
        coercer = _FieldCoercer.for_target(op_cls)
        kwargs: dict[str, Any] = {}
        render_fields: list[str] = []
        off_template: list[str] = []
        for name, value in task.with_.items():
            needs_render, has_marker = _field_markers(value)
            lowered = self._lower(value, protect=needs_render)
            if not has_marker:
                lowered = coercer.coerce(name, lowered)
            kwargs[name] = lowered
            if has_marker and name not in template_fields:
                off_template.append(name)
            if needs_render:
                render_fields.append(name)
        if off_template:
            raise YamlDagBuildError(
                f"task {task.id_!r}: value grammar markers are only allowed "
                f"on templated fields, not {off_template}"
            )
        if task.model_extra:  # Merge in task-level arguments (never templated).
            kwargs.update(_coerce_temporal(task.model_extra, BaseOperator))

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


def _reject_unbindable_run_marker(task_id: str, value: Any, *, top_level: bool = True) -> None:
    """
    Reject value-grammar a task handler's argument binding cannot carry.

    The binding carries a JSON literal or a single top-level ``$x`` (an upstream
    task's return value). So a template (``$t``) or a keyed ``$x`` anywhere, or a
    plain ``$x`` nested inside a container instead of passed as its own argument,
    is an error. ``$f`` and ``$const`` lower to literals and are fine.
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
        if not top_level:
            raise YamlDagBuildError(
                f"task {task_id!r}: an XCom ($x) nested inside a run argument is not supported; pass "
                "the upstream output as its own top-level run argument"
            )
        return
    if isinstance(value, Literal):
        _reject_unbindable_run_marker(task_id, value.root, top_level=top_level)
    elif isinstance(value, dict):
        for item in value.values():
            _reject_unbindable_run_marker(task_id, item, top_level=False)
    elif isinstance(value, list):
        for item in value:
            _reject_unbindable_run_marker(task_id, item, top_level=False)


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
    Coerce scalars to datetime or time delta types expected by *target*.

    Coercion follows how each argument is typed in the argument list. A value
    bound to ``datetime`` is promoted to a datetime. A YAML date becomes
    midnight. An ISO string is parsed

    A value bound to ``date`` is parsed from an ISO string; a YAML date or
    datetime is passed through, so a datetime keeps its time rather than being
    narrowed.

    A value bound to ``timedelta``, ``relativedelta``, or ``pendulum.Duration``
    is parsed from a duration string.

    The concrete type follows the annotation. A ``pendulum`` annotation
    (``DateTime``, ``Date``, or ``Duration``) yields a Pendulum value, and a
    standard library or ``dateutil`` annotation yields a value of that type. A
    datetime passed to a ``date`` argument is the one exception, keeping its
    stdlib type.

    An argument of none of these types is left untouched.
    """
    coercer = _FieldCoercer.for_target(target)
    return {key: coercer.coerce(key, value) for key, value in mapping.items()}


class _FieldCoercer(dict):
    """
    Maps an argument name to the coercion for its declared type.

    This is built from a Dag or operator's constructor signature. An argument
    with no known coercion is simply absent, and ``__missing__`` supplies an
    identity no-op coercion.

    A coercion is chosen by an argument's type annotation (by text matching, not
    resolved, to reduce calculation cost). This is not theoratically perfect,
    but should be plenty good enough in practice.
    """

    _TYPE_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_.]*")

    # Datetime arguments, stdlib or pendulum.
    _DATETIME_TYPE_NAMES = frozenset({"datetime", "DateTime"})
    # Date arguments, stdlib or pendulum.
    _DATE_TYPE_NAMES = frozenset({"date", "Date"})
    # Time-delta arguments, stdlib timedelta or dateutil relativedelta.
    _DELTA_TYPE_NAMES = frozenset({"timedelta", "relativedelta", "Duration"})

    # ISO 8601 duration format.
    _ISO_DURATION_RE = re.compile(
        r"^P(?=[\dYMWDT])(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)W)?(?:(\d+)D)?"
        r"(?:T(?=[\dHMS])(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$"
    )

    # Short-hand duration format (e.g. 5m, 1h).
    _SHORT_DURATION_RE = re.compile(r"^(?=\d)(?:(\d+)w)?(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$")

    @classmethod
    @functools.cache
    def for_target(cls, target: type) -> _FieldCoercer:
        """Build the coercer for *target*'s constructor arguments."""
        coercer = cls()
        try:
            params = inspect.signature(target).parameters
        except (TypeError, ValueError):
            return coercer
        for name, param in params.items():
            annotation = str(param.annotation)
            if allowed := cls._match_datetime_annotation(annotation):
                coercer[name] = functools.partial(cls._to_datetime, allowed=allowed)
            elif allowed := cls._match_date_annotation(annotation):
                coercer[name] = functools.partial(cls._to_date, allowed=allowed)
            elif allowed := cls._match_delta_annotation(annotation):
                coercer[name] = functools.partial(cls._to_delta, allowed=allowed)
        return coercer

    def __missing__(self, name: str) -> Callable[[Any], Any]:
        return lambda value: value

    def coerce(self, name: str, value: Any) -> Any:
        """Coerce *value* for argument *name*."""
        return self[name](value)

    @classmethod
    def _match_datetime_annotation(cls, annotation: str) -> frozenset[str]:
        """Return the datetime type names *annotation* mentions, stdlib or pendulum."""
        names = (ref.rsplit(".", 1)[-1] for ref in cls._TYPE_NAME_RE.findall(annotation))
        return frozenset(name for name in names if name in cls._DATETIME_TYPE_NAMES)

    @classmethod
    def _to_datetime(cls, value: Any, allowed: frozenset[str]) -> Any:
        if isinstance(value, str):
            try:
                result = datetime.datetime.fromisoformat(value)
            except ValueError:
                return value
        elif isinstance(value, datetime.datetime):
            result = value
        elif isinstance(value, datetime.date):
            result = datetime.datetime.combine(value, datetime.time.min)
        else:
            return value
        if "DateTime" in allowed:
            return pendulum.instance(result)
        return result

    @classmethod
    def _match_date_annotation(cls, annotation: str) -> frozenset[str]:
        """Return the date type names *annotation* mentions, stdlib or pendulum."""
        names = (ref.rsplit(".", 1)[-1] for ref in cls._TYPE_NAME_RE.findall(annotation))
        return frozenset(name for name in names if name in cls._DATE_TYPE_NAMES)

    @classmethod
    def _to_date(cls, value: Any, allowed: frozenset[str]) -> Any:
        if isinstance(value, str):
            try:
                result = datetime.date.fromisoformat(value)
            except ValueError:
                return value
        elif isinstance(value, datetime.date):
            result = value
        else:
            return value
        # A datetime is a date subtype, so it is left as-is; only a plain date is
        # wrapped for a ``pendulum.Date`` annotation.
        if "Date" in allowed and type(result) is datetime.date:
            return pendulum.instance(result)
        return result

    @classmethod
    def _match_delta_annotation(cls, annotation: str) -> frozenset[str]:
        """Return the duration delta type names *annotation* mentions, stdlib, dateutil, or pendulum."""
        names = (ref.rsplit(".", 1)[-1] for ref in cls._TYPE_NAME_RE.findall(annotation))
        return frozenset(name for name in names if name in cls._DELTA_TYPE_NAMES)

    @classmethod
    def _to_delta(cls, value: Any, allowed: frozenset[str]) -> Any:
        """
        Parse a duration string into a Python type.

        This can produce a :class:`datetime.timedelta`,
        :class:`dateutil.relativedelta.relativedelta`, or ``pendulum.Duration``.

        An ISO-8601 format duration (e.g. ``P1DT6H``, ``P1Y2M``) and a short
        form (``5m``, ``1h30m``) are accepted. A value that cannot be parsed
        into the target duration type is returned unchanged.

        The short form does not support specifying years or months. Similarly,
        when ``timedelta`` is the only accepted duration type on an argument, an
        ISO duration string carrying year or month components would be left
        unparsed. A ``relativedelta`` or ``pendulum.Duration`` argument would
        accept those components, so the same ISO duration string would be parsed.

        The concrete type follows the annotation. When more than one annotated
        type can hold the value, a ``timedelta`` is preferred for a value without
        years or months, and a ``relativedelta`` over a ``pendulum.Duration`` for
        years and months (which ``pendulum.Duration`` only approximates).
        """
        if not isinstance(value, str):
            return value
        if (parts := cls._parse_iso_duration(value)) is not None:
            if parts["years"] or parts["months"]:
                if "relativedelta" in allowed:
                    return cls._as_relativedelta(parts)
                if "Duration" in allowed:
                    return cls._as_duration(parts)
                return value
            if "timedelta" in allowed:
                return cls._as_timedelta(parts)
            if "Duration" in allowed:
                return cls._as_duration(parts)
            if "relativedelta" in allowed:
                return cls._as_relativedelta(parts)
            return value
        if (parts := cls._parse_short_duration(value)) is not None:
            if "timedelta" in allowed:
                return datetime.timedelta(**parts)
            if "Duration" in allowed:
                return pendulum.Duration(**parts)
            return value
        return value

    @classmethod
    def _as_relativedelta(cls, parts: dict[str, int]) -> relativedelta:
        """Build a relativedelta from duration component ints."""
        return relativedelta(
            years=parts["years"],
            months=parts["months"],
            weeks=parts["weeks"],
            days=parts["days"],
            hours=parts["hours"],
            minutes=parts["minutes"],
            seconds=parts["seconds"],
        )

    @classmethod
    def _as_timedelta(cls, parts: dict[str, int]) -> datetime.timedelta:
        """Build a timedelta from duration component ints (years/months ignored)."""
        return datetime.timedelta(
            weeks=parts["weeks"],
            days=parts["days"],
            hours=parts["hours"],
            minutes=parts["minutes"],
            seconds=parts["seconds"],
        )

    @classmethod
    def _as_duration(cls, parts: dict[str, int]) -> pendulum.Duration:
        """Build a pendulum.Duration from duration component ints."""
        return pendulum.Duration(
            years=parts["years"],
            months=parts["months"],
            weeks=parts["weeks"],
            days=parts["days"],
            hours=parts["hours"],
            minutes=parts["minutes"],
            seconds=parts["seconds"],
        )

    @classmethod
    def _parse_iso_duration(cls, value: str) -> dict[str, int] | None:
        """Parse an ISO-8601 duration into component ints."""
        match = cls._ISO_DURATION_RE.match(value)
        if match is None or not any(match.groups()):
            return None
        years, months, weeks, days, hours, minutes, seconds = match.groups()
        return {
            "years": int(years or 0),
            "months": int(months or 0),
            "weeks": int(weeks or 0),
            "days": int(days or 0),
            "hours": int(hours or 0),
            "minutes": int(minutes or 0),
            "seconds": int(seconds or 0),
        }

    @classmethod
    def _parse_short_duration(cls, value: str) -> dict[str, int] | None:
        """Parse a short duration (e.g. ``1h30m``) into timedelta kwargs."""
        match = cls._SHORT_DURATION_RE.match(value)
        if match is None or not any(match.groups()):
            return None
        weeks, days, hours, minutes, seconds = match.groups()
        return {
            "weeks": int(weeks or 0),
            "days": int(days or 0),
            "hours": int(hours or 0),
            "minutes": int(minutes or 0),
            "seconds": int(seconds or 0),
        }


def _build_schedule(schedule: Any) -> Any:
    """Build *schedule* into a schedule construct for :class:`DAG`."""
    if isinstance(schedule, TimetableSchedule):
        timetable_cls = import_string(schedule.uses)
        return timetable_cls(**_coerce_temporal(schedule.with_, timetable_cls))
    return schedule


class _FieldMarkers(NamedTuple):
    """Flags describing a value-grammar node's templating needs."""

    needs_render: bool  # carries $t or $x anywhere -- the field must render
    has_marker: bool  # carries $x/$t/$f -- only allowed on a templated field


def _field_markers(value: Any) -> _FieldMarkers:
    """
    Scan a value-grammar node for its :class:`_FieldMarkers`.

    ``$const`` is a force-literal escape, not a marker.
    """
    if isinstance(value, (XComRef, TemplateRef)):
        return _FieldMarkers(needs_render=True, has_marker=True)
    if isinstance(value, FileRef):
        return _FieldMarkers(needs_render=False, has_marker=True)
    if isinstance(value, ConstRef):
        return _FieldMarkers(needs_render=False, has_marker=False)
    if isinstance(value, Literal):
        return _field_markers(value.root)
    if isinstance(value, (dict, list)):
        children = value.values() if isinstance(value, dict) else value
        flags = [_field_markers(v) for v in children]
        return _FieldMarkers(
            needs_render=any(f.needs_render for f in flags),
            has_marker=any(f.has_marker for f in flags),
        )
    return _FieldMarkers(needs_render=False, has_marker=False)


def _topological_order(tasks: list[Task]) -> list[str]:
    """Return task ids ordered so every task follows the tasks it depends on."""
    graph = {task.id_: task.get_dependency_ids() for task in tasks}
    try:
        return list(graphlib.TopologicalSorter(graph).static_order())
    except graphlib.CycleError as exc:
        raise YamlDagBuildError(f"dependency cycle among tasks: {exc.args[1]}") from exc
