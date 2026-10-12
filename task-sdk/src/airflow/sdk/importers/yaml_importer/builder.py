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

import keyword
import linecache
from typing import TYPE_CHECKING, Any, cast

from airflow.sdk import DAG, XComArg
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
    from pathlib import Path

    from airflow.sdk import BaseOperator
    from airflow.sdk.importers.yaml_importer.models import DagDocument, Task, Value


class YamlDagBuildError(ValueError):
    """A validated document could not be built into a DAG."""


class DagBuilder:
    """
    Build a :class:`DagDocument` into a :class:`DAG`.

    This holds the state a single build shares. The state belongs to one build
    and is discarded with the instance.
    """

    def __init__(self, source_dir: Path | None) -> None:
        self._source_dir = source_dir
        self._operators: dict[str, BaseOperator] = {}
        self._stub_callables: dict[tuple[str, ...], Callable[..., None]] = {}

    def build(self, doc: DagDocument) -> DAG:
        tasks = {t.id_: t for t in doc.tasks}
        with DAG(dag_id=doc.dag_id, schedule=_build_schedule(doc.schedule), **(doc.model_extra or {})) as dag:
            # A task depended by another task must be built first.
            for task_id in _topological_order(doc.tasks):
                self._operators[task_id] = self._build_operator(tasks[task_id])

            # Wire dependency edges explicitly.
            for task in doc.tasks:
                for upstream in sorted(task.get_dependency_ids()):
                    self._operators[upstream] >> self._operators[task.id_]
        return dag

    def _build_stub_task(self, task: Task, args: dict[str, Value]) -> BaseOperator:
        """
        Build a stub from a "run:" task.

        The task body is a registered SDK task handler resolved at run time; the
        run mapping binds as that handler's arguments, where XCom and template
        references resolve natively. The stub is fed a callable whose parameters
        are the run argument names, so serialization can build and later
        validate (AIP-108) the handler's argument spec.
        """
        from airflow.sdk import task as task_decorators  # Lazy to avoid provider registration.

        run_kwargs: dict[str, Any] = {}
        for name, value in args.items():
            if not name.isidentifier() or keyword.iskeyword(name):
                raise YamlDagBuildError(f"task {task.id_!r}: run argument {name!r} is not a valid identifier")
            run_kwargs[name] = self._lower(value)[0]
        decorator = task_decorators.stub(task_id=task.id_, **(task.model_extra or {}))
        return decorator(self._stub_callable(tuple(run_kwargs)))(**run_kwargs).operator

    def _build_operator(self, task: Task) -> BaseOperator:
        """Instantiate *task* as an operator."""
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
            kwargs[name], needs_render, has_marker = self._lower(value)
            if has_marker and name not in template_fields:
                off_template.append(name)
            if needs_render:
                render_fields.append(name)
        if off_template:
            raise YamlDagBuildError(f"task {task.id_!r}: value grammar only allowed on template_fields")
        if extras := task.model_extra:  # Merge in task-level arguments (raw, never templated).
            kwargs.update(extras)

        return _apply_template_lowering(op_cls(task_id=task.id_, **kwargs), tuple(render_fields))

    def _lower(self, value: Any) -> tuple[Any, bool, bool]:
        """
        Lower a resolved value-grammar node.

        The return value is ``(operator argument, needs_render, has_marker)``.

        * ``needs_render`` is True if the node carries ``$t`` or ``$x``, and we
          must validate it's in the operator's ``template_fields``.
          reference anywhere, i.e.
        * ``has_marker`` is True when it carries any value-grammar markers, and
          we should only be allowed if the field can be templated.
        """
        if isinstance(value, XComRef):
            return self._xcom_arg(value), True, True
        if isinstance(value, TemplateRef):
            # $t over a string renders that string; $t over $f is a template file (read, then render).
            if isinstance(value.source, FileRef):
                # A constructed FileRef has a str path (its validator rejects a templated filename).
                return self._read_file(cast("str", value.source.path)), True, True
            return value.source, True, True
        if isinstance(value, FileRef):
            return self._read_file(cast("str", value.path)), False, True  # verbatim; not a template
        if isinstance(value, ConstRef):
            return value.value, False, False
        if isinstance(value, Literal):
            return self._lower(value.root)
        if isinstance(value, dict):
            items = {k: self._lower(v) for k, v in value.items()}
            return (
                {k: lv for k, (lv, _, _) in items.items()},
                any(render for _, render, _ in items.values()),
                any(marker for _, _, marker in items.values()),
            )
        if isinstance(value, list):
            elements = [self._lower(v) for v in value]
            return (
                [lv for lv, _, _ in elements],
                any(render for _, render, _ in elements),
                any(marker for _, _, marker in elements),
            )
        return value, False, False

    def _xcom_arg(self, value: XComRef) -> XComArg:
        """Resolve an :class:`XComRef` against the already-built operators."""
        target = value.target
        if isinstance(target, str):
            return XComArg(self._operators[target])
        if target.key is None:
            return XComArg(self._operators[target.task])
        return XComArg(self._operators[target.task], key=target.key)

    def _read_file(self, rel_path: str) -> str:
        """Read a ``$f`` at build time, resolved against ``source_dir``."""
        if self._source_dir is None:
            raise YamlDagBuildError(f"cannot read $f {rel_path!r}: the document has no source directory")
        try:
            return (self._source_dir / rel_path).read_text(encoding="utf-8")
        except OSError as exc:
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


def _apply_template_lowering(op: BaseOperator, templated_fields: tuple[str, ...]) -> BaseOperator:
    """
    Make *op* render exactly *templated_fields*.

    This allows us to implement AIP-80 file-reading semantics without changing
    core; fortunately Airflow reads them per-instance so a simple patch works.
    """
    op.template_fields = templated_fields
    op.template_ext = ()
    return op


def _build_schedule(schedule: Any) -> Any:
    """Build *schedule* into a schedule construct for :class:`DAG`."""
    if isinstance(schedule, TimetableSchedule):
        return import_string(schedule.uses)(**schedule.with_)
    return schedule


def _topological_order(tasks: list[Task]) -> list[str]:
    """Return task ids ordered so every task follows the tasks it depends on."""
    upstreams = {task.id_: task.get_dependency_ids() for task in tasks}
    order: list[str] = []
    done: set[str] = set()
    remaining = [task.id_ for task in tasks]
    while remaining:
        ready = [task_id for task_id in remaining if upstreams[task_id] <= done]
        if not ready:
            raise YamlDagBuildError(f"dependency cycle among tasks: {sorted(remaining)}")
        order.extend(ready)
        done.update(ready)
        remaining = [task_id for task_id in remaining if task_id not in done]
    return order
