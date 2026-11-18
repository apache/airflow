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
Pydantic models for the YAML/JSON Dag format.

These are the single source of truth for the format; they validate an authored
document (rich Pydantic errors) and back the published JSON Schema. Only the
format skeleton and value grammar are described; operator ``with:`` args,
Dag- and task-level attributes pass through untyped because the Python
constructors validate those at import time.
"""

from __future__ import annotations

import collections
import contextlib
import copy
import datetime
import itertools
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, NamedTuple

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Discriminator,
    Field,
    RootModel,
    Tag,
    field_validator,
    model_validator,
)

from airflow.sdk.bases.xcom import BaseXCom

if TYPE_CHECKING:
    from collections.abc import Iterator
    from typing import Protocol, TypeAlias, TypeVar

    from pydantic import GetJsonSchemaHandler
    from pydantic.json_schema import JsonSchemaValue
    from pydantic_core import CoreSchema

    from airflow.sdk import XComArg
    from airflow.sdk.definitions._internal.templater import LiteralValue

    _T = TypeVar("_T")

    _OperatorArgument: TypeAlias = (
        "str | int | float | bool | datetime.datetime | datetime.date | None | "  # Plain values.
        "XComArg | LiteralValue | "  # Airflow values.
        "list[_OperatorArgument] | dict[str, _OperatorArgument]"  # Nested (recursive types).
    )

    class _ArgumentContext(Protocol):
        """What a value model needs from the builder to turn itself into an operator argument."""

        def build_xcom_arg(self, ref: XComRef) -> XComArg: ...
        def read_file(self, path: str) -> str: ...
        def ensure_literal(self, value: _T, *, rendered: bool) -> _T | LiteralValue: ...


XCOM_KEYS = ("$x", "$xcom")
TEMPLATE_KEYS = ("$t", "$template")
FILE_KEYS = ("$f", "$file")
CONST_KEY = "$const"

# Keys deferred to a later edition...
DAG_CALLBACK_KEYS = {"on_success_callback", "on_failure_callback"}
TASK_CALLBACK_KEYS = {
    "on_success_callback",
    "on_failure_callback",
    "on_retry_callback",
    "on_execute_callback",
    "on_skipped_callback",
}
TASK_ASSET_IO_KEYS = {"inlets", "outlets"}


def _as_exclusive_union(schema: JsonSchemaValue) -> JsonSchemaValue:
    """
    Rewrite a disjoint ``anyOf`` union in place to ``oneOf``.

    Pydantic emits ``anyOf`` (one or more match) for ``X | Y`` unions, but when
    the branches are mutually exclusive, ``oneOf`` (exactly one match) accepts
    the same values but generates better client code.

    Only call this on unions whose branches are provably disjoint; on an
    overlapping union, ``oneOf`` incorrectly rejects valid values.

    This operation is performed in-place. The same modified object is returned.
    """
    with contextlib.suppress(KeyError):
        schema["oneOf"] = schema.pop("anyOf")
    return schema


class _MarkerFlags(NamedTuple):
    """Whether a value-grammar node needs rendering, and whether it carries a marker."""

    needs_render: bool  # carries $t or $x anywhere -- the field must render
    has_marker: bool  # carries $x/$t/$f -- only allowed on a templated field


class _MarkerRef(BaseModel):
    """
    Base for a reserved ``$``-marker object.

    A marker's operand is the value of its ``$`` key. Its options are plain keys
    beside the ``$`` key, e.g. ``{$x: task, key: k}``. Subclasses set
    ``_marker_keys`` and ``_operand_field``.
    """

    model_config = ConfigDict(extra="forbid")

    _marker_keys: ClassVar[tuple[str, ...]]
    """The ``$`` keys that spell this marker."""

    _operand_field: ClassVar[str]
    """Name of the field holding the operand; every other field is an option."""

    @model_validator(mode="before")
    @classmethod
    def _reject_unknown_options(cls, data: Any) -> Any:
        """Reject a key beside the ``$`` key that is not one of this marker's options."""
        if not isinstance(data, dict):
            return data
        if (marker := next((k for k in data if k in cls._marker_keys), None)) is None:
            return data  # Not this marker at all; let field validation report what is missing.
        options = {name for name in cls.model_fields if name != cls._operand_field}
        if not (unknown := [k for k in data if k not in options and k not in cls._marker_keys]):
            return data
        expected = f"expected one of {sorted(options)}" if options else "it takes no options"
        raise ValueError(f"{unknown[0]!r} is not an option of {marker}; {expected}")

    @classmethod
    def _process_value_schema(cls, field_schema: JsonSchemaValue) -> JsonSchemaValue:
        """
        Post-process the operand's published schema, once for each marker key.

        The default publishes the schema Pydantic generates as-is.
        """
        return field_schema

    @classmethod
    def __get_pydantic_json_schema__(
        cls, core_schema: CoreSchema, handler: GetJsonSchemaHandler
    ) -> JsonSchemaValue:
        schema = handler(core_schema)
        schema["additionalProperties"] = False

        # Pydantic lists the operand under one of its aliases; pop it, leaving only the options.
        options = schema["properties"]
        (operand,) = (options.pop(key) for key in cls._marker_keys if key in options)
        schema["properties"] = {
            **{key: cls._process_value_schema(copy.deepcopy(operand)) for key in cls._marker_keys},
            **options,
        }

        required_options = [k for k in schema.pop("required", ()) if k not in cls._marker_keys]
        if len(cls._marker_keys) == 1:
            schema["required"] = [*cls._marker_keys, *required_options]
        else:  # Exactly one spelling of the marker key.
            schema["oneOf"] = [{"required": [key]} for key in cls._marker_keys]
            if required_options:
                schema["required"] = required_options

        return schema

    def iter_dependency_task_ids(self) -> Iterator[str]:
        """Task ids this node references via XCom; overridden by :class:`XComRef`."""
        return iter(())

    def get_run_binding_error(self, *, top_level: bool = True) -> str | None:
        """Why this node can't cross a run: handler binding, or None; overridden by $t/$x."""
        return None

    def build_marker_flags(self) -> _MarkerFlags:
        """Whether this node needs rendering and whether it carries a marker."""
        raise NotImplementedError

    def as_operator_argument(self, ctx: _ArgumentContext, *, rendered: bool) -> _OperatorArgument:
        """Turn this node into an operator argument."""
        raise NotImplementedError


class XComRef(_MarkerRef):
    """An upstream task's XCom output."""

    _marker_keys = XCOM_KEYS
    _operand_field = "target"

    target: str = Field(
        serialization_alias="$x",
        validation_alias=AliasChoices(*XCOM_KEYS),
        title="XCom task ID",
    )
    key: str = Field(default=BaseXCom.XCOM_RETURN_KEY, title="XCom key")

    @model_validator(mode="before")
    @classmethod
    def _reject_nested_target(cls, data: Any) -> Any:
        """Point the removed ``{$x: {task: t, key: k}}`` form at its replacement."""
        if not isinstance(data, dict):
            return data
        for marker in cls._marker_keys:
            operand = data.get(marker)
            if not isinstance(operand, dict) or "task" not in operand:
                continue
            if not operand.keys() <= {"task", "key"}:
                continue  # Not the old form either; leave it to field validation.
            if (key := operand.get("key")) is None:
                fixed = f"{marker}: {operand['task']}"
            else:
                fixed = f"{marker}: {operand['task']}, key: {key}"
            raise ValueError(f"the key of an XCom reference goes beside {marker}; write {{{fixed}}}")
        return data

    def build_marker_flags(self) -> _MarkerFlags:
        return _MarkerFlags(needs_render=True, has_marker=True)

    def iter_dependency_task_ids(self) -> Iterator[str]:
        yield self.target

    def get_run_binding_error(self, *, top_level: bool = True) -> str | None:
        if self.key != BaseXCom.XCOM_RETURN_KEY:
            return "non-return-value XCom is not supported in a 'run:' task"
        if not top_level:
            return (
                "an XCom ($x) nested inside a run argument is not supported; "
                "pass the upstream output as its own top-level run argument"
            )
        return None

    def as_operator_argument(self, ctx: _ArgumentContext, *, rendered: bool) -> XComArg:
        return ctx.build_xcom_arg(self)


class TemplateRef(_MarkerRef):
    """
    A Jinja template, rendered when the task runs.

    The template is a Jinja string, or a file reference (``$f``) to load a
    template file.
    """

    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "description": (
                "A Jinja template, rendered when the task runs. This can be an "
                "inline string, or a file reference ($f) to load a template file."
            ),
        },
    )

    _marker_keys = TEMPLATE_KEYS
    _operand_field = "source"

    source: str | FileRef = Field(
        serialization_alias="$t",
        validation_alias=AliasChoices(*TEMPLATE_KEYS),
        title="Jinja template",
    )

    @classmethod
    def _process_value_schema(cls, field_schema: JsonSchemaValue) -> JsonSchemaValue:
        return _as_exclusive_union(field_schema)  # A string and an $f object are disjoint.

    def build_marker_flags(self) -> _MarkerFlags:
        return _MarkerFlags(needs_render=True, has_marker=True)

    def get_run_binding_error(self, *, top_level: bool = True) -> str | None:
        return (
            "a template ($t) reference is not supported in a run: task; the task "
            "handler binding carries literal values and XCom return values, not rendered templates"
        )

    def as_operator_argument(self, ctx: _ArgumentContext, *, rendered: bool) -> str:
        # $t over a string renders that string; $t over $f is a template file (read, then render).
        if isinstance(self.source, FileRef):
            return ctx.read_file(self.source.path)
        return self.source


class FileRef(_MarkerRef):
    """
    A file input (AIP-80 FileTemplate).

    The path is a static string. A templated filename (a ``$f`` wrapping a
    ``$t``) is reserved for a future release and rejected for now.
    """

    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={"description": "A file input."},
    )

    _marker_keys = FILE_KEYS
    _operand_field = "path"

    path: str = Field(
        serialization_alias="$f",
        validation_alias=AliasChoices(*FILE_KEYS),
        title="File path",
    )

    @field_validator("path", mode="before")
    @classmethod
    def _reject_templated_filename(cls, v: Any) -> Any:
        if _value_discriminator(v) == "template":
            raise ValueError("templated filename ($f wrapping $t) is not supported yet")
        return v

    def build_marker_flags(self) -> _MarkerFlags:
        return _MarkerFlags(needs_render=False, has_marker=True)

    def as_operator_argument(self, ctx: _ArgumentContext, *, rendered: bool) -> str | LiteralValue:
        return ctx.ensure_literal(ctx.read_file(self.path), rendered=rendered)


class ConstRef(_MarkerRef):
    """Force a literal; the value is taken verbatim."""

    _marker_keys = (CONST_KEY,)
    _operand_field = "value"

    value: Any = Field(
        serialization_alias="$const",
        validation_alias=AliasChoices(CONST_KEY),
        title="Literal value",
    )

    def build_marker_flags(self) -> _MarkerFlags:
        return _MarkerFlags(needs_render=False, has_marker=False)

    def as_operator_argument(self, ctx: _ArgumentContext, *, rendered: bool) -> Any:
        return ctx.ensure_literal(self.value, rendered=rendered)


def _is_reserved_key(key: Any) -> bool:
    return isinstance(key, str) and key.startswith("$")


def _value_discriminator(v: Any) -> str:
    """
    Route a raw value to a marker branch, or to ``literal``.

    The prefix ``$`` is reserved to express Airflow constructs. To prevent
    typos, any unknown ``$``-prefixed strings are routed here and rejected.

    If such a key is needed, it can be wrapped in ``$const``, which would
    avoid this discriminator (only used by :class:`Literal`).

    A mapping is routed by its one ``$``-prefixed key; any other keys are that
    marker's options, validated by the marker. A mapping with more than one
    ``$`` key is routed to ``literal``, which rejects it.
    """
    if isinstance(v, dict) and len(dollar_keys := [k for k in v if _is_reserved_key(k)]) == 1:
        (key,) = dollar_keys
        if key in XCOM_KEYS:
            return "xcom"
        if key in TEMPLATE_KEYS:
            return "template"
        if key in FILE_KEYS:
            return "file"
        if key == CONST_KEY:
            return "const"
    if isinstance(v, XComRef):
        return "xcom"
    if isinstance(v, TemplateRef):
        return "template"
    if isinstance(v, FileRef):
        return "file"
    if isinstance(v, ConstRef):
        return "const"
    return "literal"


class Literal(RootModel):
    """
    A literal value.

    Containers are validated recursively so nested markers resolve; scalars pass
    through. A ``$``-prefixed key here is a misused marker, so it is rejected.
    """

    root: dict[str, Value] | list[Value] | str | int | float | bool | datetime.datetime | datetime.date | None

    model_config = ConfigDict(
        json_schema_extra={
            "not": {"type": "object", "not": {"patternProperties": {r"^\$": False}}},
            "description": "A literal value.",
        }
    )

    @model_validator(mode="before")
    @classmethod
    def _reject_reserved_keys(cls, v: Any) -> Any:
        if isinstance(v, dict):
            if len(reserved := [k for k in v if _is_reserved_key(k)]) > 1:
                raise ValueError(f"a value may carry only one '$' marker, found {reserved}")
            if reserved:
                raise ValueError(
                    f"unexpected key {reserved[0]!r}: '$'-prefixed keys are reserved "
                    f"markers; wrap a literal '$' key in $const"
                )
        return v

    @classmethod
    def __get_pydantic_json_schema__(
        cls, core_schema: CoreSchema, handler: GetJsonSchemaHandler
    ) -> JsonSchemaValue:
        # Pydantic generates a pretty messy schema from this. Clean it up since
        # essentially we accept "whatever the format can implicitly accept".
        schema = handler(core_schema)
        schema["oneOf"] = [
            *(b for b in schema.pop("anyOf") if b.get("type") in ("object", "array")),
            {"type": "boolean"},
            {"type": "null"},
            {"type": "number"},
            {"type": "string"},
        ]
        return schema

    def _iter_nested_values(self) -> Iterator[Value]:
        """Yield the nested values of a container literal; nothing for a scalar."""
        if isinstance(self.root, dict):
            yield from self.root.values()
        elif isinstance(self.root, list):
            yield from self.root

    def build_marker_flags(self) -> _MarkerFlags:
        child_flags = [child.build_marker_flags() for child in self._iter_nested_values()]
        return _MarkerFlags(
            needs_render=any(m.needs_render for m in child_flags),
            has_marker=any(m.has_marker for m in child_flags),
        )

    def iter_dependency_task_ids(self) -> Iterator[str]:
        for child in self._iter_nested_values():
            yield from child.iter_dependency_task_ids()

    def get_run_binding_error(self, *, top_level: bool = True) -> str | None:
        # Anything inside a literal container is nested, so a bindable top-level $x cannot live here.
        for child in self._iter_nested_values():
            if error := child.get_run_binding_error(top_level=False):
                return error
        return None

    def as_operator_argument(self, ctx: _ArgumentContext, *, rendered: bool) -> _OperatorArgument:
        if isinstance(self.root, dict):
            return {k: v.as_operator_argument(ctx, rendered=rendered) for k, v in self.root.items()}
        if isinstance(self.root, list):
            return [v.as_operator_argument(ctx, rendered=rendered) for v in self.root]
        return ctx.ensure_literal(self.root, rendered=rendered)


Value = Annotated[
    Annotated[XComRef, Tag("xcom")]
    | Annotated[TemplateRef, Tag("template")]
    | Annotated[FileRef, Tag("file")]
    | Annotated[ConstRef, Tag("const")]
    | Annotated[Literal, Tag("literal")],
    Discriminator(_value_discriminator),
]

TemplateRef.model_rebuild()
FileRef.model_rebuild()
Literal.model_rebuild()


class Task(BaseModel):
    """
    A ready-made operator (``uses``) or task with custom code (``run``).

    At most one of ``uses`` or ``run`` may carry a body; ``null`` or omission
    means no body. A task with neither will be turned by the importer into an
    ``EmptyOperator``. Unknown keys pass through untyped as operator arguments.
    """

    id_: str = Field(alias="id", min_length=1, title="Task ID")
    needs: list[str] = Field(default_factory=list)
    extends: list[str] = Field(default_factory=list, title="Template names")
    with_: dict[str, Value] = Field(default_factory=dict, alias="with", title="Operator arguments")
    uses: str | None = Field(default=None, min_length=1, title="Operator class")
    run: dict[str, Value] | None = Field(default=None, title="Task handler arguments")

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={"description": "A ready-made operator (uses) or task with custom code (run)."},
    )

    @model_validator(mode="before")
    @classmethod
    def _check_task(cls, data: Any) -> Any:
        if isinstance(data, dict):
            for k in TASK_ASSET_IO_KEYS:
                if k in data:
                    raise ValueError(f"{k!r} not yet implemented")
            for k in TASK_CALLBACK_KEYS:
                if k in data:
                    raise ValueError(f"{k!r} (callback) not yet implemented")
            if sum(1 for k in ("uses", "run") if data.get(k) is not None) > 1:
                raise ValueError(f"task {data.get('id')!r} may not set both 'uses' and 'run'")
        return data

    def get_dependency_ids(self) -> set[str]:
        """Ids of the tasks this task depends on: explicit ``needs`` plus any XCom references."""
        ids = set(self.needs)
        for value in itertools.chain(self.with_.values(), (self.run or {}).values()):
            ids.update(value.iter_dependency_task_ids())
        return ids

    @classmethod
    def __get_pydantic_json_schema__(
        cls, core_schema: CoreSchema, handler: GetJsonSchemaHandler
    ) -> JsonSchemaValue:
        schema = handler(core_schema)
        schema["not"] = {
            "allOf": [
                {"required": ["uses"], "properties": {"uses": {"type": "string"}}},
                {"required": ["run"], "properties": {"run": {"type": "object"}}},
            ]
        }

        properties = schema["properties"]
        properties["uses"] = {
            "title": properties["uses"]["title"],
            "type": ["string", "null"],
            "minLength": 1,
        }
        properties["run"] = {
            "title": properties["run"]["title"],
            "type": ["object", "null"],
            "additionalProperties": properties["with"]["additionalProperties"],
        }
        properties["extends"] = {
            "title": properties["extends"]["title"],
            "oneOf": [{"type": "string"}, {"type": "array", "items": {"type": "string"}}],
        }

        return schema


class TaskTemplate(BaseModel):
    """
    A reusable, partial task fragment merged into any task that ``extends`` it.

    This holds arbitrary keys as-is. All validation is done on the merged task.
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "description": (
                "A reusable, partial task fragment merged into any task that "
                "extends it.\n\nThis holds arbitrary keys as-is. All "
                "validation is done on the merged task."
            ),
        },
    )


class TimetableSchedule(BaseModel):
    """A constructed timetable."""

    uses: str = Field(min_length=1, title="Timetable class")
    with_: dict[str, Any] = Field(default_factory=dict, alias="with", title="Timetable arguments")

    model_config = ConfigDict(extra="forbid")


Schedule = str | None | TimetableSchedule


def _merge_task(base: dict, over: dict) -> dict:
    """
    Shallow merge *over* onto *base*.

    Note that ``with`` and ``run`` need to merge one level deep. All other
    fields simply replace.
    """
    out = base.copy()
    for k, v in over.items():
        if k in ("with", "run") and isinstance(v, dict) and isinstance(out.get(k), dict):
            merged = dict(out[k])
            merged.update(v)
            out[k] = merged
        else:
            out[k] = v
    return out


def _validate_extends(value: Any, *, where: str) -> list[str]:
    """
    Return *value* as the list of template names it must be, or raise a clear ``ValueError``.

    A bare string is accepted as shorthand for a single-element list.
    """
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if not isinstance(value, list) or not all(isinstance(name, str) for name in value):
        raise ValueError(f"{where}: 'extends' must be a list of template names, got {value!r}")
    return value


def _expand_template(name: str, templates: dict[str, Any], _seen: tuple = ()) -> dict:
    if name in _seen:
        raise ValueError(f"template cycle via {name!r}")
    try:
        spec = templates[name]
    except KeyError:
        raise ValueError(f"unknown template {name!r}") from None
    if not isinstance(spec, dict):
        raise ValueError(f"template {name!r} must be a mapping")
    acc: dict = {}
    for parent in _validate_extends(spec.get("extends"), where=f"template {name!r}"):
        acc = _merge_task(acc, _expand_template(parent, templates, _seen + (name,)))
    own = {k: v for k, v in spec.items() if k != "extends"}
    return _merge_task(acc, own)


class DagDocument(BaseModel):
    """
    A Dag represented by one YAML document.

    Extra top-level keys are Dag-level arguments.
    """

    schema_: str = Field(alias="$schema", title="Dag format schema URL")
    dag_id: str = Field(min_length=1, title="Dag ID")
    schedule: Schedule = None
    templates: dict[str, TaskTemplate] = Field(default_factory=dict)
    tasks: list[Task] = Field(default_factory=list)

    model_config = ConfigDict(extra="allow")

    @classmethod
    def __get_pydantic_json_schema__(
        cls, core_schema: CoreSchema, handler: GetJsonSchemaHandler
    ) -> JsonSchemaValue:
        schema = handler(core_schema)
        _as_exclusive_union(schema["properties"]["schedule"])
        return schema

    @model_validator(mode="before")
    @classmethod
    def _reject_deferred(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if "default_args" in data:
                raise ValueError("default_args is not supported; use 'templates' with 'extends' instead")
            for k in DAG_CALLBACK_KEYS:
                if k in data:
                    raise ValueError(f"Dag-level {k!r} not yet implemented")
        return data

    @model_validator(mode="before")
    @classmethod
    def _resolve_extends(cls, data: Any) -> Any:
        """Merge in each task's ``extends`` templates before task validation."""
        if not isinstance(data, dict):
            return data
        templates = data.get("templates")
        if templates is None:
            templates = {}
        tasks = data.get("tasks")
        # Only merge when the shapes are as expected. A malformed input is left
        # untouched, so normal field validation reports it as a ValidationError.
        if not isinstance(templates, dict) or not isinstance(tasks, list):
            return data
        resolved = []
        for task in tasks:
            if not isinstance(task, dict) or "extends" not in task:
                resolved.append(task)
                continue
            if not (extends := _validate_extends(task["extends"], where=f"task {task.get('id')!r}")):
                resolved.append(task)
                continue
            merged: dict = {}
            for name in extends:
                merged = _merge_task(merged, _expand_template(name, templates))
            resolved.append(_merge_task(merged, {k: v for k, v in task.items() if k != "extends"}))
        return {**data, "tasks": resolved}

    @model_validator(mode="after")
    def _check_refs(self):
        """Check task ID uniqueness and ensure dependencies are internal."""
        task_id_counts = collections.Counter(t.id_ for t in self.tasks)
        if duplicates := sorted(i for i, n in task_id_counts.items() if n > 1):
            raise ValueError(f"duplicate task id(s): {duplicates}")
        validated_task_ids = set(task_id_counts)
        for t in self.tasks:
            if t.id_ in (refs := t.get_dependency_ids()):
                raise ValueError(f"task {t.id_!r} depends on itself")
            if missing := (refs - validated_task_ids):
                raise ValueError(f"task {t.id_!r} references unknown task(s): {sorted(missing)}")
        return self
