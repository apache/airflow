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
Pydantic models for the YAML/JSON DAG format.

These are the single source of truth for the format; they validate an authored
document (rich Pydantic errors) and back the published JSON Schema. Only the
format skeleton and value grammar only is described; operator ``with:`` args,
Dag- and task-level attributes pass through untyped because the Python
constructors validate those at import time.
"""

from __future__ import annotations

import collections
import itertools
from typing import TYPE_CHECKING, Annotated, Any, ClassVar

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Discriminator,
    Field,
    RootModel,
    Tag,
    model_validator,
)

if TYPE_CHECKING:
    from pydantic import GetJsonSchemaHandler
    from pydantic.json_schema import JsonSchemaValue
    from pydantic_core import CoreSchema

XCOM_KEYS = ("$x", "$xcom")
TEMPLATE_KEYS = ("$t", "$template")
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


class XComTarget(BaseModel):
    """The object form of an XCom reference."""

    task: str
    key: str | None = None

    model_config = ConfigDict(extra="forbid")


class _MarkerRef(BaseModel):
    """Base for a reserved ``$``-marker object."""

    model_config = ConfigDict(extra="forbid")

    _marker_keys: ClassVar[tuple[str, ...]]

    @classmethod
    def __get_pydantic_json_schema__(
        cls, core_schema: CoreSchema, handler: GetJsonSchemaHandler
    ) -> JsonSchemaValue:
        # Emit the marker as a single key in every accepted spelling, nothing else.
        schema = handler(core_schema)
        (value_schema,) = schema.get("properties", {}).values()
        schema["properties"] = {key: value_schema for key in cls._marker_keys}
        schema.pop("required", None)
        schema["oneOf"] = [{"required": [key]} for key in cls._marker_keys]
        schema["additionalProperties"] = False
        return schema


class XComRef(_MarkerRef):
    """An upstream task's XCom output."""

    _marker_keys = XCOM_KEYS

    target: str | XComTarget = Field(
        serialization_alias="$x",
        validation_alias=AliasChoices(*XCOM_KEYS),
    )


class TemplateRef(_MarkerRef):
    """A Jinja template."""

    _marker_keys = TEMPLATE_KEYS

    source: str = Field(serialization_alias="$t", validation_alias=AliasChoices(*TEMPLATE_KEYS))


class ConstRef(_MarkerRef):
    """Force a literal; the value is taken verbatim."""

    _marker_keys = (CONST_KEY,)

    value: Any = Field(serialization_alias="$const", validation_alias=AliasChoices(CONST_KEY))


def _value_discriminator(v: Any) -> str:
    """
    Route a raw value to a marker branch, or to ``literal``.

    The prefix ``$`` is reserved to express Airflow constructs. To prevent
    typos, any unknown ``$``-prefixed strings are routed here and rejected.

    If such a key is needed, it can be wrapped in ``$const``, which would
    avoid this discrimator (only used by :class:`_Literal`).
    """
    if isinstance(v, dict) and len(v) == 1:
        (key,) = v
        if key in XCOM_KEYS:
            return "xcom"
        if key in TEMPLATE_KEYS:
            return "template"
        if key == CONST_KEY:
            return "const"
    if isinstance(v, XComRef):
        return "xcom"
    if isinstance(v, TemplateRef):
        return "template"
    if isinstance(v, ConstRef):
        return "const"
    return "literal"


class _Literal(RootModel):
    """
    A literal value.

    Containers are validated recursively so nested markers resolve; scalars pass
    through. A ``$``-prefixed key here is a misused marker, so it is rejected.
    """

    root: dict[str, Value] | list[Value] | str | int | float | bool | None

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
            if (res := next((k for k in v if isinstance(k, str) and k.startswith("$")), None)) is not None:
                raise ValueError(
                    f"unexpected key {res!r}: '$'-prefixed keys are reserved "
                    f"markers; wrap a literal '$' key in $const"
                )
        return v


Value = Annotated[
    Annotated[XComRef, Tag("xcom")]
    | Annotated[TemplateRef, Tag("template")]
    | Annotated[ConstRef, Tag("const")]
    | Annotated[_Literal, Tag("literal")],
    Discriminator(_value_discriminator),
]

_Literal.model_rebuild()


class Task(BaseModel):
    """
    A ready-made operator (``uses``) or task with custom code (``run``).

    Exactly one body (either ``uses`` or ``run``) may be present once
    templates (``extends``) are merged. Unknown keys are operator arguments;
    they pass through untyped.
    """

    id_: str = Field(alias="id")
    needs: list[str] = Field(default_factory=list)
    extends: list[str] = Field(default_factory=list)
    with_: dict[str, Value] = Field(default_factory=dict, alias="with")
    uses: str | None = None
    run: dict[str, Value] | None = None

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
            bodies = [k for k in ("uses", "run") if k in data]
            if len(bodies) != 1:
                raise ValueError(
                    f"task {data.get('id')!r} must have exactly one of 'uses' or 'run'"
                    + (f"; got {bodies}" if bodies else "")
                )
            if data[bodies[0]] is None or data[bodies[0]] == "":  # present but empty is not a body
                raise ValueError(f"task {data.get('id')!r}: {bodies[0]!r} must not be empty")
        return data

    @classmethod
    def __get_pydantic_json_schema__(
        cls, core_schema: CoreSchema, handler: GetJsonSchemaHandler
    ) -> JsonSchemaValue:
        schema = handler(core_schema)
        value_schema = schema["properties"]["with"]["additionalProperties"]
        schema["properties"]["uses"] = {"type": "string"}
        schema["properties"]["run"] = {"type": "object", "additionalProperties": value_schema}
        schema["not"] = {"required": ["uses", "run"]}
        schema["anyOf"] = [{"required": ["uses"]}, {"required": ["run"]}, {"required": ["extends"]}]
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

    uses: str
    with_: dict[str, Any] = Field(default_factory=dict, alias="with")

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
    """Return *value* as the list of template names it must be, or raise a clear ``ValueError``."""
    if value is None:
        return []
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


def _iter_xcom_task_ids(value: Any):
    """Yield task ids referenced by any XComRef inside a resolved value."""
    if isinstance(value, XComRef):
        yield value.target if isinstance(value.target, str) else value.target.task
    elif isinstance(value, _Literal):
        yield from _iter_xcom_task_ids(value.root)
    elif isinstance(value, dict):
        for v in value.values():
            yield from _iter_xcom_task_ids(v)
    elif isinstance(value, list):
        for v in value:
            yield from _iter_xcom_task_ids(v)


class DagDocument(BaseModel):
    """
    A Dag represented by one YAML document.

    Extra top-level keys are Dag-level arguments.
    """

    schema_: str = Field(alias="$schema")
    dag_id: str
    schedule: Schedule = None
    templates: dict[str, TaskTemplate] = Field(default_factory=dict)
    tasks: list[Task] = Field(default_factory=list)

    model_config = ConfigDict(extra="allow")

    @model_validator(mode="before")
    @classmethod
    def _reject_deferred(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if "default_args" in data:
                raise ValueError("default_args is not supported; use 'templates' with 'extends' instead")
            for k in DAG_CALLBACK_KEYS:
                if k in data:
                    raise ValueError(f"Dag-level {k!r} is not implemented yet")
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
            refs = set(t.needs)
            for v in itertools.chain(t.with_.values(), (t.run or {}).values()):
                refs.update(_iter_xcom_task_ids(v))
            if t.id_ in refs:
                raise ValueError(f"task {t.id_!r} depends on itself")
            if missing := (refs - validated_task_ids):
                raise ValueError(f"task {t.id_!r} references unknown task(s): {sorted(missing)}")
        return self
