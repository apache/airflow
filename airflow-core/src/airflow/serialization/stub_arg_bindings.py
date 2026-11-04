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
Materialize the TaskFlow arg-binding spec for lang-SDK stub tasks, at Dag-serialization time.

Any operator flagged ``is_stub`` (currently only ``@task.stub``'s ``_StubOperator``) gets its
ordered positional-argument spec built here, from the live operator's already-bound
``python_callable``/``op_args``/``op_kwargs`` -- called from ``OperatorSerialization`` the same way
``is_stub`` itself is derived, so no provider needs to duplicate this against the execution API's
``TaskArgBinding`` schema.

A value that differs per map index (any argument of a mapped stub, or a mapped task group's expand
argument passed to a member) cannot be resolved at parse time, so it is a ``param`` entry that
ti_run combines with the serialized Dag to derive per map index.
"""

from __future__ import annotations

import copy
import datetime
import json
import types
import typing
from collections.abc import Mapping
from functools import cache
from inspect import Parameter, Signature, signature
from typing import TYPE_CHECKING, Any, Literal, NotRequired, cast

from pydantic import PydanticUserError, TypeAdapter
from pydantic.json_schema import GenerateJsonSchema
from typing_extensions import TypedDict

from airflow.models.xcom import XCOM_RETURN_KEY
from airflow.sdk import XComArg
from airflow.sdk.definitions._internal.expandinput import MappedArgument
from airflow.sdk.definitions.context import KNOWN_CONTEXT_KEYS
from airflow.sdk.definitions.xcom_arg import PlainXComArg

if TYPE_CHECKING:
    from airflow.sdk.bases.decorator import DecoratedMappedOperator, DecoratedOperator


class _BindingEntryBase(TypedDict):
    name: str
    value_schema: NotRequired[dict[str, Any]]


class XComBindingEntry(_BindingEntryBase):
    """An upstream task's output, read whole (``sequence``) when that task is mapped."""

    kind: Literal["xcom", "sequence"]
    task_id: str


class LiteralBindingEntry(_BindingEntryBase):
    """An inline JSON value from the Dag file, or the signature's default."""

    kind: Literal["literal"]
    value: Any
    from_default: NotRequired[bool]


class LocalBindingEntry(_BindingEntryBase):
    """An argument only the worker can resolve, such as a ``map()``/``zip()``/``concat()`` result."""

    kind: Literal["local"]


class GroupArgEntry(TypedDict):
    """The enclosing mapped task group whose expand argument *key* a ``param`` binding reads."""

    group_id: str
    key: str


class ParamBindingEntry(_BindingEntryBase):
    """A parameter the server derives per map index; it never reaches the runtime."""

    kind: Literal["param"]
    default: NotRequired[Any]
    group_arg: NotRequired[GroupArgEntry]


# TODO(#71364): validate_schema also runs on read, so a core that predates a kind rejects the whole Dag.
ArgBindingEntry = XComBindingEntry | LiteralBindingEntry | LocalBindingEntry | ParamBindingEntry
"""One serialized ``_arg_bindings`` entry, stored as plain JSON."""


class _ValueSchemaGenerator(GenerateJsonSchema):
    """
    Pydantic's stock JSON-schema generation plus OpenAPI's fixed-width numeric formats.

    A foreign runtime decodes numbers into machine types, which the bare
    ``integer``/``number`` type names cannot convey; ``format`` is an annotation per
    JSON schema, so runtimes that don't know these names simply skip them.
    """

    def int_schema(self, schema):
        return {**super().int_schema(schema), "format": "int64"}

    def float_schema(self, schema):
        return {**super().float_schema(schema), "format": "double"}


# Most-derived first: datetime subclasses date, so it must be matched before date.
_TEMPORAL_BASES = (datetime.datetime, datetime.date, datetime.time, datetime.timedelta)


def _normalize_temporal_annotation(annotation: Any) -> Any:
    """
    Map temporal subclasses (e.g. ``pendulum.DateTime``) to their stdlib base.

    Applied recursively through unions and containers, and only as a retry when direct
    schema generation fails, so temporal types carrying their own pydantic schema keep it.
    """
    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)
    if origin is not None and args:
        normalized = tuple(_normalize_temporal_annotation(arg) for arg in args)
        if normalized == args:
            return annotation
        if origin in (typing.Union, types.UnionType):
            return typing.Union[normalized]  # noqa: UP007 -- runtime construction from a tuple
        return origin[normalized]
    if isinstance(annotation, type):
        return next((base for base in _TEMPORAL_BASES if issubclass(annotation, base)), annotation)
    return annotation


def _infer_value_schema(annotation: Any, task_id: str, name: str) -> dict[str, Any] | None:
    """
    Build the JSON-schema fragment for one stub parameter annotation, via pydantic.

    The pydantic-generated schema ships verbatim, so runtimes must treat it as
    open-vocabulary JSON schema. Returns ``None`` when the annotation constrains nothing
    (missing, ``Any``, bare ``None``) or pydantic cannot generate a schema for it; the
    binding then omits ``value_schema`` and the foreign runtime falls back to a
    decode-only check.

    :raises ValueError: if the annotation uses ``typing.TypedDict``, which pydantic reads only on
        Python 3.12+.
    """
    if annotation is Parameter.empty or annotation is None or annotation is Any:
        return None
    if annotation is type(None):
        # get_type_hints normalizes a bare ``None`` annotation to NoneType; a parameter
        # that can only ever be None constrains nothing worth shipping.
        return None
    try:
        try:
            schema = _generate_value_schema(annotation)
        except TypeError:
            # Unhashable annotations cannot key the cache; generate directly. Any other pydantic
            # failure inside the body degrades to None there.
            schema = _generate_value_schema.__wrapped__(annotation)
    except PydanticUserError as e:
        raise ValueError(
            f"@task.stub task {task_id!r} parameter {name!r} is annotated with typing.TypedDict, "
            "which has no value schema before Python 3.12; use typing_extensions.TypedDict instead"
        ) from e
    # Deep-copy so callers embedding the fragment never alias the cached dict.
    return copy.deepcopy(schema) if schema else None


@cache
def _generate_value_schema(annotation: Any) -> dict[str, Any] | None:
    """
    Generate the schema for one annotation, cached for the process lifetime.

    TypeAdapter construction is one of pydantic's most expensive operations and
    annotations are static, so re-serializations of the same Dag must not re-pay it.
    """
    # PydanticUserError/TypeError cover annotations pydantic can't schema; either way,
    # that degrades to no schema rather than failing Dag serialization.
    try:
        return TypeAdapter(annotation).json_schema(schema_generator=_ValueSchemaGenerator)
    except (PydanticUserError, TypeError) as e:
        if isinstance(e, PydanticUserError) and e.code == "typed-dict-version":
            raise
        normalized = _normalize_temporal_annotation(annotation)
        if normalized is annotation:
            return None
        try:
            return TypeAdapter(normalized).json_schema(schema_generator=_ValueSchemaGenerator)
        except (PydanticUserError, TypeError):
            return None


def _validate_stub_signature(sig: Signature, task_id: str) -> None:
    for param in sig.parameters.values():
        if param.kind in (Parameter.VAR_POSITIONAL, Parameter.VAR_KEYWORD):
            raise ValueError(
                f"@task.stub task {task_id!r} must declare a fixed number of parameters for the "
                f"foreign runtime to bind against; *{param.name} is not supported"
            )
        if param.name in KNOWN_CONTEXT_KEYS:
            raise ValueError(
                f"@task.stub task {task_id!r} parameter {param.name!r} is an Airflow context key; "
                "stub signatures declare only data parameters -- the lang-SDK runtime injects its "
                "own task context natively (e.g. the Go SDK's sdk.TIRunContext parameter)"
            )


def _resolve_param_annotations(python_callable: Any, sig: Signature) -> dict[str, Any]:
    """Map each parameter to its serialization-time-resolvable annotation (``Parameter.empty`` when not)."""
    try:
        hints = typing.get_type_hints(python_callable)
    except (NameError, TypeError):
        # Annotations that cannot be resolved (e.g. names behind TYPE_CHECKING with
        # ``from __future__ import annotations``) degrade to "any".
        hints = {}

    def _resolve(name: str, param: Parameter) -> Any:
        if name in hints:
            return hints[name]
        if isinstance(param.annotation, str):
            return Parameter.empty
        return param.annotation

    return {name: _resolve(name, param) for name, param in sig.parameters.items()}


def _ensure_json_literal(value: Any, task_id: str, name: str) -> None:
    if next(XComArg.iter_xcom_references(value), None) is not None:
        raise ValueError(
            f"@task.stub task {task_id!r} parameter {name!r} received a collection with an "
            "upstream task output nested inside it; only a direct XComArg argument can cross "
            "the language boundary -- pass the upstream output as its own argument"
        )
    try:
        json.dumps(value, allow_nan=False)
    except (TypeError, ValueError):
        raise ValueError(
            f"@task.stub task {task_id!r} parameter {name!r} received a literal of type "
            f"{type(value).__name__} that is not JSON-serializable, so it cannot be passed "
            "to the foreign runtime; pass it in its JSON form instead"
        )


def _validate_xcom_value(value: Any, task_id: str, name: str) -> bool:
    """Validate an XComArg argument, returning True when *value* is one."""
    if not isinstance(value, XComArg):
        return False
    for operator, key in value.iter_references():
        if key != XCOM_RETURN_KEY:
            raise ValueError(
                f"@task.stub task {task_id!r} parameter {name!r} references the XCom key "
                f"{key!r} of task {operator.task_id!r}; only an upstream task's return value can "
                "cross the language boundary -- indexing an output by a custom key is not supported"
            )
    return True


def _validate_value(value: Any, task_id: str, name: str) -> None:
    if not _validate_xcom_value(value, task_id, name):
        _ensure_json_literal(value, task_id, name)


def _build_xcom_binding(value: XComArg, name: str) -> XComBindingEntry | LocalBindingEntry:
    """Bind the whole output of *value*; for a task with per-map-index arguments, the server re-derives it."""
    if not isinstance(value, PlainXComArg):
        # Only the worker can apply map() callables or assemble a zip()/concat() result.
        return {"name": name, "kind": "local"}
    if value.operator.get_needs_expansion():
        return {"name": name, "kind": "sequence", "task_id": value.operator.task_id}
    return {"name": name, "kind": "xcom", "task_id": value.operator.task_id}


def _build_group_arg_binding(value: MappedArgument, op: DecoratedOperator, name: str) -> ParamBindingEntry:
    group = next(g for g in op.iter_mapped_task_groups() if g._expand_input is value._input)
    # Only the root task group has no id, and it is never mapped.
    group_id = cast("str", group.group_id)
    return {"name": name, "kind": "param", "group_arg": {"group_id": group_id, "key": value._key}}


def build_arg_bindings(op: DecoratedOperator) -> list[ArgBindingEntry] | None:
    """
    Bind the TaskFlow call arguments to the stub signature and build the ordered arg spec.

    The caller owns the precondition: ``OperatorSerialization._serialize_node`` calls this only for
    a non-mapped ``DecoratedOperator`` flagged ``is_stub``, and nothing here re-checks it.

    Each entry is ``xcom`` for an unmapped upstream's output, ``sequence`` for a mapped upstream's
    whole output, ``local`` for a ``map()``/``zip()``/``concat()`` result, ``param`` for an expand
    argument of an enclosing mapped task group, and ``literal`` for everything else. ``name`` is
    always the stub function's parameter name, so a foreign runtime can bind by name in addition to
    the positional order.

    Returns ``None`` for argless calls: the binding contract (including the signature checks
    below) applies only once a TaskFlow call actually passes arguments, so pre-TaskFlow stub
    Dags whose call arguments were always ignored keep serializing.
    """
    python_callable = op.python_callable
    op_args = op.op_args
    op_kwargs = op.op_kwargs
    task_id = op.task_id

    if not op_args and not op_kwargs:
        return None

    op_signature = signature(python_callable)
    _validate_stub_signature(op_signature, task_id)

    bound = op_signature.bind(*op_args, **op_kwargs)
    explicitly_bound = set(bound.arguments)
    bound.apply_defaults()

    annotations = _resolve_param_annotations(python_callable, op_signature)

    spec: list[ArgBindingEntry] = []
    for name in op_signature.parameters:
        value = bound.arguments[name]
        entry: ArgBindingEntry
        if isinstance(value, MappedArgument):
            entry = _build_group_arg_binding(value, op, name)
        elif _validate_xcom_value(value, task_id, name):
            entry = _build_xcom_binding(value, name)
        else:
            _ensure_json_literal(value, task_id, name)
            entry = {"name": name, "kind": "literal", "value": value}
            if name not in explicitly_bound:
                entry["from_default"] = True
        if (value_schema := _infer_value_schema(annotations[name], task_id, name)) is not None:
            # Key omission (never ``None``) is the wire contract for "unconstrained":
            # ti_run responds with ``exclude_unset``, so an absent key stays absent.
            entry["value_schema"] = value_schema
        spec.append(entry)
    return spec


def _build_param_binding(name: str, param: Parameter, annotation: Any, task_id: str) -> ParamBindingEntry:
    entry: ParamBindingEntry = {"name": name, "kind": "param"}
    if (value_schema := _infer_value_schema(annotation, task_id, name)) is not None:
        entry["value_schema"] = value_schema
    if param.default is not Parameter.empty:
        _ensure_json_literal(param.default, task_id, name)
        entry["default"] = param.default
    return entry


def build_mapped_arg_bindings(op: DecoratedMappedOperator) -> list[ArgBindingEntry] | None:
    """
    Build the ordered ``param`` bindings of a mapped (``.expand()``/``.expand_kwargs()``) stub task.

    The caller owns the precondition: ``OperatorSerialization.serialize_mapped_operator`` calls
    this only for a decorated mapped operator flagged ``is_stub``, and nothing here re-checks it.

    Per-map-index values only resolve at run time, so unlike ``build_arg_bindings`` this captures
    what the server-side derivation cannot recover from the serialized Dag alone: the declaration
    order the wire contract promises, each parameter's default, and its value schema. Returns
    ``None`` for parameterless stubs (the legacy fan-out shape whose call args were always ignored
    keeps serializing).
    """
    python_callable = op.python_callable
    task_id = op.task_id
    partial_op_kwargs = op.partial_kwargs.get("op_kwargs") or {}
    expand_value = op._get_specified_expand_input().value

    op_signature = signature(python_callable)
    if not op_signature.parameters:
        return None
    _validate_stub_signature(op_signature, task_id)

    if isinstance(expand_value, Mapping):
        call_kwargs = {**partial_op_kwargs, **expand_value}
    else:
        # expand_kwargs(): the per-index mappings bind at run time, so only check the ones written
        # in the Dag file.
        call_kwargs = partial_op_kwargs
        if not isinstance(expand_value, XComArg):
            for mapping in expand_value:
                if isinstance(mapping, XComArg):
                    _validate_xcom_value(mapping, task_id, "expand_kwargs()")
                    continue
                if unknown := set(mapping).difference(op_signature.parameters):
                    raise ValueError(
                        f"@task.stub task {task_id!r} expand_kwargs() mapping passes unknown "
                        f"parameters {sorted(unknown)}"
                    )
                for name, value in mapping.items():
                    _validate_value(value, task_id, name)
        else:
            _validate_xcom_value(expand_value, task_id, "expand_kwargs()")
    try:
        bound = op_signature.bind_partial(**call_kwargs)
    except TypeError as e:
        raise ValueError(f"@task.stub task {task_id!r} TaskFlow mapping does not bind to its signature: {e}")
    if isinstance(expand_value, Mapping):
        bound.apply_defaults()
        if missing := set(op_signature.parameters).difference(bound.arguments):
            raise ValueError(
                f"@task.stub task {task_id!r} TaskFlow mapping does not bind to its signature: "
                f"missing parameters {sorted(missing)}"
            )

    annotations = _resolve_param_annotations(python_callable, op_signature)
    spec: list[ArgBindingEntry] = []
    for name, param in op_signature.parameters.items():
        if name in call_kwargs:
            _validate_value(call_kwargs[name], task_id, name)
        spec.append(_build_param_binding(name, param, annotations[name], task_id))
    return spec
