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

Any non-mapped operator flagged ``is_stub`` (currently only ``@task.stub``'s ``_StubOperator``)
gets its ordered positional-argument spec built here, from the live operator's already-bound
``python_callable``/``op_args``/``op_kwargs`` -- called from
``OperatorSerialization._serialize_node`` the same way ``is_stub`` itself is derived, so no
provider needs to duplicate this against the execution API's ``TaskArgBinding`` schema.
"""

from __future__ import annotations

import copy
import datetime
import json
import types
import typing
from functools import cache
from inspect import Parameter, Signature, signature
from typing import TYPE_CHECKING, Any, NamedTuple

from pydantic import PydanticUserError, TypeAdapter
from pydantic.json_schema import GenerateJsonSchema

from airflow._shared.timezones.timezone import coerce_datetime
from airflow.models.xcom import XCOM_RETURN_KEY
from airflow.sdk import XComArg
from airflow.sdk.definitions.context import KNOWN_CONTEXT_KEYS
from airflow.sdk.definitions.mappedoperator import MappedOperator
from airflow.sdk.definitions.xcom_arg import PlainXComArg

if TYPE_CHECKING:
    from airflow.sdk.bases.decorator import DecoratedOperator


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
    # Parametrized generics must be detected before the plain-class branch: on Python
    # 3.10, isinstance(list[X], type) is True and issubclass silently consults the
    # origin, so the class branch would return list[X] unnormalized.
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


def _infer_value_schema(annotation: Any) -> dict[str, Any] | None:
    """
    Build the JSON-schema fragment for one stub parameter annotation, via pydantic.

    The pydantic-generated schema ships verbatim, so runtimes must treat it as
    open-vocabulary JSON schema. Returns ``None`` when the annotation constrains nothing
    (missing, ``Any``, bare ``None``) or pydantic cannot generate a schema for it; the
    binding then omits ``value_schema`` and the foreign runtime falls back to a
    decode-only check.
    """
    if annotation is Parameter.empty or annotation is None or annotation is Any:
        return None
    if annotation is type(None):
        # get_type_hints normalizes a bare ``None`` annotation to NoneType; a parameter
        # that can only ever be None constrains nothing worth shipping.
        return None
    wire_form = _get_wire_form(annotation)
    # Deep-copy so callers embedding the fragment never alias the cached dict.
    return copy.deepcopy(wire_form.schema) if wire_form else None


class _ValueWireForm(NamedTuple):
    """The schema describing an annotation's JSON form, and the adapter that renders values into it."""

    adapter: TypeAdapter
    schema: dict[str, Any]


def _get_wire_form(annotation: Any) -> _ValueWireForm | None:
    try:
        return _build_wire_form(annotation)
    except TypeError:
        # Unhashable annotations cannot key the cache; build directly. Any pydantic
        # failure inside the body degrades to None there, so this retry never re-raises.
        return _build_wire_form.__wrapped__(annotation)


@cache
def _build_wire_form(annotation: Any) -> _ValueWireForm | None:
    """
    Build the adapter and schema for one annotation together, cached for the process lifetime.

    TypeAdapter construction is one of pydantic's most expensive operations and
    annotations are static, so re-serializations of the same Dag must not re-pay it.

    Pairing them is what keeps a literal from being rendered in a spelling its own
    ``value_schema`` does not describe: both come from the same adapter, including when
    the temporal-normalization retry below settles on a different annotation.
    """
    # PydanticUserError/TypeError cover annotations pydantic can't schema; either way,
    # that degrades to no schema rather than failing Dag serialization.
    for candidate in (annotation, _normalize_temporal_annotation(annotation)):
        try:
            adapter = TypeAdapter(candidate)
            return _ValueWireForm(adapter, adapter.json_schema(schema_generator=_ValueSchemaGenerator))
        except (PydanticUserError, TypeError):
            continue
    return None


def _to_json_value(value: Any, annotation: Any) -> Any:
    """
    Render a native value in the JSON form its ``value_schema`` advertises.

    A ``datetime``/``timedelta``/``UUID`` is not JSON-serializable, so without this it
    could not cross the language boundary at all. Dumping it through the same adapter
    that produced the schema gives every lang SDK exactly one spelling per format --
    RFC 3339 timestamps, ISO-8601 durations, canonical UUIDs -- instead of each Dag
    author picking their own.

    Values pydantic cannot render for this annotation pass through untouched, leaving
    the JSON-serializability check to reject them.
    """
    if annotation is Parameter.empty or annotation is None or annotation is Any:
        return value
    if isinstance(value, datetime.datetime):
        # A naive timestamp is ambiguous once it leaves Python: Go would read it as UTC,
        # JavaScript as the worker's local time, and Java would refuse to parse it. Pin
        # the offset here, using the same default timezone the rest of Airflow applies.
        value = coerce_datetime(value)
    wire_form = _get_wire_form(annotation)
    if wire_form is None:
        return value
    # warnings=False: a value that does not match its annotation is the JSON-literal
    # check's business, not a serializer warning's.
    return wire_form.adapter.dump_python(value, mode="json", warnings=False)


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


def _reject_nested_xcom(value: Any, task_id: str, name: str) -> None:
    if next(XComArg.iter_xcom_references(value), None) is not None:
        raise ValueError(
            f"@task.stub task {task_id!r} parameter {name!r} received a collection with an "
            "upstream task output nested inside it; only a direct XComArg argument can cross "
            "the language boundary -- pass the upstream output as its own argument"
        )


def _ensure_json_literal(value: Any, task_id: str, name: str) -> None:
    try:
        json.dumps(value, allow_nan=False)
    except (TypeError, ValueError):
        raise ValueError(
            f"@task.stub task {task_id!r} parameter {name!r} received a literal of type "
            f"{type(value).__name__} that is not JSON-serializable, so it cannot be passed "
            "to the foreign runtime; annotate the stub parameter with the value's type so it "
            "can be serialized, or pass it in its JSON form instead"
        )


def _validate_xcom_value(value: Any, task_id: str, name: str) -> bool:
    """Validate an XComArg argument, returning True when it is a bindable direct upstream output."""
    if isinstance(value, PlainXComArg):
        if value.key != XCOM_RETURN_KEY:
            raise ValueError(
                f"@task.stub task {task_id!r} parameter {name!r} references the XCom key "
                f"{value.key!r}; only an upstream task's return value can cross the language "
                "boundary -- indexing an output by a custom key is not supported"
            )
        # isinstance, not .is_mapped: Airflow 2.11 operators have no is_mapped attribute.
        if isinstance(value.operator, MappedOperator):
            raise ValueError(
                f"@task.stub task {task_id!r} parameter {name!r} references the aggregated "
                f"output of the mapped task {value.operator.task_id!r}; a foreign runtime "
                "pulls single XCom rows, so a mapped upstream's combined output is not "
                "supported"
            )
        return True
    if isinstance(value, XComArg):
        raise ValueError(
            f"@task.stub task {task_id!r} parameter {name!r} received a "
            f"{type(value).__name__}; only direct upstream task outputs can cross the "
            "language boundary -- .map()/.zip()/.concat() results are not supported"
        )
    return False


def build_arg_bindings(op: DecoratedOperator) -> list[dict[str, Any]] | None:
    """
    Bind the TaskFlow call arguments to the stub signature and build the ordered arg spec.

    The caller owns the precondition: ``OperatorSerialization._serialize_node`` calls this only for
    a non-mapped ``DecoratedOperator`` flagged ``is_stub``, and nothing here re-checks it.

    Each spec entry is a plain dict matching one variant of the execution API's
    ``TaskArgBinding`` union: an ``XComArgBinding`` (``kind="xcom"``) for upstream TaskFlow
    outputs, or a ``LiteralArgBinding`` (``kind="literal"``) for everything else. ``name`` is
    always the stub function's parameter name, so a foreign runtime can bind by name in
    addition to the existing positional order.

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

    # Direct .expand() on the stub needs no spec here (ti_run derives per-map-index
    # bindings from the serialized expand input), but a mapped task group creates
    # per-map-index instances of the tasks inside it with no expand input of their own,
    # so their arg values are unresolvable both here and server-side.
    if op.get_closest_mapped_task_group() is not None:
        raise ValueError(
            f"@task.stub task {task_id!r} passes TaskFlow call arguments inside a mapped "
            "task group; the captured spec cannot carry values that resolve per map index at "
            "runtime, so stub tasks with arguments are not supported under a task group's "
            ".expand()"
        )

    op_signature = signature(python_callable)
    _validate_stub_signature(op_signature, task_id)

    bound = op_signature.bind(*op_args, **op_kwargs)
    explicitly_bound = set(bound.arguments)
    bound.apply_defaults()

    annotations = _resolve_param_annotations(python_callable, op_signature)

    spec: list[dict[str, Any]] = []
    for name in op_signature.parameters:
        value = bound.arguments[name]
        value_schema = _infer_value_schema(annotations[name])
        if _validate_xcom_value(value, task_id, name):
            xcom_entry: dict[str, Any] = {"name": name, "kind": "xcom", "task_id": value.operator.task_id}
            if value_schema is not None:
                xcom_entry["value_schema"] = value_schema
            spec.append(xcom_entry)
            continue
        _reject_nested_xcom(value, task_id, name)
        value = _to_json_value(value, annotations[name])
        _ensure_json_literal(value, task_id, name)
        entry: dict[str, Any] = {"name": name, "kind": "literal", "value": value}
        if value_schema is not None:
            # Key omission (never ``None``) is the wire contract for "unconstrained":
            # ti_run responds with ``exclude_unset``, so an absent key stays absent.
            entry["value_schema"] = value_schema
        if name not in explicitly_bound:
            entry["from_default"] = True
        spec.append(entry)
    return spec
