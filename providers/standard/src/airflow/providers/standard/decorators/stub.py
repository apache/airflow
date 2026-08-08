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

from __future__ import annotations

import ast
import copy
import datetime
import inspect
import json
import types
import typing
from collections.abc import Callable, Collection, Mapping
from functools import cache
from typing import TYPE_CHECKING, Any

try:
    from pydantic import PydanticUserError, TypeAdapter
    from pydantic.json_schema import GenerateJsonSchema
except ImportError:
    # Airflow 3 always ships pydantic but Airflow 2.x base installs do not; without it,
    # stub args carry no value schemas and runtimes keep their decode-only fallback.
    GenerateJsonSchema = object  # type: ignore[assignment,misc]
    TypeAdapter = None  # type: ignore[assignment,misc]
    PydanticUserError = None  # type: ignore[assignment,misc]

from airflow.providers.common.compat.sdk import (
    KNOWN_CONTEXT_KEYS,
    XCOM_RETURN_KEY,
    DecoratedOperator,
    MappedOperator,
    PlainXComArg,
    TaskDecorator,
    XComArg,
    task_decorator_factory,
)

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


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
    if TypeAdapter is None:
        return None
    if annotation is inspect.Parameter.empty or annotation is None or annotation is Any:
        return None
    if annotation is type(None):
        # get_type_hints normalizes a bare ``None`` annotation to NoneType; a parameter
        # that can only ever be None constrains nothing worth shipping.
        return None
    try:
        schema = _generate_value_schema(annotation)
    except TypeError:
        # Unhashable annotations cannot key the cache; generate directly. Any pydantic
        # failure inside the body degrades to None there, so this retry never re-raises.
        schema = _generate_value_schema.__wrapped__(annotation)
    # Deep-copy so callers embedding the fragment never alias the cached dict.
    return copy.deepcopy(schema) if schema else None


@cache
def _generate_value_schema(annotation: Any) -> dict[str, Any] | None:
    """
    Generate the schema for one annotation, cached for the process lifetime.

    TypeAdapter construction is one of pydantic's most expensive operations and
    annotations are static, so re-parses of the same Dag file must not re-pay it.
    """
    # Reached only when pydantic is installed (``_infer_value_schema`` guards on
    # ``TypeAdapter is None``), so ``PydanticUserError`` is a real exception class here.
    # It is the base of PydanticSchemaGenerationError and PydanticInvalidForJsonSchema and
    # covers annotations pydantic rejects outright (e.g. bare ClassVar); TypeError catches
    # the exotic generics pydantic chokes on with a plain TypeError. Either way, "pydantic
    # cannot schema this" degrades to no schema rather than failing Dag parsing.
    try:
        return TypeAdapter(annotation).json_schema(schema_generator=_ValueSchemaGenerator)
    except (PydanticUserError, TypeError):
        normalized = _normalize_temporal_annotation(annotation)
        if normalized is annotation:
            return None
        try:
            return TypeAdapter(normalized).json_schema(schema_generator=_ValueSchemaGenerator)
        except (PydanticUserError, TypeError):
            return None


def _validate_stub_signature(signature: inspect.Signature, task_id: str) -> None:
    for param in signature.parameters.values():
        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
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


def _resolve_param_annotations(python_callable: Callable, signature: inspect.Signature) -> dict[str, Any]:
    """Map each parameter to its parse-time-resolvable annotation (``Parameter.empty`` when not)."""
    try:
        hints = typing.get_type_hints(python_callable)
    except (NameError, TypeError):
        # Annotations that cannot be resolved at parse time (e.g. names behind
        # TYPE_CHECKING with ``from __future__ import annotations``) degrade to "any".
        hints = {}

    def resolve(name: str, param: inspect.Parameter) -> Any:
        if name in hints:
            return hints[name]
        if isinstance(param.annotation, str):
            return inspect.Parameter.empty
        return param.annotation

    return {name: resolve(name, param) for name, param in signature.parameters.items()}


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


def _build_arg_bindings(
    python_callable: Callable,
    op_args: Collection[Any],
    op_kwargs: Mapping[str, Any],
    task_id: str,
    *,
    in_mapped_group: bool,
) -> list[dict[str, Any]] | None:
    """
    Bind the TaskFlow call arguments to the stub signature and build the ordered arg spec.

    Each spec entry is a plain dict matching one variant of the execution API's
    ``TaskArgBinding`` union: an ``XComArgBinding`` (``kind="xcom"``) for upstream TaskFlow
    outputs, or a ``LiteralArgBinding`` (``kind="literal"``) for everything else. ``name`` is
    always the stub function's parameter name, so a foreign runtime can bind by name (e.g. the
    Go SDK's ``sdk.TaskInput`` struct fields) in addition to the existing positional order.
    Returns ``None`` for argless calls: the binding contract (including the signature checks
    below) applies only once a TaskFlow call actually passes arguments, so pre-TaskFlow stub
    Dags whose call arguments were always ignored keep parsing.
    """
    if not op_args and not op_kwargs:
        return None

    # Direct .expand() on the stub needs no parse-time spec (ti_run derives per-map-index
    # bindings from the serialized expand input), but a mapped task group creates
    # per-map-index instances of the tasks inside it with no expand input of their own,
    # so their arg values are unresolvable both here and server-side.
    if in_mapped_group:
        raise ValueError(
            f"@task.stub task {task_id!r} passes TaskFlow call arguments inside a mapped "
            "task group; the captured spec cannot carry values that resolve per map index at "
            "runtime, so stub tasks with arguments are not supported under a task group's "
            ".expand()"
        )

    signature = inspect.signature(python_callable)
    _validate_stub_signature(signature, task_id)

    bound = signature.bind(*op_args, **op_kwargs)
    explicitly_bound = set(bound.arguments)
    bound.apply_defaults()

    annotations = _resolve_param_annotations(python_callable, signature)

    spec: list[dict[str, Any]] = []
    for name in signature.parameters:
        value = bound.arguments[name]
        value_schema = _infer_value_schema(annotations[name])
        if _validate_xcom_value(value, task_id, name):
            xcom_entry: dict[str, Any] = {"name": name, "kind": "xcom", "task_id": value.operator.task_id}
            if value_schema is not None:
                xcom_entry["value_schema"] = value_schema
            spec.append(xcom_entry)
            continue
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


class _StubOperator(DecoratedOperator):
    custom_operator_name: str = "@task.stub"

    def __init__(
        self,
        *,
        python_callable: Callable,
        task_id: str,
        **kwargs,
    ) -> None:
        super().__init__(
            python_callable=python_callable,
            task_id=task_id,
            **kwargs,
        )
        # A retry_policy is user Python evaluated in-process by the task runner. Stub tasks
        # execute on a remote/native worker via the Task Execution Interface and never run the
        # Python task runner, so the policy would silently never fire. Reject it up front.
        # (retries is fine -- the server computes retry eligibility regardless of runtime.)
        if getattr(self, "retry_policy", None) is not None:
            raise ValueError(
                "@task.stub does not support `retry_policy`: it runs Python in-process, but stub "
                "tasks execute on a lang-sdk runtime and never evaluate the policy. Use `retries` "
                "instead."
            )
        # Validate python callable
        module = ast.parse(self.get_python_source())

        if len(module.body) != 1:
            raise ValueError("Expected a single statement")
        fn = module.body[0]
        if not isinstance(fn, ast.FunctionDef):
            raise ValueError("Expected a single sync function")
        for stmt in fn.body:
            if isinstance(stmt, ast.Pass):
                continue
            if isinstance(stmt, ast.Expr):
                if isinstance(stmt.value, ast.Constant) and isinstance(stmt.value.value, (str, type(...))):
                    continue

            raise ValueError(
                f"Functions passed to @task.stub must be an empty function (`pass`, or `...` only) (got {stmt})"
            )

        # Bind the TaskFlow call to the *original* signature (DecoratedOperator mangles context
        # key defaults, which stubs reject anyway) and persist the ordered arg spec so the
        # execution API can hand it to the foreign runtime via StartupDetails.
        self._arg_bindings = _build_arg_bindings(
            python_callable,
            self.op_args,
            self.op_kwargs,
            self.task_id,
            in_mapped_group=self.get_closest_mapped_task_group() is not None,
        )

    @classmethod
    def get_serialized_fields(cls):
        # _arg_bindings must round-trip back to plain JSON (not {__type, __var}-encoded) so the
        # execution API can validate it straight off the serialized Dag: it deserializes fully
        # only while it stays out of SerializedBaseOperator's static serialized-field set.
        return super().get_serialized_fields() | {"_arg_bindings"}

    def execute(self, context: Context) -> Any:
        raise RuntimeError(
            "@task.stub should not be executed directly -- we expected this to go to a remote worker. "
            "Check your pool and worker configs"
        )


def stub(
    python_callable: Callable | None = None,
    queue: str | None = None,
    executor: str | None = None,
    **kwargs,
) -> TaskDecorator:
    """
    Define a stub task in the DAG.

    Stub tasks exist in the Dag graph only, but the execution must happen in an external
    environment via the Task Execution Interface.

    Stub functions may declare parameters and be called TaskFlow-style with upstream task
    outputs or JSON-serializable literals; the resulting argument-binding spec (parameter
    names, value schemas, and values, in declaration order) is delivered to the foreign
    runtime, which binds the values onto the native task function.

    Mapped (``.expand()``) stubs do not receive TaskFlow arguments yet -- their call args
    keep the legacy ignored behavior; per-map-index delivery is part of
    https://github.com/apache/airflow/issues/66937 and lands in a follow-up.
    """
    return task_decorator_factory(
        decorated_operator_class=_StubOperator,
        python_callable=python_callable,
        queue=queue,
        executor=executor,
        **kwargs,
    )
