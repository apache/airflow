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
import datetime
import inspect
import json
import types
import typing
from collections.abc import Callable, Collection, Mapping
from typing import TYPE_CHECKING, Any

try:
    from pydantic import PydanticInvalidForJsonSchema, PydanticSchemaGenerationError, TypeAdapter
    from pydantic.json_schema import GenerateJsonSchema
except ImportError:
    # Airflow 3 always ships pydantic but Airflow 2.x base installs do not; without it,
    # stub args carry no value schemas and runtimes keep their decode-only fallback.
    GenerateJsonSchema = object  # type: ignore[assignment,misc]
    TypeAdapter = None  # type: ignore[assignment,misc]
    PydanticInvalidForJsonSchema = PydanticSchemaGenerationError = None  # type: ignore[assignment,misc]

from airflow.providers.common.compat.sdk import (
    KNOWN_CONTEXT_KEYS,
    DecoratedOperator,
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
    if isinstance(annotation, type):
        return next((base for base in _TEMPORAL_BASES if issubclass(annotation, base)), annotation)
    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)
    if origin is None or not args:
        return annotation
    normalized = tuple(_normalize_temporal_annotation(arg) for arg in args)
    if normalized == args:
        return annotation
    if origin in (typing.Union, types.UnionType):
        return typing.Union[normalized]  # noqa: UP007 -- runtime construction from a tuple
    return origin[normalized]


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
        schema = TypeAdapter(annotation).json_schema(schema_generator=_ValueSchemaGenerator)
    except (PydanticSchemaGenerationError, PydanticInvalidForJsonSchema):
        normalized = _normalize_temporal_annotation(annotation)
        if normalized is annotation:
            return None
        try:
            schema = TypeAdapter(normalized).json_schema(schema_generator=_ValueSchemaGenerator)
        except (PydanticSchemaGenerationError, PydanticInvalidForJsonSchema):
            return None
    return schema or None


def _build_arg_bindings(
    python_callable: Callable,
    op_args: Collection[Any],
    op_kwargs: Mapping[str, Any],
    task_id: str,
) -> list[dict[str, Any]] | None:
    """
    Bind the TaskFlow call arguments to the stub signature and build the ordered arg spec.

    Each spec entry is a plain dict matching one variant of the execution API's
    ``TaskArgBinding`` union: an ``XComArgBinding`` (``kind="xcom"``) for upstream TaskFlow
    outputs, or a ``LiteralArgBinding`` (``kind="literal"``) for everything else. ``name`` is
    always the stub function's parameter name, so a foreign runtime can bind by name (e.g. the
    Go SDK's name-based struct fields) in addition to the existing positional order.
    Returns ``None`` for argless calls: the binding contract (including the signature checks
    below) applies only once a TaskFlow call actually passes arguments, so pre-TaskFlow stub
    Dags whose call arguments were always ignored keep parsing.
    """
    if not op_args and not op_kwargs:
        return None

    signature = inspect.signature(python_callable)

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

    bound = signature.bind(*op_args, **op_kwargs)
    explicitly_bound = set(bound.arguments)
    bound.apply_defaults()

    try:
        hints = typing.get_type_hints(python_callable)
    except (NameError, TypeError):
        # Annotations that cannot be resolved at parse time (e.g. names behind
        # TYPE_CHECKING with ``from __future__ import annotations``) degrade to "any".
        hints = {}

    def get_annotation_for(name: str, param: inspect.Parameter) -> Any:
        if name in hints:
            return hints[name]
        if isinstance(param.annotation, str):
            return inspect.Parameter.empty
        return param.annotation

    spec: list[dict[str, Any]] = []
    for name, param in signature.parameters.items():
        value = bound.arguments[name]
        value_schema = _infer_value_schema(get_annotation_for(name, param))
        if isinstance(value, PlainXComArg):
            if value.key != "return_value":
                raise ValueError(
                    f"@task.stub task {task_id!r} parameter {name!r} references the XCom key "
                    f"{value.key!r}; only an upstream task's return value can cross the language "
                    "boundary -- indexing an output by a custom key is not supported"
                )
            xcom_entry: dict[str, Any] = {"name": name, "kind": "xcom", "task_id": value.operator.task_id}
            if value_schema is not None:
                xcom_entry["value_schema"] = value_schema
            spec.append(xcom_entry)
            continue
        if isinstance(value, XComArg):
            raise ValueError(
                f"@task.stub task {task_id!r} parameter {name!r} received a "
                f"{type(value).__name__}; only direct upstream task outputs can cross the "
                "language boundary -- .map()/.zip()/.concat() results are not supported"
            )
        try:
            json.dumps(value, allow_nan=False)
        except (TypeError, ValueError):
            raise ValueError(
                f"@task.stub task {task_id!r} parameter {name!r} received a literal of type "
                f"{type(value).__name__} that is not JSON-serializable, so it cannot be passed "
                "to the foreign runtime"
            )
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
        self._arg_bindings = _build_arg_bindings(python_callable, self.op_args, self.op_kwargs, self.task_id)

        # Direct .expand() on the stub needs no parse-time spec (ti_run derives per-map-index
        # bindings from the serialized expand input), but a mapped task group creates
        # per-map-index instances of the tasks inside it with no expand input of their own,
        # so their arg values are unresolvable both here and server-side.
        in_mapped_group = getattr(self, "get_closest_mapped_task_group", lambda: None)() is not None
        if self._arg_bindings is not None and in_mapped_group:
            raise ValueError(
                f"@task.stub task {self.task_id!r} passes TaskFlow call arguments inside a mapped "
                "task group; the captured spec cannot carry values that resolve per map index at "
                "runtime, so stub tasks with arguments are not supported under a task group's "
                ".expand()"
            )

    @classmethod
    def get_serialized_fields(cls):
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
    """
    return task_decorator_factory(
        decorated_operator_class=_StubOperator,
        python_callable=python_callable,
        queue=queue,
        executor=executor,
        **kwargs,
    )
