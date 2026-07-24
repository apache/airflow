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
from collections.abc import Callable, Collection, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Union

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


def _json_schema_fragment(annotation: Any) -> dict[str, Any] | None:
    """Map one non-union annotation to a JSON-schema fragment; ``None`` = unclassifiable."""
    if annotation is type(None):
        return {"type": "null"}
    origin = typing.get_origin(annotation)
    if origin is not None:
        annotation = origin
    if not isinstance(annotation, type):
        return None
    # bool subclasses int, str/bytes are Sequences, and datetime subclasses date -- order matters.
    if issubclass(annotation, bool):
        return {"type": "boolean"}
    if issubclass(annotation, int):
        return {"type": "integer", "format": "int64"}
    if issubclass(annotation, float):
        return {"type": "number", "format": "double"}
    if issubclass(annotation, str):
        return {"type": "string"}
    if issubclass(annotation, bytes):
        return None
    if issubclass(annotation, datetime.datetime):
        return {"type": "string", "format": "date-time"}
    if issubclass(annotation, datetime.date):
        return {"type": "string", "format": "date"}
    if issubclass(annotation, datetime.time):
        return {"type": "string", "format": "time"}
    if issubclass(annotation, datetime.timedelta):
        return {"type": "string", "format": "duration"}
    if issubclass(annotation, (dict, Mapping)):
        return {"type": "object"}
    if issubclass(annotation, (list, tuple, set, frozenset, Sequence)):
        return {"type": "array"}
    return None


def _infer_value_schema(annotation: Any) -> dict[str, Any] | None:
    """
    Map a stub function parameter annotation to a JSON-schema fragment.

    Fragments carry the standard ``type`` keyword -- a single name, or a list for union
    annotations (set semantics; member order follows the annotation) -- plus a ``format``
    annotation where the Python type implies a wire representation the type name alone
    cannot (``int`` -> ``int64``, ``datetime`` -> ``date-time``, ``timedelta`` ->
    ``duration``, ...). Returns ``None`` when the annotation gives no constraint; the
    binding then omits ``value_schema`` and the foreign runtime falls back to a
    decode-only check.
    """
    if annotation is inspect.Parameter.empty or annotation is None or annotation is Any:
        return None
    if annotation is type(None):
        # get_type_hints normalizes a bare ``None`` annotation to NoneType; a parameter
        # that can only ever be None constrains nothing worth shipping.
        return None
    origin = typing.get_origin(annotation)
    if origin is Union or origin is types.UnionType:
        fragments: list[dict[str, Any]] = []
        for member in typing.get_args(annotation):
            fragment = _json_schema_fragment(member)
            if fragment is None:
                # One unclassifiable member makes the whole union unconstrained: a
                # partial schema would wrongly reject that member's values.
                return None
            if fragment not in fragments:
                fragments.append(fragment)
        data_fragments = [f for f in fragments if f["type"] != "null"]
        if len(data_fragments) == 1:
            # A single data type (+ optional null) keeps its format: format only
            # constrains values of its own type, so null passes it untouched.
            schema = dict(data_fragments[0])
            if len(fragments) > len(data_fragments):
                schema["type"] = [schema["type"], "null"]
            return schema
        # Mixed-type union: formats are per-member and inexpressible in one flat
        # fragment, so carry the type names only.
        type_names = [f["type"] for f in fragments]
        deduped = list(dict.fromkeys(type_names))
        return {"type": deduped if len(deduped) > 1 else deduped[0]}
    return _json_schema_fragment(annotation)


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
    Go SDK's ``sdk.TaskInput`` struct fields) in addition to the existing positional order.
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

    # A mapped stub's arg *types* are uniform across map indexes (same function), but its
    # arg *values* only resolve per map index at runtime, while the spec below is captured
    # at parse time and the wire contract has no mapped-binding kind yet -- so .expand()
    # is rejected rather than shipped with a wrong or empty spec. The task-sdk decorator
    # machinery rejects direct .expand() at parse time for operator classes that opt out
    # on Airflow >= 3.4 (older cores cannot enforce it, and never serialize a spec for the
    # mapped stub); stubs called with arguments inside a mapped task group are rejected in
    # __init__ below.
    supports_expand: bool = False

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

        # supports_expand only blocks direct .expand() on the stub itself; a mapped task
        # group still creates per-map-index instances of every task inside it, whose arg
        # values resolve per map index at runtime -- after this parse-time capture.
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
