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
import inspect
import json
import types
import typing
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Union

from airflow.providers.common.compat.sdk import (
    DecoratedOperator,
    TaskDecorator,
    task_decorator_factory,
)

try:
    from airflow.sdk.definitions.context import KNOWN_CONTEXT_KEYS
    from airflow.sdk.definitions.xcom_arg import PlainXComArg, XComArg
except ImportError:  # Airflow 2
    from airflow.models.xcom_arg import PlainXComArg, XComArg  # type: ignore[no-redef]
    from airflow.utils.context import KNOWN_CONTEXT_KEYS  # type: ignore[no-redef]

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


def _data_type_from_annotation(annotation: Any) -> str:
    """
    Map a stub function parameter annotation to the language-neutral arg-type vocabulary.

    The foreign runtime type-checks the bound value against the returned name; anything we
    cannot classify confidently maps to ``"any"`` so binding falls back to a decode-only check.
    """
    if annotation is inspect.Parameter.empty or annotation is None or annotation is Any:
        return "any"
    origin = typing.get_origin(annotation)
    if origin is not None:
        if origin is Union or origin is getattr(types, "UnionType", None):
            members = [a for a in typing.get_args(annotation) if a is not type(None)]
            if len(members) == 1:
                return _data_type_from_annotation(members[0])
            return "any"
        annotation = origin
    if not isinstance(annotation, type):
        return "any"
    # bool subclasses int, and str/bytes are Sequences -- order matters.
    if issubclass(annotation, bool):
        return "boolean"
    if issubclass(annotation, int):
        return "integer"
    if issubclass(annotation, float):
        return "number"
    if issubclass(annotation, str):
        return "string"
    if issubclass(annotation, bytes):
        return "any"
    if issubclass(annotation, (dict, Mapping)):
        return "object"
    if issubclass(annotation, (list, tuple, set, frozenset, Sequence)):
        return "array"
    return "any"


def _build_arg_bindings(
    python_callable: Callable,
    op_args: Sequence[Any],
    op_kwargs: Mapping[str, Any],
    task_id: str,
) -> list[dict[str, Any]] | None:
    """
    Bind the TaskFlow call arguments to the stub signature and build the ordered arg spec.

    Each spec entry is a plain dict matching the execution API ``TaskArgBinding`` shape: an XCom
    reference (``kind="xcom"``) for upstream TaskFlow outputs, or an inline value
    (``kind="literal"``) for everything else. Returns ``None`` for parameterless stubs.
    """
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
                "context injection does not happen in a foreign runtime, so pass the value "
                "explicitly under a different parameter name"
            )

    if not signature.parameters:
        return None

    bound = signature.bind(*op_args, **op_kwargs)
    bound.apply_defaults()

    try:
        hints = typing.get_type_hints(python_callable)
    except Exception:
        # Annotations that cannot be resolved at parse time (e.g. names behind
        # TYPE_CHECKING with ``from __future__ import annotations``) degrade to "any".
        hints = {}

    def annotation_for(name: str, param: inspect.Parameter) -> Any:
        if name in hints:
            return hints[name]
        if isinstance(param.annotation, str):
            return inspect.Parameter.empty
        return param.annotation

    spec: list[dict[str, Any]] = []
    for name, param in signature.parameters.items():
        value = bound.arguments[name]
        data_type = _data_type_from_annotation(annotation_for(name, param))
        if isinstance(value, PlainXComArg):
            spec.append(
                {
                    "kind": "xcom",
                    "data_type": data_type,
                    "task_id": value.operator.task_id,
                    "key": value.key,
                }
            )
            continue
        if isinstance(value, XComArg):
            raise ValueError(
                f"@task.stub task {task_id!r} parameter {name!r} received a "
                f"{type(value).__name__}; only direct upstream task outputs (optionally "
                "indexed by key) can cross the language boundary -- .map()/.zip()/.concat() "
                "results are not supported"
            )
        try:
            json.dumps(value)
        except (TypeError, ValueError):
            raise ValueError(
                f"@task.stub task {task_id!r} parameter {name!r} received a literal of type "
                f"{type(value).__name__} that is not JSON-serializable, so it cannot be passed "
                "to the foreign runtime"
            )
        spec.append({"kind": "literal", "data_type": data_type, "value": value})
    return spec


class _StubOperator(DecoratedOperator):
    custom_operator_name: str = "@task.stub"

    # Mapped stubs would need per-map-index arg specs, which the foreign runtime cannot
    # receive yet; the task-sdk decorator machinery rejects .expand() at parse time for
    # operator classes that opt out.
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
            raise RuntimeError("Expected a single statement")
        fn = module.body[0]
        if not isinstance(fn, ast.FunctionDef):
            raise RuntimeError("Expected a single sync function")
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
    outputs or JSON-serializable literals; the resulting positional-argument spec is delivered
    to the foreign runtime, which binds the values onto the native task function.
    """
    return task_decorator_factory(
        decorated_operator_class=_StubOperator,
        python_callable=python_callable,
        queue=queue,
        executor=executor,
        **kwargs,
    )
