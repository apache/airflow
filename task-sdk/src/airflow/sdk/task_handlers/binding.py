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
Runtime argument binder.

At execution time, a stub task's inputs arrive as ``TIRunContext.arg_bindings``.
This converts it into keyword arguments, and uses them to call the registered
task handler.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypedDict

from airflow.sdk.bases.xcom import BaseXCom

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from typing import Literal, NotRequired, TypeAlias

    XComResolver = Callable[[str, str], Any]

__all__ = ["ArgBinding", "call_handler"]


class LiteralArgBinding(TypedDict):
    """Literal argument representation."""

    name: str
    kind: Literal["literal"]
    value: NotRequired[Any]


class XComArgBinding(TypedDict):
    """XCom reference argument representation."""

    name: str
    kind: Literal["xcom"]
    task_id: str


ArgBinding: TypeAlias = "LiteralArgBinding | XComArgBinding"


def _bind_arguments(
    arg_bindings: list[ArgBinding] | None,
    xcom_resolver: XComResolver,
    *,
    skip_defaults: bool = True,
) -> Iterator[tuple[str, Any]]:
    """
    Resolve an ordered arg-binding spec into handler keyword arguments.

    * a ``LiteralArgBinding`` -> its inline ``value``.
    * an ``XComArgBinding``    -> ``xcom_resolver(task_id, "return_value")``.

    ``skip_defaults``: when true (default), literal entries flagged ``from_default`` are omitted
    so the handler's own default applies -- matching ADR-0007's "defaults are explicit" so a
    keyword consumer can leave them unclaimed. Set false to bind them explicitly.

    ``None`` (an argless call) yields ``{}``.
    """
    for i, binding in enumerate(arg_bindings or ()):
        if not (name := binding.get("name")):
            raise ValueError(f"arg_bindings[{i}]: missing 'name'")
        match binding:
            case {"kind": "literal", "from_default": True} if skip_defaults:
                continue
            case {"kind": "literal"}:
                yield name, binding.get("value")
            case {"kind": "xcom", "task_id": str(task_id)} if task_id:
                yield name, xcom_resolver(task_id, BaseXCom.XCOM_RETURN_KEY)
            case {"kind": "xcom"}:
                raise ValueError(f"arg_bindings[{i}] ({name!r}): xcom binding missing 'task_id'")
            case {"kind": kind}:
                raise ValueError(f"arg_bindings[{i}] ({name!r}): unknown kind {kind!r}")
            case _:
                raise ValueError(f"arg_bindings[{i}] ({name!r}): missing 'kind'")


def call_handler(
    fn: Callable[..., Any],
    arg_bindings: list[ArgBinding] | None,
    xcom_resolver: XComResolver,
    *,
    skip_defaults: bool = True,
) -> Any:
    """Bind ``arg_bindings`` and invoke ``fn``, returning its result (the task's return_value XCom)."""
    return fn(**dict(_bind_arguments(arg_bindings, xcom_resolver, skip_defaults=skip_defaults)))
