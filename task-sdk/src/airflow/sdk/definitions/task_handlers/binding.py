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

from typing import TYPE_CHECKING, Any

from airflow.sdk.api.datamodels._generated import LiteralArgBinding, XComArgBinding
from airflow.sdk.bases.xcom import BaseXCom

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from airflow.sdk.api.datamodels._generated import TaskArgBinding

    XComResolver = Callable[[str, str], Any]

__all__ = ["call_handler"]


def _bind_arguments(
    arg_bindings: list[TaskArgBinding] | None,
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

    ``None`` (an argless call) yields nothing.

    :raises ValueError: if a binding needs the per-map-index resolution this binder does not do yet,
        checked for every binding before any XCom is pulled.
    """
    bindings = arg_bindings or []
    for binding in bindings:
        _check_bindable(binding)
    for binding in bindings:
        match binding.root:
            case LiteralArgBinding(from_default=True) if skip_defaults:
                continue
            case LiteralArgBinding(name=name, value=value):
                yield name, value
            case XComArgBinding(name=name, task_id=task_id):
                yield name, xcom_resolver(task_id, BaseXCom.XCOM_RETURN_KEY)


_PER_MAP_INDEX_FIELDS = ("map_index", "element_index", "apply_map", "kwarg", "fallback")


def _check_bindable(binding: TaskArgBinding) -> None:
    root = binding.root
    if not isinstance(root, LiteralArgBinding | XComArgBinding):
        raise _mapped_task_not_implemented(root.name, f"binding kind {root.kind!r}")
    for field in _PER_MAP_INDEX_FIELDS:
        if (value := getattr(root, field, None)) is not None and value is not False:
            raise _mapped_task_not_implemented(root.name, field)


def _mapped_task_not_implemented(name: str, what: str) -> NotImplementedError:
    return NotImplementedError(
        f"Task argument {name!r}: {what} needs mapped task support, which the stub TaskHandler "
        "does not implement yet"
    )


def call_handler(
    fn: Callable[..., Any],
    arg_bindings: list[TaskArgBinding] | None,
    xcom_resolver: XComResolver,
    *,
    skip_defaults: bool = True,
) -> Any:
    """Bind ``arg_bindings`` and invoke ``fn``, returning its result (the task's return_value XCom)."""
    return fn(**dict(_bind_arguments(arg_bindings, xcom_resolver, skip_defaults=skip_defaults)))
