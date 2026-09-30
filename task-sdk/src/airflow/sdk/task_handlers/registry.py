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

from collections.abc import Callable
from typing import NamedTuple, TypeVar

__all__ = ["TaskHandlerRegistry", "get_registry", "task_handler"]

TaskHandlerF = TypeVar("TaskHandlerF", bound=Callable)


class _TaskHandlerKey(NamedTuple):
    dag_id: str
    task_id: str


class TaskHandlerRegistry:
    """
    Process-local map of (dag_id, task_id) -> callable.

    This type should generally not be used directly. Use :func:`get_registray`
    to get a process-local instance, or use :func:``task_handler` to register
    a function into the registry.

    :meta private:
    """

    def __init__(self) -> None:
        self._handlers: dict[_TaskHandlerKey, Callable] = {}

    def register(self, dag_id: str, task_id: str, fn: Callable) -> None:
        key = _TaskHandlerKey(dag_id, task_id)
        if key in self._handlers:
            raise ValueError(f"handler already registered for dag_id={dag_id!r} task_id={task_id!r}")
        if not callable(fn):
            raise TypeError(f"task handler for {dag_id!r}/{task_id!r} is not callable: {fn!r}")
        self._handlers[key] = fn

    def get(self, dag_id: str, task_id: str) -> Callable | None:
        return self._handlers.get(_TaskHandlerKey(dag_id, task_id))


_REGISTRY = TaskHandlerRegistry()


def get_registry() -> TaskHandlerRegistry:
    """
    Get the process-global task-handler registry.

    :meta private:
    """
    return _REGISTRY


def task_handler(dag_id: str, task_id: str | None = None) -> Callable[[TaskHandlerF], TaskHandlerF]:
    """
    Register the decorated function as the body of a stub task.

    ``task_id`` defaults to the function name. The decorated function is
    returned unchanged.

    Example:

    .. code-block:: python

        @task_handler("events_pipeline")
        def transform(region: str, events: list[dict]) -> dict: ...

    :meta private:
    """

    def _task_handler(fn: TaskHandlerF) -> TaskHandlerF:
        if not (resolved_task_id := task_id or getattr(fn, "__name__", None)):
            raise ValueError("task_id could not be derived from the handler; pass task_id explicitly")
        _REGISTRY.register(dag_id, resolved_task_id, fn)
        return fn

    return _task_handler
