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
from typing import TypeVar

TaskHandlerF = TypeVar("TaskHandlerF", bound=Callable)

_REGISTRY: dict[tuple[str, str], Callable] = {}


def get_handler(dag_id: str, task_id: str) -> Callable | None:
    """Get the handler registered for the task, or *None* if none is."""
    return _REGISTRY.get((dag_id, task_id))


def task_handler(dag_id: str, task_id: str | None = None) -> Callable[[TaskHandlerF], TaskHandlerF]:
    """
    Register the decorated function as the body of a stub task.

    If not specified, *task_id* defaults to the function name. The decorated
    function is returned unchanged.

    Example:

    .. code-block:: python

        @task_handler("events_pipeline")
        def transform(region: str, events: list[dict]) -> dict: ...
    """

    def _task_handler(fn: TaskHandlerF) -> TaskHandlerF:
        if not (resolved_task_id := task_id or getattr(fn, "__name__", None)):
            raise ValueError("task_id could not be derived from the handler; pass task_id explicitly")
        if (key := (dag_id, resolved_task_id)) in _REGISTRY:
            raise ValueError(f"handler already registered for dag_id={dag_id!r} task_id={resolved_task_id!r}")
        _REGISTRY[key] = fn
        return fn

    return _task_handler
