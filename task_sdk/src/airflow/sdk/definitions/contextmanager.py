#
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

import sys
from collections import deque
from types import ModuleType
from typing import Any, Generic, Optional, TypeVar, cast

from airflow.sdk.definitions.dag import DAG
from airflow.sdk.definitions.taskgroup import TaskGroup

T = TypeVar("T")

__all__ = [
    "DagContext",
    "TaskGroupContext",
]


# In order to add a `@classproperty`-like thing we need to define a property on a metaclass.
class ContextStackMeta(type):
    _context: deque

    # TODO: Task-SDK:
    # share_parent_context can go away once the DAG and TaskContext manager in airflow.models are removed and
    # everything uses sdk fully for definition/parsing
    def __new__(cls, name, bases, namespace, share_parent_context: bool = False, **kwargs: Any):
        if not share_parent_context:
            namespace["_context"] = deque()

        new_cls = super().__new__(cls, name, bases, namespace, **kwargs)

        return new_cls

    @property
    def active(self) -> bool:
        """The active property says if any object is currently in scope."""
        return bool(self._context)


class ContextStack(Generic[T], metaclass=ContextStackMeta):
    _context: deque[T]

    @classmethod
    def push(cls, obj: T):
        cls._context.appendleft(obj)

    @classmethod
    def pop(cls) -> T | None:
        return cls._context.popleft()

    @classmethod
    def get_current(cls) -> T | None:
        try:
            return cls._context[0]
        except IndexError:
            return None


class DagContext(ContextStack[DAG]):
    """
    DAG context is used to keep the current DAG when DAG is used as ContextManager.

    You can use DAG as context:

    .. code-block:: python

        with DAG(
            dag_id="example_dag",
            default_args=default_args,
            schedule="0 0 * * *",
            dagrun_timeout=timedelta(minutes=60),
        ) as dag:
            ...

    If you do this the context stores the DAG and whenever new task is created, it will use
    such stored DAG as the parent DAG.

    """

    # TODO: Task-SDK, should module type be optional? Will that break more?
    autoregistered_dags: set[tuple[DAG, ModuleType | None]] = set()
    current_autoregister_module_name: str | None = None

    @classmethod
    def pop(cls) -> DAG | None:
        dag = super().pop()
        # In a few cases around serialization we explicitly push None in to the stack
        if cls.current_autoregister_module_name is not None and dag and getattr(dag, "auto_register", True):
            mod = sys.modules[cls.current_autoregister_module_name]
            cls.autoregistered_dags.add((dag, mod))
        return dag

    @classmethod
    def get_current_dag(cls) -> DAG | None:
        return cast(Optional[DAG], cls.get_current())


class TaskGroupContext(ContextStack[TaskGroup]):
    """TaskGroup context is used to keep the current TaskGroup when TaskGroup is used as ContextManager."""

    @classmethod
    def get_current(cls, dag: DAG | None = None) -> TaskGroup | None:
        if current := super().get_current():
            return current
        if dag := dag or DagContext.get_current():
            # If there's currently a DAG but no TaskGroup, return the root TaskGroup of the dag.
            return dag.task_group
        return None
