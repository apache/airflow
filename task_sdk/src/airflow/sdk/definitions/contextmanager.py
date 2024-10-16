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

from collections import deque
from types import ModuleType
from typing import Generic, TypeVar

from airflow.sdk.definitions.dag import DAG
from airflow.sdk.definitions.taskgroup import TaskGroup

T = TypeVar("T")


class ContextStack(Generic[T]):
    _context: deque[T]

    def __init_subclass__(cls, /, **kwargs) -> None:
        if not kwargs.get("share_parent_context", False):
            cls._context = deque()
        return super().__init_subclass__()

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

    @classmethod
    @property
    def active(cls) -> bool:
        """The active property says if any object is currently in scope."""
        try:
            cls._context[0]
            return True
        except IndexError:
            return False


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

    autoregistered_dags: set[tuple[DAG, ModuleType]] = set()
    current_autoregister_module_name: str | None = None

    @classmethod
    def pop(cls) -> DAG | None:
        dag = super().pop()
        # In a few cases around serialization we explicitly push None in to the stack
        if cls.current_autoregister_module_name is not None and dag and getattr(dag, "auto_register", True):
            # mod = sys.modules[cls.current_autoregister_module_name]
            cls.autoregistered_dags.add((dag, None))


class TaskGroupContext(ContextStack[TaskGroup]):
    """TaskGroup context is used to keep the current TaskGroup when TaskGroup is used as ContextManager."""

    @classmethod
    def get_current(cls, dag: DAG | None = None) -> TaskGroup | None:
        if current := super().get_current():
            return current
        if dag := dag or DagContext.get_current():
            # If there's currently a DAG but no TaskGroup, return the root TaskGroup of the dag.
            return dag.task_group
