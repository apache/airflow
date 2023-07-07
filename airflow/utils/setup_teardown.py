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

from typing import TYPE_CHECKING, cast

from airflow.exceptions import AirflowException

if TYPE_CHECKING:
    from airflow.models.abstractoperator import AbstractOperator
    from airflow.models.taskmixin import DependencyMixin
    from airflow.models.xcom_arg import PlainXComArg


class BaseSetupTeardownContext:
    """Context manager for setup/teardown tasks.

    :meta private:
    """

    active: bool = False
    context_map: dict[AbstractOperator | tuple[AbstractOperator], list[AbstractOperator]] = {}
    _context_managed_setup_task: AbstractOperator | list[AbstractOperator] = []
    _previous_context_managed_setup_task: list[AbstractOperator | list[AbstractOperator]] = []
    _context_managed_teardown_task: AbstractOperator | list[AbstractOperator] = []
    _previous_context_managed_teardown_task: list[AbstractOperator | list[AbstractOperator]] = []

    @classmethod
    def push_context_managed_setup_task(cls, task: AbstractOperator | list[AbstractOperator]):
        if cls._context_managed_setup_task:
            cls._previous_context_managed_setup_task.append(cls._context_managed_setup_task)
        cls._context_managed_setup_task = task

    @classmethod
    def push_context_managed_teardown_task(cls, task: AbstractOperator | list[AbstractOperator]):
        if cls._context_managed_teardown_task:
            cls._previous_context_managed_teardown_task.append(cls._context_managed_teardown_task)
        cls._context_managed_teardown_task = task

    @classmethod
    def pop_context_managed_setup_task(cls) -> AbstractOperator | list[AbstractOperator]:
        old_setup_task = cls._context_managed_setup_task
        if cls._previous_context_managed_setup_task:
            cls._context_managed_setup_task = cls._previous_context_managed_setup_task.pop()
            setup_task = cls._context_managed_setup_task
            if setup_task and old_setup_task:
                if isinstance(setup_task, list):
                    for task in setup_task:
                        task.set_downstream(old_setup_task)
                else:
                    setup_task.set_downstream(old_setup_task)
        else:
            cls._context_managed_setup_task = []
        return old_setup_task

    @classmethod
    def update_context_map(cls, task: DependencyMixin):
        from airflow.models.abstractoperator import AbstractOperator

        task_ = cast(AbstractOperator, task)
        if task_.is_setup or task_.is_teardown:
            return
        ctx = cls.context_map

        def _get_or_set_item(item):
            if ctx.get(item) is None:
                ctx[item] = [task_]
            else:
                ctx[item].append(task_)

        if setup_task := cls.get_context_managed_setup_task():
            if isinstance(setup_task, list):
                _get_or_set_item(tuple(setup_task))
            else:
                _get_or_set_item(setup_task)
        if teardown_task := cls.get_context_managed_teardown_task():
            if isinstance(teardown_task, list):
                _get_or_set_item(tuple(teardown_task))
            else:
                _get_or_set_item(teardown_task)

    @classmethod
    def pop_context_managed_teardown_task(cls) -> AbstractOperator | list[AbstractOperator]:
        old_teardown_task = cls._context_managed_teardown_task
        if cls._previous_context_managed_teardown_task:
            cls._context_managed_teardown_task = cls._previous_context_managed_teardown_task.pop()
            teardown_task = cls._context_managed_teardown_task
            if teardown_task and old_teardown_task:
                if isinstance(teardown_task, list):
                    for task in teardown_task:
                        task.set_upstream(old_teardown_task)
                else:
                    teardown_task.set_upstream(old_teardown_task)
        else:
            cls._context_managed_teardown_task = []
        return old_teardown_task

    @classmethod
    def get_context_managed_setup_task(cls) -> AbstractOperator | list[AbstractOperator]:
        return cls._context_managed_setup_task

    @classmethod
    def get_context_managed_teardown_task(cls) -> AbstractOperator | list[AbstractOperator]:
        return cls._context_managed_teardown_task

    @classmethod
    def push_setup_teardown_task(cls, operator: AbstractOperator | list[AbstractOperator]):
        if isinstance(operator, list):
            if operator[0].is_teardown:
                cls._push_tasks(operator)
            elif operator[0].is_setup:
                cls._push_tasks(operator, setup=True)
        elif operator.is_teardown:
            cls._push_tasks(operator)
        elif operator.is_setup:
            cls._push_tasks(operator, setup=True)
        cls.active = True

    @classmethod
    def _push_tasks(cls, operator: AbstractOperator | list[AbstractOperator], setup: bool = False):
        if isinstance(operator, list):
            upstream_tasks = operator[0].upstream_list
            downstream_list = operator[0].downstream_list
            if not all(task.is_setup == operator[0].is_setup for task in operator):
                cls.error("All tasks in the list must be either setup or teardown tasks")
        else:
            upstream_tasks = operator.upstream_list
            downstream_list = operator.downstream_list
        if setup:
            if upstream_tasks:
                cls.error("Setup tasks cannot have upstreams set manually on the context manager")
            cls.push_context_managed_setup_task(operator)
            for task in downstream_list:
                if not task.is_teardown:
                    cls.error(
                        "Downstream tasks to a setup task must be a teardown task on the context manager"
                    )
                if task.downstream_list:
                    cls.error("Multiple shifts are not allowed in the context manager")
            if downstream_list:
                cls.push_context_managed_teardown_task(list(downstream_list))
        else:
            for task in upstream_tasks:
                if not task.is_setup:
                    cls.error("Upstream tasks to a teardown task must be a setup task on the context manager")
                if task.upstream_list:
                    cls.error("Multiple shifts are not allowed in the context manager")
            if downstream_list:
                cls.error("Downstream to a teardown task cannot be set manually on the context manager")
            cls.push_context_managed_teardown_task(operator)
            if upstream_tasks:
                cls.push_context_managed_setup_task(list(upstream_tasks))

    @classmethod
    def set_work_task_roots_and_leaves(cls):
        if setup_task := cls.get_context_managed_setup_task():
            if isinstance(setup_task, list):
                setup_task = tuple(setup_task)
            tasks_in_context = cls.context_map.get(setup_task, [])
            if tasks_in_context:
                roots = [task for task in tasks_in_context if not task.upstream_list]
                if not roots:
                    setup_task >> tasks_in_context[0]
                elif isinstance(setup_task, tuple):
                    for task in setup_task:
                        task >> roots
                else:
                    setup_task >> roots
        if teardown_task := cls.get_context_managed_teardown_task():
            if isinstance(teardown_task, list):
                teardown_task = tuple(teardown_task)
            tasks_in_context = cls.context_map.get(teardown_task, [])
            if tasks_in_context:
                leaves = [task for task in tasks_in_context if not task.downstream_list]
                if not leaves:
                    teardown_task << tasks_in_context[-1]
                elif isinstance(teardown_task, tuple):
                    for task in teardown_task:
                        task << leaves
                else:
                    teardown_task << leaves
        setup_task = cls.pop_context_managed_setup_task()
        teardown_task = cls.pop_context_managed_teardown_task()
        if isinstance(setup_task, list):
            setup_task = tuple(setup_task)
        if isinstance(teardown_task, list):
            teardown_task = tuple(teardown_task)
        cls.active = False
        cls.context_map.pop(setup_task, None)
        cls.context_map.pop(teardown_task, None)

    @classmethod
    def error(cls, message: str):
        cls.active = False
        cls.context_map.clear()
        cls._context_managed_setup_task = []
        cls._context_managed_teardown_task = []
        cls._previous_context_managed_setup_task = []
        cls._previous_context_managed_teardown_task = []
        raise ValueError(message)


class SetupTeardownContext(BaseSetupTeardownContext):
    """Context manager for setup and teardown tasks."""

    @staticmethod
    def add_task(task: AbstractOperator | PlainXComArg):
        """Add task to context manager."""
        from airflow.models.xcom_arg import PlainXComArg

        if not SetupTeardownContext.active:
            raise AirflowException("Cannot add task to context outside the context manager.")
        if isinstance(task, PlainXComArg):
            task = task.operator
        SetupTeardownContext.update_context_map(task)
