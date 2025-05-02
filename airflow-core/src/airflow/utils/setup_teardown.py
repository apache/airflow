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
    from airflow.models.taskmixin import DependencyMixin
    from airflow.sdk.definitions._internal.abstractoperator import AbstractOperator
    from airflow.sdk.definitions.xcom_arg import PlainXComArg


class BaseSetupTeardownContext:
    """
    Context manager for setup/teardown tasks.

    :meta private:
    """

    active: bool = False
    context_map: dict[AbstractOperator | tuple[AbstractOperator], list[AbstractOperator]] = {}
    _context_managed_setup_task: AbstractOperator | list[AbstractOperator] = []
    _previous_context_managed_setup_task: list[AbstractOperator | list[AbstractOperator]] = []
    _context_managed_teardown_task: AbstractOperator | list[AbstractOperator] = []
    _previous_context_managed_teardown_task: list[AbstractOperator | list[AbstractOperator]] = []
    _teardown_downstream_of_setup: AbstractOperator | list[AbstractOperator] = []
    _previous_teardown_downstream_of_setup: list[AbstractOperator | list[AbstractOperator]] = []
    _setup_upstream_of_teardown: AbstractOperator | list[AbstractOperator] = []
    _previous_setup_upstream_of_teardown: list[AbstractOperator | list[AbstractOperator]] = []

    @classmethod
    def push_context_managed_setup_task(cls, task: AbstractOperator | list[AbstractOperator]):
        setup_task = cls._context_managed_setup_task
        if setup_task and setup_task != task:
            cls._previous_context_managed_setup_task.append(cls._context_managed_setup_task)
        cls._context_managed_setup_task = task

    @classmethod
    def push_context_managed_teardown_task(cls, task: AbstractOperator | list[AbstractOperator]):
        teardown_task = cls._context_managed_teardown_task
        if teardown_task and teardown_task != task:
            cls._previous_context_managed_teardown_task.append(cls._context_managed_teardown_task)
        cls._context_managed_teardown_task = task

    @classmethod
    def pop_context_managed_setup_task(cls) -> AbstractOperator | list[AbstractOperator]:
        old_setup_task = cls._context_managed_setup_task
        if cls._previous_context_managed_setup_task:
            cls._context_managed_setup_task = cls._previous_context_managed_setup_task.pop()
            setup_task = cls._context_managed_setup_task
            if setup_task and old_setup_task:
                cls.set_dependency(old_setup_task, setup_task, upstream=False)
        else:
            cls._context_managed_setup_task = []
        return old_setup_task

    @classmethod
    def pop_context_managed_teardown_task(cls) -> AbstractOperator | list[AbstractOperator]:
        old_teardown_task = cls._context_managed_teardown_task
        if cls._previous_context_managed_teardown_task:
            cls._context_managed_teardown_task = cls._previous_context_managed_teardown_task.pop()
            teardown_task = cls._context_managed_teardown_task
            if teardown_task and old_teardown_task:
                cls.set_dependency(old_teardown_task, teardown_task)
        else:
            cls._context_managed_teardown_task = []
        return old_teardown_task

    @classmethod
    def pop_teardown_downstream_of_setup(cls) -> AbstractOperator | list[AbstractOperator]:
        old_teardown_task = cls._teardown_downstream_of_setup
        if cls._previous_teardown_downstream_of_setup:
            cls._teardown_downstream_of_setup = cls._previous_teardown_downstream_of_setup.pop()
            teardown_task = cls._teardown_downstream_of_setup
            if teardown_task and old_teardown_task:
                cls.set_dependency(old_teardown_task, teardown_task)
        else:
            cls._teardown_downstream_of_setup = []
        return old_teardown_task

    @classmethod
    def pop_setup_upstream_of_teardown(cls) -> AbstractOperator | list[AbstractOperator]:
        old_setup_task = cls._setup_upstream_of_teardown
        if cls._previous_setup_upstream_of_teardown:
            cls._setup_upstream_of_teardown = cls._previous_setup_upstream_of_teardown.pop()
            setup_task = cls._setup_upstream_of_teardown
            if setup_task and old_setup_task:
                cls.set_dependency(old_setup_task, setup_task, upstream=False)
        else:
            cls._setup_upstream_of_teardown = []
        return old_setup_task

    @classmethod
    def set_dependency(
        cls,
        receiving_task: AbstractOperator | list[AbstractOperator],
        new_task: AbstractOperator | list[AbstractOperator],
        upstream=True,
    ):
        if isinstance(new_task, (list, tuple)):
            for task in new_task:
                cls._set_dependency(task, receiving_task, upstream)
        else:
            cls._set_dependency(new_task, receiving_task, upstream)

    @staticmethod
    def _set_dependency(task, receiving_task, upstream):
        if upstream:
            task.set_upstream(receiving_task)
        else:
            task.set_downstream(receiving_task)

    @classmethod
    def update_context_map(cls, task: DependencyMixin):
        task_ = cast("AbstractOperator", task)
        if task_.is_setup or task_.is_teardown:
            return
        ctx = cls.context_map

        def _append_or_set_item(item):
            if ctx.get(item) is None:
                ctx[item] = [task_]
            else:
                ctx[item].append(task_)

        if setup_task := cls._context_managed_setup_task:
            if isinstance(setup_task, list):
                _append_or_set_item(tuple(setup_task))
            else:
                _append_or_set_item(setup_task)
        if teardown_task := cls._context_managed_teardown_task:
            if isinstance(teardown_task, list):
                _append_or_set_item(tuple(teardown_task))
            else:
                _append_or_set_item(teardown_task)

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
            if any(task.is_setup != operator[0].is_setup for task in operator):
                cls.error("All tasks in the list must be either setup or teardown tasks")
        if setup:
            cls.push_context_managed_setup_task(operator)
            # workout the teardown
            cls._update_teardown_downstream(operator)
        else:
            cls.push_context_managed_teardown_task(operator)
            # workout the setups
            cls._update_setup_upstream(operator)

    @classmethod
    def _update_teardown_downstream(cls, operator: AbstractOperator | list[AbstractOperator]):
        """
        Recursively go through the tasks downstream of the setup in the context manager.

        If found, update the _teardown_downstream_of_setup accordingly.
        """
        operator = operator[0] if isinstance(operator, list) else operator

        def _get_teardowns(tasks):
            teardowns = [i for i in tasks if i.is_teardown]
            if not teardowns:
                all_lists = [task.downstream_list + task.upstream_list for task in tasks]
                new_list = [
                    x
                    for sublist in all_lists
                    for x in sublist
                    if (isinstance(operator, list) and x in operator) or x != operator
                ]
                if not new_list:
                    return []
                return _get_teardowns(new_list)
            return teardowns

        teardowns = _get_teardowns(operator.downstream_list)
        teardown_task = cls._teardown_downstream_of_setup
        if teardown_task and teardown_task != teardowns:
            cls._previous_teardown_downstream_of_setup.append(cls._teardown_downstream_of_setup)
        cls._teardown_downstream_of_setup = teardowns

    @classmethod
    def _update_setup_upstream(cls, operator: AbstractOperator | list[AbstractOperator]):
        """
        Recursively go through the tasks upstream of the teardown task in the context manager.

        If found, updates the _setup_upstream_of_teardown accordingly.
        """
        operator = operator[0] if isinstance(operator, list) else operator

        def _get_setups(tasks):
            setups = [i for i in tasks if i.is_setup]
            if not setups:
                all_lists = [task.downstream_list + task.upstream_list for task in tasks]
                new_list = [
                    x
                    for sublist in all_lists
                    for x in sublist
                    if (isinstance(operator, list) and x in operator) or x != operator
                ]
                if not new_list:
                    return []
                return _get_setups(new_list)
            return setups

        setups = _get_setups(operator.upstream_list)
        setup_task = cls._setup_upstream_of_teardown
        if setup_task and setup_task != setups:
            cls._previous_setup_upstream_of_teardown.append(cls._setup_upstream_of_teardown)
        cls._setup_upstream_of_teardown = setups

    @classmethod
    def set_teardown_task_as_leaves(cls, leaves):
        teardown_task = cls._teardown_downstream_of_setup
        if cls._context_managed_teardown_task:
            cls.set_dependency(cls._context_managed_teardown_task, teardown_task)
        else:
            cls.set_dependency(leaves, teardown_task)

    @classmethod
    def set_setup_task_as_roots(cls, roots):
        setup_task = cls._setup_upstream_of_teardown
        if cls._context_managed_setup_task:
            cls.set_dependency(cls._context_managed_setup_task, setup_task, upstream=False)
        else:
            cls.set_dependency(roots, setup_task, upstream=False)

    @classmethod
    def set_work_task_roots_and_leaves(cls):
        """Set the work task roots and leaves."""
        if setup_task := cls._context_managed_setup_task:
            if isinstance(setup_task, list):
                setup_task = tuple(setup_task)
            tasks_in_context = [
                x for x in cls.context_map.get(setup_task, []) if not x.is_teardown and not x.is_setup
            ]
            if tasks_in_context:
                roots = [task for task in tasks_in_context if not task.upstream_list]
                if not roots:
                    setup_task >> tasks_in_context[0]
                else:
                    cls.set_dependency(roots, setup_task, upstream=False)
                leaves = [task for task in tasks_in_context if not task.downstream_list]
                if not leaves:
                    leaves = tasks_in_context[-1]
                cls.set_teardown_task_as_leaves(leaves)

        if teardown_task := cls._context_managed_teardown_task:
            if isinstance(teardown_task, list):
                teardown_task = tuple(teardown_task)
            tasks_in_context = [
                x for x in cls.context_map.get(teardown_task, []) if not x.is_teardown and not x.is_setup
            ]
            if tasks_in_context:
                leaves = [task for task in tasks_in_context if not task.downstream_list]
                if not leaves:
                    teardown_task << tasks_in_context[-1]
                else:
                    cls.set_dependency(leaves, teardown_task)
                roots = [task for task in tasks_in_context if not task.upstream_list]
                if not roots:
                    roots = tasks_in_context[0]
                cls.set_setup_task_as_roots(roots)
        cls.set_setup_teardown_relationships()
        cls.active = False

    @classmethod
    def set_setup_teardown_relationships(cls):
        """
        Set relationship between setup to setup and teardown to teardown.

        code:: python
            with setuptask >> teardowntask:
                with setuptask2 >> teardowntask2:
                    ...

        We set setuptask >> setuptask2, teardowntask >> teardowntask2
        """
        setup_task = cls.pop_context_managed_setup_task()
        teardown_task = cls.pop_context_managed_teardown_task()
        if isinstance(setup_task, list):
            setup_task = tuple(setup_task)
        if isinstance(teardown_task, list):
            teardown_task = tuple(teardown_task)
        cls.pop_teardown_downstream_of_setup()
        cls.pop_setup_upstream_of_teardown()
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
        from airflow.sdk.definitions.xcom_arg import PlainXComArg

        if not SetupTeardownContext.active:
            raise AirflowException("Cannot add task to context outside the context manager.")
        if isinstance(task, PlainXComArg):
            task = task.operator
        SetupTeardownContext.update_context_map(task)
