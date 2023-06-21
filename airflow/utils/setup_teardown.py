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

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.models.operator import Operator


class SetupTeardownContext:
    """Context manager for setup/teardown tasks."""

    _context_managed_setup_task: Operator | list[Operator] | None = None
    _previous_context_managed_setup_task: list[Operator | list[Operator]] = []
    _context_managed_teardown_task: Operator | list[Operator] | None = None
    _previous_context_managed_teardown_task: list[Operator | list[Operator]] = []
    active: bool = False
    context_map: dict[Operator | tuple[Operator], list[Operator]] = {}

    @classmethod
    def push_context_managed_setup_task(cls, task: Operator | list[Operator]):
        if cls._context_managed_setup_task:
            cls._previous_context_managed_setup_task.append(cls._context_managed_setup_task)
        cls._context_managed_setup_task = task

    @classmethod
    def push_context_managed_teardown_task(cls, task: Operator | list[Operator]):
        if cls._context_managed_teardown_task:
            cls._previous_context_managed_teardown_task.append(cls._context_managed_teardown_task)
        cls._context_managed_teardown_task = task

    @classmethod
    def pop_context_managed_setup_task(cls) -> Operator | list[Operator] | None:
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
            cls._context_managed_setup_task = None
        return old_setup_task

    @classmethod
    def update_context_map(cls, operator):
        ctx = SetupTeardownContext.context_map
        if setup_task := SetupTeardownContext.get_context_managed_setup_task():
            if isinstance(setup_task, list):
                setup_task = tuple(setup_task)
            if ctx.get(setup_task) is None:
                ctx[setup_task] = [operator]
            else:
                ctx[setup_task].append(operator)
        if teardown_task := SetupTeardownContext.get_context_managed_teardown_task():
            if isinstance(teardown_task, list):
                teardown_task = tuple(teardown_task)
            if ctx.get(teardown_task) is None:
                ctx[teardown_task] = [operator]
            else:
                ctx[teardown_task].append(operator)

    @classmethod
    def pop_context_managed_teardown_task(cls) -> Operator | list[Operator] | None:
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
            cls._context_managed_teardown_task = None
        return old_teardown_task

    @classmethod
    def get_context_managed_setup_task(cls) -> Operator | list[Operator] | None:
        return cls._context_managed_setup_task

    @classmethod
    def get_context_managed_teardown_task(cls) -> Operator | list[Operator] | None:
        return cls._context_managed_teardown_task

    @classmethod
    def push_setup_teardown_task(cls, operator: Operator | list[Operator]):
        if isinstance(operator, list):
            first_task: Operator = operator[0]
            if first_task.is_teardown:
                if not all(task.is_teardown == first_task.is_teardown for task in operator):
                    raise ValueError("All tasks in the list must be either setup or teardown tasks")
                upstream_tasks = first_task.upstream_list
                for task in upstream_tasks:
                    if not task.is_setup and not task.is_teardown:
                        raise ValueError(
                            "All upstream tasks in the context manager must be a setup or teardown task"
                        )
                SetupTeardownContext.push_context_managed_teardown_task(operator)
                upstream_setup: list[Operator] = [task for task in upstream_tasks if task.is_setup]
                if upstream_setup:
                    SetupTeardownContext.push_context_managed_setup_task(upstream_setup)
            elif first_task.is_setup:
                if not all(task.is_setup == first_task.is_setup for task in operator):
                    raise ValueError("All tasks in the list must be either setup or teardown tasks")
                for task in first_task.upstream_list:
                    if not task.is_setup and not task.is_teardown:
                        raise ValueError(
                            "All upstream tasks in the context manager must be a setup or teardown task"
                        )
                SetupTeardownContext.push_context_managed_setup_task(operator)
                downstream_teardown: list[Operator] = [
                    task for task in first_task.downstream_list if task.is_teardown
                ]
                if downstream_teardown:
                    SetupTeardownContext.push_context_managed_teardown_task(downstream_teardown)
        elif operator.is_teardown:
            upstream_tasks = operator.upstream_list
            for task in upstream_tasks:
                if not task.is_setup and not task.is_teardown:
                    raise ValueError(
                        "All upstream tasks in the context manager must be a setup or teardown task"
                    )
            SetupTeardownContext.push_context_managed_teardown_task(operator)
            upstream_setup = [task for task in upstream_tasks if task.is_setup]
            if upstream_setup:
                SetupTeardownContext.push_context_managed_setup_task(upstream_setup)
        elif operator.is_setup:
            for task in operator.upstream_list:
                if not task.is_setup and not task.is_teardown:
                    raise ValueError(
                        "All upstream tasks in the context manager must be a setup or teardown task"
                    )
            SetupTeardownContext.push_context_managed_setup_task(operator)
            downstream_teardown = [task for task in operator.downstream_list if task.is_teardown]
            if downstream_teardown:
                SetupTeardownContext.push_context_managed_teardown_task(downstream_teardown)
        SetupTeardownContext.active = True

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
        setup_task = SetupTeardownContext.pop_context_managed_setup_task()
        teardown_task = SetupTeardownContext.pop_context_managed_teardown_task()
        if isinstance(setup_task, list):
            setup_task = tuple(setup_task)
        if isinstance(teardown_task, list):
            teardown_task = tuple(teardown_task)
        SetupTeardownContext.active = False
        SetupTeardownContext.context_map.pop(setup_task, None)
        SetupTeardownContext.context_map.pop(teardown_task, None)
