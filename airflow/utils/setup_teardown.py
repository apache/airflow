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

from airflow.models.mappedoperator import MappedOperator

if TYPE_CHECKING:
    from airflow.models.operator import Operator


class SetupTeardownContext:
    """Context manager for setup/teardown XComArg."""

    _context_managed_setup_task: Operator | None = None
    _previous_context_managed_setup_task: list[Operator] = []
    _context_managed_teardown_task: Operator | None = None
    _previous_context_managed_teardown_task: list[Operator] = []
    active: bool = False
    children: list[Operator] = []

    @classmethod
    def push_context_managed_setup_task(cls, task: Operator):
        if cls._context_managed_setup_task:
            cls._previous_context_managed_setup_task.append(cls._context_managed_setup_task)
        cls._context_managed_setup_task = task

    @classmethod
    def push_context_managed_teardown_task(cls, task: Operator):
        if cls._context_managed_teardown_task:
            cls._previous_context_managed_teardown_task.append(cls._context_managed_teardown_task)
        cls._context_managed_teardown_task = task

    @classmethod
    def pop_context_managed_setup_task(cls) -> Operator | None:
        old_setup_task = cls._context_managed_setup_task
        if cls._previous_context_managed_setup_task:
            cls._context_managed_setup_task = cls._previous_context_managed_setup_task.pop()
            if cls._context_managed_setup_task and old_setup_task:
                cls._context_managed_setup_task.set_downstream(old_setup_task)
        else:
            cls._context_managed_setup_task = None
        return old_setup_task

    @classmethod
    def pop_context_managed_teardown_task(cls) -> Operator | None:
        old_teardown_task = cls._context_managed_teardown_task
        if cls._previous_context_managed_teardown_task:
            cls._context_managed_teardown_task = cls._previous_context_managed_teardown_task.pop()
            if cls._context_managed_teardown_task and old_teardown_task:
                cls._context_managed_teardown_task.set_upstream(old_teardown_task)
        else:
            cls._context_managed_teardown_task = None
        return old_teardown_task

    @classmethod
    def get_context_managed_setup_task(cls) -> Operator | None:
        return cls._context_managed_setup_task

    @classmethod
    def get_context_managed_teardown_task(cls) -> Operator | None:
        return cls._context_managed_teardown_task

    @classmethod
    def connect_setup_upstream(cls, operator):

        if (
            SetupTeardownContext.active
            and not isinstance(operator, MappedOperator)
            and not (operator._is_setup or operator._is_teardown)
        ):
            if not operator.upstream_list:
                setup_task = SetupTeardownContext.get_context_managed_setup_task()
                if setup_task:
                    setup_task.set_downstream(operator)

    @classmethod
    def connect_teardown_downstream(cls, operator):
        if (
            SetupTeardownContext.active
            and not isinstance(operator, MappedOperator)
            and not (operator._is_setup or operator._is_teardown)
        ):
            if not operator.upstream_list:
                teardown_task = SetupTeardownContext.get_context_managed_teardown_task()
                if teardown_task:
                    teardown_task.set_upstream(operator)

    @classmethod
    def set_work_task_roots_and_leaves(cls):
        normal_tasks = [
            task for task in SetupTeardownContext.children if not task._is_setup and not task._is_teardown
        ]
        for child in normal_tasks:
            if not child.downstream_list:
                child.set_downstream(SetupTeardownContext.get_context_managed_teardown_task())
            if not child.upstream_list:
                child.set_upstream(SetupTeardownContext.get_context_managed_setup_task())
