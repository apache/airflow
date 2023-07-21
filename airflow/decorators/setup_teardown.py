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

import types
from typing import Callable

from airflow import AirflowException, XComArg
from airflow.decorators import python_task
from airflow.decorators.task_group import _TaskGroupFactory
from airflow.models import BaseOperator
from airflow.utils.setup_teardown import SetupTeardownContext


def setup_task(func: Callable) -> Callable:
    # Using FunctionType here since _TaskDecorator is also a callable
    if isinstance(func, types.FunctionType):
        func = python_task(func)
    if isinstance(func, _TaskGroupFactory):
        raise AirflowException("Task groups cannot be marked as setup or teardown.")
    func.is_setup = True  # type: ignore[attr-defined]
    return func


def teardown_task(_func=None, *, on_failure_fail_dagrun: bool = False) -> Callable:
    def teardown(func: Callable) -> Callable:
        # Using FunctionType here since _TaskDecorator is also a callable
        if isinstance(func, types.FunctionType):
            func = python_task(func)
        if isinstance(func, _TaskGroupFactory):
            raise AirflowException("Task groups cannot be marked as setup or teardown.")
        func.is_teardown = True  # type: ignore[attr-defined]
        func.on_failure_fail_dagrun = on_failure_fail_dagrun  # type: ignore[attr-defined]
        return func

    if _func is None:
        return teardown
    return teardown(_func)


class ContextWrapper(list):
    """A list subclass that has a context manager that pushes setup/teardown tasks to the context."""

    def __init__(self, tasks: list[BaseOperator | XComArg]):
        self.tasks = tasks
        super().__init__(tasks)

    def __enter__(self):
        operators = []
        for task in self.tasks:
            if isinstance(task, BaseOperator):
                operators.append(task)
                if not task.is_setup and not task.is_teardown:
                    raise AirflowException("Only setup/teardown tasks can be used as context managers.")
            elif not task.operator.is_setup and not task.operator.is_teardown:
                raise AirflowException("Only setup/teardown tasks can be used as context managers.")
        if not operators:
            # means we have XComArgs
            operators = [task.operator for task in self.tasks]
        SetupTeardownContext.push_setup_teardown_task(operators)
        return SetupTeardownContext

    def __exit__(self, exc_type, exc_val, exc_tb):
        SetupTeardownContext.set_work_task_roots_and_leaves()


context_wrapper = ContextWrapper
