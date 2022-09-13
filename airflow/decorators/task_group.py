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
"""Implements the ``@task_group`` function decorator.

When the decorated function is called, a task group will be created to represent
a collection of closely related tasks on the same DAG that should be grouped
together when the DAG is displayed graphically.
"""

from __future__ import annotations

import functools
from inspect import signature
from typing import TYPE_CHECKING, Any, Callable, Generic, TypeVar, overload

import attr

from airflow.models.taskmixin import DAGNode
from airflow.typing_compat import ParamSpec
from airflow.utils.task_group import TaskGroup

if TYPE_CHECKING:
    from airflow.models.dag import DAG
    from airflow.models.expandinput import OperatorExpandArgument, OperatorExpandKwargsArgument

FParams = ParamSpec("FParams")
FReturn = TypeVar("FReturn", None, DAGNode)

task_group_sig = signature(TaskGroup.__init__)


@attr.define
class _TaskGroupFactory(Generic[FParams, FReturn]):
    function: Callable[FParams, FReturn] = attr.ib(validator=attr.validators.is_callable())
    kwargs: dict[str, Any] = attr.ib(factory=dict)

    @function.validator
    def _validate_function(self, _, f: Callable[FParams, FReturn]):
        if 'self' in signature(f).parameters:
            raise TypeError('@task_group does not support methods')

    @kwargs.validator
    def _validate(self, _, kwargs):
        task_group_sig.bind_partial(**kwargs)

    def __attrs_post_init__(self):
        if not self.kwargs.get("group_id"):
            self.kwargs["group_id"] = self.function.__name__

    def __call__(self, *args: FParams.args, **kwargs: FParams.kwargs) -> DAGNode:
        """Instantiate the task group.

        with self._make_task_group(add_suffix_on_collision=True, **self.kwargs) as task_group:
        This uses the wrapped function to create a task group. Depending on the
        return type of the wrapped function, this either returns the last task
        in the group, or the group itself, to support task chaining.
        """
        with TaskGroup(add_suffix_on_collision=True, **self.kwargs) as task_group:
            if self.function.__doc__ and not task_group.tooltip:
                task_group.tooltip = self.function.__doc__

            # Invoke function to run Tasks inside the TaskGroup
            retval = self.function(*args, **kwargs)

        # If the task-creating function returns a task, forward the return value
        # so dependencies bind to it. This is equivalent to
        #   with TaskGroup(...) as tg:
        #       t2 = task_2(task_1())
        #   start >> t2 >> end
        if retval is not None:
            return retval

        # Otherwise return the task group as a whole, equivalent to
        #   with TaskGroup(...) as tg:
        #       task_1()
        #       task_2()
        #   start >> tg >> end
        return task_group

    def override(self, **kwargs: Any) -> _TaskGroupFactory[FParams, FReturn]:
        return attr.evolve(self, kwargs={**self.kwargs, **kwargs})

    def partial(self, **kwargs: Any) -> _TaskGroupFactory[FParams, FReturn]:
        raise NotImplementedError("TODO: Implement me")

    def expand(self, **kwargs: OperatorExpandArgument) -> TaskGroup:
        raise NotImplementedError("TODO: Implement me")

    def expand_kwargs(self, kwargs: OperatorExpandKwargsArgument, *, strict: bool = True) -> TaskGroup:
        raise NotImplementedError("TODO: Implement me")


# This covers the @task_group() case. Annotations are copied from the TaskGroup
# class, only providing a default to 'group_id' (this is optional for the
# decorator and defaults to the decorated function's name). Please keep them in
# sync with TaskGroup when you can! Note that since this is an overload, these
# argument defaults aren't actually used at runtime--the real implementation
# does not use them, and simply rely on TaskGroup's defaults, so it's not
# disastrous if they go out of sync with TaskGroup.
@overload
def task_group(
    group_id: str | None = None,
    prefix_group_id: bool = True,
    parent_group: TaskGroup | None = None,
    dag: DAG | None = None,
    default_args: dict[str, Any] | None = None,
    tooltip: str = "",
    ui_color: str = "CornflowerBlue",
    ui_fgcolor: str = "#000",
    add_suffix_on_collision: bool = False,
) -> Callable[FParams, _TaskGroupFactory[FParams, FReturn]]:
    ...


# This covers the @task_group case (no parentheses).
@overload
def task_group(python_callable: Callable[FParams, FReturn]) -> _TaskGroupFactory[FParams, FReturn]:
    ...


def task_group(python_callable=None, **tg_kwargs):
    """Python TaskGroup decorator.

    This wraps a function into an Airflow TaskGroup. When used as the
    ``@task_group()`` form, all arguments are forwarded to the underlying
    TaskGroup class. Can be used to parametrize TaskGroup.

    :param python_callable: Function to decorate.
    :param tg_kwargs: Keyword arguments for the TaskGroup object.
    """
    if callable(python_callable) and not tg_kwargs:
        return _TaskGroupFactory(function=python_callable, kwargs=tg_kwargs)
    return functools.partial(_TaskGroupFactory, kwargs=tg_kwargs)
