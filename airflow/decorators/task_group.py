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
"""
A TaskGroup is a collection of closely related tasks on the same DAG that should be grouped
together when the DAG is displayed graphically.
"""
import functools
from inspect import signature
from typing import TYPE_CHECKING, Any, Callable, Dict, Generic, Optional, TypeVar, Union, cast, overload

import attr

from airflow.utils.task_group import TaskGroup

if TYPE_CHECKING:
    from airflow.models.dag import DAG
    from airflow.models.mappedoperator import Mappable

F = TypeVar("F", bound=Callable)
R = TypeVar("R")

task_group_sig = signature(TaskGroup.__init__)


@attr.define
class TaskGroupDecorator(Generic[R]):
    """:meta private:"""

    function: Callable[..., Optional[R]] = attr.ib(validator=attr.validators.is_callable())
    kwargs: Dict[str, Any] = attr.ib(factory=dict)
    """kwargs for the TaskGroup"""

    @function.validator
    def _validate_function(self, _, f):
        if 'self' in signature(f).parameters:
            raise TypeError('@task_group does not support methods')

    @kwargs.validator
    def _validate(self, _, kwargs):
        task_group_sig.bind_partial(**kwargs)

    def __attrs_post_init__(self):
        self.kwargs.setdefault('group_id', self.function.__name__)

    def _make_task_group(self, **kwargs) -> TaskGroup:
        return TaskGroup(**kwargs)

    def __call__(self, *args, **kwargs) -> Union[R, TaskGroup]:
        with self._make_task_group(add_suffix_on_collision=True, **self.kwargs) as task_group:
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

    def override(self, **kwargs: Any) -> "TaskGroupDecorator[R]":
        return attr.evolve(self, kwargs={**self.kwargs, **kwargs})


class Group(Generic[F]):
    """Declaration of a @task_group-decorated callable for type-checking.

    An instance of this type inherits the call signature of the decorated
    function wrapped in it (not *exactly* since it actually turns the function
    into an XComArg-compatible, but there's no way to express that right now),
    and provides two additional methods for task-mapping.

    This type is implemented by ``TaskGroupDecorator`` at runtime.
    """

    __call__: F

    function: F

    # Return value should match F's return type, but that's impossible to declare.
    def expand(self, **kwargs: "Mappable") -> Any:
        ...

    def partial(self, **kwargs: Any) -> "Group[F]":
        ...


# This covers the @task_group() case. Annotations are copied from the TaskGroup
# class, only providing a default to 'group_id' (this is optional for the
# decorator and defaults to the decorated function's name). Please keep them in
# sync with TaskGroup when you can! Note that since this is an overload, these
# argument defaults aren't actually used at runtime--the real implementation
# does not use them, and simply rely on TaskGroup's defaults, so it's not
# disastrous if they go out of sync with TaskGroup.
@overload
def task_group(
    group_id: Optional[str] = None,
    prefix_group_id: bool = True,
    parent_group: Optional[TaskGroup] = None,
    dag: Optional["DAG"] = None,
    default_args: Optional[Dict[str, Any]] = None,
    tooltip: str = "",
    ui_color: str = "CornflowerBlue",
    ui_fgcolor: str = "#000",
    add_suffix_on_collision: bool = False,
) -> Callable[[F], Group[F]]:
    ...


# This covers the @task_group case (no parentheses).
@overload
def task_group(python_callable: F) -> Group[F]:
    ...


def task_group(python_callable=None, **tg_kwargs):
    """
    Python TaskGroup decorator.

    This wraps a function into an Airflow TaskGroup. When used as the
    ``@task_group()`` form, all arguments are forwarded to the underlying
    TaskGroup class. Can be used to parametrize TaskGroup.

    :param python_callable: Function to decorate.
    :param tg_kwargs: Keyword arguments for the TaskGroup object.
    """
    if callable(python_callable):
        return TaskGroupDecorator(function=python_callable, kwargs=tg_kwargs)
    return cast(Callable[[F], F], functools.partial(TaskGroupDecorator, kwargs=tg_kwargs))
