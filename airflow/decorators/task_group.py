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
import warnings
from inspect import signature
from typing import TYPE_CHECKING, Any, Callable, Dict, Generic, Optional, TypeVar, cast, overload

import attr

from airflow.utils.task_group import MappedTaskGroup, TaskGroup

if TYPE_CHECKING:
    from airflow.models import DAG

F = TypeVar("F", bound=Callable[..., Any])
T = TypeVar("T", bound=Callable)
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

    def __call__(self, *args, **kwargs) -> R:
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

    def partial(self, **kwargs) -> "MappedTaskGroupDecorator[R]":
        return MappedTaskGroupDecorator(function=self.function, kwargs=self.kwargs).partial(**kwargs)

    def map(self, **kwargs) -> R:
        return MappedTaskGroupDecorator(function=self.function, kwargs=self.kwargs).map(**kwargs)


@attr.define
class MappedTaskGroupDecorator(TaskGroupDecorator[R]):
    """:meta private:"""

    partial_kwargs: Dict[str, Any] = attr.ib(factory=dict)
    """static kwargs for the decorated function"""
    mapped_kwargs: Dict[str, Any] = attr.ib(factory=dict)
    """kwargs for the decorated function"""

    def __call__(self, *args, **kwargs):
        raise RuntimeError("A mapped @task_group cannot be called. Use `.map` and `.partial` instead")

    def _make_task_group(self, **kwargs) -> MappedTaskGroup:
        tg = MappedTaskGroup(**kwargs)
        tg.partial_kwargs = self.partial_kwargs
        tg.mapped_kwargs = self.mapped_kwargs
        return tg

    def partial(self, **kwargs) -> "MappedTaskGroupDecorator[R]":
        if self.partial_kwargs:
            raise RuntimeError("Already a partial task group")
        self.partial_kwargs.update(kwargs)
        return self

    def map(self, **kwargs) -> R:
        if self.mapped_kwargs:
            raise RuntimeError("Already a mapped task group")
        self.mapped_kwargs = kwargs

        call_kwargs = self.partial_kwargs.copy()
        duplicated_keys = set(call_kwargs).intersection(kwargs)
        if duplicated_keys:
            raise RuntimeError(f"Cannot map partial arguments: {', '.join(sorted(duplicated_keys))}")
        call_kwargs.update({k: object() for k in kwargs})

        return super().__call__(**call_kwargs)

    def __del__(self):
        if not self.mapped_kwargs:
            warnings.warn(f"Partial task group {self.function.__name__} was never mapped!")


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
    parent_group: Optional["TaskGroup"] = None,
    dag: Optional["DAG"] = None,
    default_args: Optional[Dict[str, Any]] = None,
    tooltip: str = "",
    ui_color: str = "CornflowerBlue",
    ui_fgcolor: str = "#000",
    add_suffix_on_collision: bool = False,
) -> Callable[[F], F]:
    ...


# This covers the @task_group case (no parentheses).
@overload
def task_group(python_callable: F) -> F:
    ...


def task_group(python_callable=None, *tg_args, **tg_kwargs):
    """
    Python TaskGroup decorator.

    This wraps a function into an Airflow TaskGroup. When used as the
    ``@task_group()`` form, all arguments are forwarded to the underlying
    TaskGroup class. Can be used to parametrize TaskGroup.

    :param python_callable: Function to decorate.
    :param tg_args: Positional arguments for the TaskGroup object.
    :param tg_kwargs: Keyword arguments for the TaskGroup object.
    """
    if callable(python_callable):
        return TaskGroupDecorator(function=python_callable, kwargs=tg_kwargs)
    return cast("Callable[[T], T]", functools.partial(TaskGroupDecorator, kwargs=tg_kwargs))
