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
import inspect
import warnings
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Generic, Mapping, Sequence, TypeVar, overload

import attr

from airflow.decorators.base import ExpandableFactory
from airflow.models.expandinput import (
    DictOfListsExpandInput,
    ListOfDictsExpandInput,
    MappedArgument,
    OperatorExpandArgument,
    OperatorExpandKwargsArgument,
)
from airflow.models.taskmixin import DAGNode
from airflow.models.xcom_arg import XComArg
from airflow.typing_compat import ParamSpec
from airflow.utils.helpers import prevent_duplicates
from airflow.utils.task_group import MappedTaskGroup, TaskGroup

if TYPE_CHECKING:
    from airflow.models.dag import DAG

FParams = ParamSpec("FParams")
FReturn = TypeVar("FReturn", None, DAGNode)

task_group_sig = inspect.signature(TaskGroup.__init__)


@attr.define()
class _TaskGroupFactory(ExpandableFactory, Generic[FParams, FReturn]):
    function: Callable[FParams, FReturn] = attr.ib(validator=attr.validators.is_callable())
    tg_kwargs: dict[str, Any] = attr.ib(factory=dict)  # Parameters forwarded to TaskGroup.
    partial_kwargs: dict[str, Any] = attr.ib(factory=dict)  # Parameters forwarded to 'function'.

    _task_group_created: bool = attr.ib(False, init=False)

    tg_class: ClassVar[type[TaskGroup]] = TaskGroup

    @tg_kwargs.validator
    def _validate(self, _, kwargs):
        task_group_sig.bind_partial(**kwargs)

    def __attrs_post_init__(self):
        self.tg_kwargs.setdefault("group_id", self.function.__name__)

    def __del__(self):
        if self.partial_kwargs and not self._task_group_created:
            try:
                group_id = repr(self.tg_kwargs["group_id"])
            except KeyError:
                group_id = f"at {hex(id(self))}"
            warnings.warn(f"Partial task group {group_id} was never mapped!")

    def __call__(self, *args: FParams.args, **kwargs: FParams.kwargs) -> DAGNode:
        """Instantiate the task group.

        This uses the wrapped function to create a task group. Depending on the
        return type of the wrapped function, this either returns the last task
        in the group, or the group itself, to support task chaining.
        """
        return self._create_task_group(TaskGroup, *args, **kwargs)

    def _create_task_group(self, tg_factory: Callable[..., TaskGroup], *args: Any, **kwargs: Any) -> DAGNode:
        with tg_factory(add_suffix_on_collision=True, **self.tg_kwargs) as task_group:
            if self.function.__doc__ and not task_group.tooltip:
                task_group.tooltip = self.function.__doc__

            # Invoke function to run Tasks inside the TaskGroup
            retval = self.function(*args, **kwargs)

        self._task_group_created = True

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
        # TODO: fixme when mypy gets compatible with new attrs
        return attr.evolve(self, tg_kwargs={**self.tg_kwargs, **kwargs})  # type: ignore[arg-type]

    def partial(self, **kwargs: Any) -> _TaskGroupFactory[FParams, FReturn]:
        self._validate_arg_names("partial", kwargs)
        prevent_duplicates(self.partial_kwargs, kwargs, fail_reason="duplicate partial")
        kwargs.update(self.partial_kwargs)
        # TODO: fixme when mypy gets compatible with new attrs
        return attr.evolve(self, partial_kwargs=kwargs)  # type: ignore[arg-type]

    def expand(self, **kwargs: OperatorExpandArgument) -> DAGNode:
        if not kwargs:
            raise TypeError("no arguments to expand against")
        self._validate_arg_names("expand", kwargs)
        prevent_duplicates(self.partial_kwargs, kwargs, fail_reason="mapping already partial")
        expand_input = DictOfListsExpandInput(kwargs)
        return self._create_task_group(
            functools.partial(MappedTaskGroup, expand_input=expand_input),
            **self.partial_kwargs,
            **{k: MappedArgument(input=expand_input, key=k) for k in kwargs},
        )

    def expand_kwargs(self, kwargs: OperatorExpandKwargsArgument) -> DAGNode:
        if isinstance(kwargs, Sequence):
            for item in kwargs:
                if not isinstance(item, (XComArg, Mapping)):
                    raise TypeError(f"expected XComArg or list[dict], not {type(kwargs).__name__}")
        elif not isinstance(kwargs, XComArg):
            raise TypeError(f"expected XComArg or list[dict], not {type(kwargs).__name__}")

        # It's impossible to build a dict of stubs as keyword arguments if the
        # function uses * or ** wildcard arguments.
        function_has_vararg = any(
            v.kind == inspect.Parameter.VAR_POSITIONAL or v.kind == inspect.Parameter.VAR_KEYWORD
            for v in self.function_signature.parameters.values()
        )
        if function_has_vararg:
            raise TypeError("calling expand_kwargs() on task group function with * or ** is not supported")

        # We can't be sure how each argument is used in the function (well
        # technically we can with AST but let's not), so we have to create stubs
        # for every argument, including those with default values.
        map_kwargs = (k for k in self.function_signature.parameters if k not in self.partial_kwargs)

        expand_input = ListOfDictsExpandInput(kwargs)
        return self._create_task_group(
            functools.partial(MappedTaskGroup, expand_input=expand_input),
            **self.partial_kwargs,
            **{k: MappedArgument(input=expand_input, key=k) for k in map_kwargs},
        )


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
) -> Callable[[Callable[FParams, FReturn]], _TaskGroupFactory[FParams, FReturn]]:
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
        return _TaskGroupFactory(function=python_callable, tg_kwargs=tg_kwargs)
    return functools.partial(_TaskGroupFactory, tg_kwargs=tg_kwargs)
