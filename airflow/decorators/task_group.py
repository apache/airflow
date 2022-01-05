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
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, TypeVar, overload

from airflow.utils.task_group import TaskGroup

if TYPE_CHECKING:
    from airflow.models import DAG

F = TypeVar("F", bound=Callable[..., Any])

task_group_sig = signature(TaskGroup.__init__)


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

    def wrapper(f):
        # Setting group_id as function name if not given in kwarg group_id
        if not tg_args and 'group_id' not in tg_kwargs:
            tg_kwargs['group_id'] = f.__name__
        task_group_bound_args = task_group_sig.bind_partial(*tg_args, **tg_kwargs)

        @functools.wraps(f)
        def factory(*args, **kwargs):
            # Generate signature for decorated function and bind the arguments when called
            # we do this to extract parameters so we can annotate them on the DAG object.
            # In addition, this fails if we are missing any args/kwargs with TypeError as expected.
            # Apply defaults to capture default values if set.

            # Initialize TaskGroup with bound arguments
            with TaskGroup(
                *task_group_bound_args.args,
                add_suffix_on_collision=True,
                **task_group_bound_args.kwargs,
            ):
                # Invoke function to run Tasks inside the TaskGroup
                return f(*args, **kwargs)

        return factory

    if callable(python_callable):
        return wrapper(python_callable)
    return wrapper
