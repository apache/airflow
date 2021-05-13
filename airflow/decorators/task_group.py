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
from typing import Callable, Optional, TypeVar, cast

from airflow.utils.task_group import TaskGroup

T = TypeVar("T", bound=Callable)  # pylint: disable=invalid-name

task_group_sig = signature(TaskGroup.__init__)


def task_group(python_callable: Optional[Callable] = None, *tg_args, **tg_kwargs) -> Callable[[T], T]:
    """
    Python TaskGroup decorator. Wraps a function into an Airflow TaskGroup.
    Accepts kwargs for operator TaskGroup. Can be used to parametrize TaskGroup.

    :param python_callable: Function to decorate
    :param tg_args: Arguments for TaskGroup object
    :type tg_args: list
    :param tg_kwargs: Kwargs for TaskGroup object.
    :type tg_kwargs: dict
    """

    def wrapper(f: T):
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
                *task_group_bound_args.args, add_suffix_on_collision=True, **task_group_bound_args.kwargs
            ):
                # Invoke function to run Tasks inside the TaskGroup
                return f(*args, **kwargs)

        return cast(T, factory)

    if callable(python_callable):
        return wrapper(python_callable)
    return wrapper
