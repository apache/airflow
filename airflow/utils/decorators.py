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
from __future__ import annotations

import warnings
from collections import deque
from functools import wraps
from typing import Callable, TypeVar, cast

from airflow.exceptions import RemovedInAirflow3Warning

T = TypeVar('T', bound=Callable)


def apply_defaults(func: T) -> T:
    """
    This decorator is deprecated.

    In previous versions, all subclasses of BaseOperator must use apply_default decorator for the"
    `default_args` feature to work properly.

    In current version, it is optional. The decorator is applied automatically using the metaclass.
    """
    warnings.warn(
        "This decorator is deprecated. \n"
        "\n"
        "In previous versions, all subclasses of BaseOperator must use apply_default decorator for the "
        "`default_args` feature to work properly.\n"
        "\n"
        "In current version, it is optional. The decorator is applied automatically using the metaclass.\n",
        RemovedInAirflow3Warning,
        stacklevel=3,
    )

    # Make it still be a wrapper to keep the previous behaviour of an extra stack frame
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return cast(T, wrapper)


def remove_task_decorator(python_source: str, task_decorator_name: str) -> str:
    """
    Removed @task.

    :param python_source:
    """
    if task_decorator_name not in python_source:
        return python_source
    split = python_source.split(task_decorator_name)
    before_decorator, after_decorator = split[0], split[1]
    if after_decorator[0] == "(":
        after_decorator = _balance_parens(after_decorator)
    if after_decorator[0] == "\n":
        after_decorator = after_decorator[1:]
    return before_decorator + after_decorator


def _balance_parens(after_decorator):
    num_paren = 1
    after_decorator = deque(after_decorator)
    after_decorator.popleft()
    while num_paren:
        current = after_decorator.popleft()
        if current == "(":
            num_paren = num_paren + 1
        elif current == ")":
            num_paren = num_paren - 1
    return ''.join(after_decorator)
