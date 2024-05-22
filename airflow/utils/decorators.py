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

import sys
import warnings
from collections import deque
from functools import wraps
from typing import Callable, TypeVar, cast

from airflow.exceptions import RemovedInAirflow3Warning

T = TypeVar("T", bound=Callable)


def apply_defaults(func: T) -> T:
    """
    Use apply_default decorator for the `default_args` feature to work properly; deprecated.

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
    Remove @task or similar decorators as well as @setup and @teardown.

    :param python_source: The python source code
    :param task_decorator_name: the decorator name

    TODO: Python 3.9+: Rewrite this to use ast.parse and ast.unparse
    """

    def _remove_task_decorator(py_source, decorator_name):
        # if no line starts with @decorator_name, we can early exit
        for line in py_source.split("\n"):
            if line.startswith(decorator_name):
                break
        else:
            return python_source
        split = python_source.split(decorator_name, 1)
        before_decorator, after_decorator = split[0], split[1]
        if after_decorator[0] == "(":
            after_decorator = _balance_parens(after_decorator)
        if after_decorator[0] == "\n":
            after_decorator = after_decorator[1:]
        return before_decorator + after_decorator

    decorators = ["@setup", "@teardown", task_decorator_name]
    for decorator in decorators:
        python_source = _remove_task_decorator(python_source, decorator)
    return python_source


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
    return "".join(after_decorator)


class _autostacklevel_warn:
    def __init__(self):
        self.warnings = __import__("warnings")

    def __getattr__(self, name):
        return getattr(self.warnings, name)

    def __dir__(self):
        return dir(self.warnings)

    def warn(self, message, category=None, stacklevel=1, source=None):
        self.warnings.warn(message, category, stacklevel + 2, source)


def fixup_decorator_warning_stack(func):
    if func.__globals__.get("warnings") is sys.modules["warnings"]:
        # Yes, this is more than slightly hacky, but it _automatically_ sets the right stacklevel parameter to
        # `warnings.warn` to ignore the decorator.
        func.__globals__["warnings"] = _autostacklevel_warn()
