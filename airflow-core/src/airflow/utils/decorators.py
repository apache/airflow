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
from typing import Callable, TypeVar

import libcst as cst

T = TypeVar("T", bound=Callable)


class _TaskDecoratorRemover(cst.CSTTransformer):
    def __init__(self, task_decorator_name: str) -> None:
        self.decorators_to_remove: set[str] = {
            "setup",
            "teardown",
            "task.skip_if",
            "task.run_if",
            task_decorator_name.strip("@"),
        }

    def _is_task_decorator(self, decorator_node: cst.Decorator) -> bool:
        decorator_expr = decorator_node.decorator
        if isinstance(decorator_expr, cst.Name):
            return decorator_expr.value in self.decorators_to_remove
        if isinstance(decorator_expr, cst.Attribute) and isinstance(decorator_expr.value, cst.Name):
            return f"{decorator_expr.value.value}.{decorator_expr.attr.value}" in self.decorators_to_remove
        if isinstance(decorator_expr, cst.Call):
            return self._is_task_decorator(cst.Decorator(decorator=decorator_expr.func))
        return False

    def leave_FunctionDef(
        self, original_node: cst.FunctionDef, updated_node: cst.FunctionDef
    ) -> cst.FunctionDef:
        new_decorators = [dec for dec in updated_node.decorators if not self._is_task_decorator(dec)]
        if len(new_decorators) == len(updated_node.decorators):
            return updated_node
        return updated_node.with_changes(decorators=new_decorators)


def remove_task_decorator(python_source: str, task_decorator_name: str) -> str:
    """
    Remove @task or similar decorators as well as @setup and @teardown.

    :param python_source: The python source code
    :param task_decorator_name: the decorator name
    """
    source_tree = cst.parse_module(python_source)
    modified_tree = source_tree.visit(_TaskDecoratorRemover(task_decorator_name))
    return modified_tree.code


class _autostacklevel_warn:
    def __init__(self, delta):
        self.warnings = __import__("warnings")
        self.delta = delta

    def __getattr__(self, name):
        return getattr(self.warnings, name)

    def __dir__(self):
        return dir(self.warnings)

    def warn(self, message, category=None, stacklevel=1, source=None):
        self.warnings.warn(message, category, stacklevel + self.delta, source)


def fixup_decorator_warning_stack(func, delta: int = 2):
    if func.__globals__.get("warnings") is sys.modules["warnings"]:
        # Yes, this is more than slightly hacky, but it _automatically_ sets the right stacklevel parameter to
        # `warnings.warn` to ignore the decorator.
        func.__globals__["warnings"] = _autostacklevel_warn(delta)
