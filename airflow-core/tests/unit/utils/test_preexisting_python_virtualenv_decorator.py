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

from textwrap import dedent

import libcst as cst
import pytest


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


class TestExternalPythonDecorator:
    @pytest.mark.parametrize(
        "decorators, expected_decorators",
        [
            (["@task.external_python"], []),
            (["@task.external_python()"], []),
            (['@task.external_python(serializer="dill")'], []),
            (["@foo", "@task.external_python", "@bar"], ["@foo", "@bar"]),
            (["@foo", "@task.external_python()", "@bar"], ["@foo", "@bar"]),
        ],
        ids=["without_parens", "parens", "with_args", "nested_without_parens", "nested_with_parens"],
    )
    def test_remove_task_decorator(self, decorators: list[str], expected_decorators: list[str]):
        concated_decorators = "\n".join(decorators)
        expected_decorator = "\n".join(expected_decorators)
        SCRIPT = dedent(
            """
        def f():
            import funcsigs
        """
        )
        py_source = concated_decorators + SCRIPT
        expected_source = expected_decorator + SCRIPT if expected_decorator else SCRIPT.lstrip()

        res = remove_task_decorator(python_source=py_source, task_decorator_name="@task.external_python")
        assert res == expected_source
