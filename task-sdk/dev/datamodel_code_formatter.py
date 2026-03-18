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

import subprocess
from pathlib import Path

import libcst as cst
from datamodel_code_generator.format import CustomCodeFormatter
from libcst.helpers import parse_template_statement

AIRFLOW_ROOT_PATH = Path(__file__).parents[2].resolve()


def license_text() -> str:
    license = (AIRFLOW_ROOT_PATH / "scripts" / "ci" / "license-templates" / "LICENSE.txt").read_text()
    return "\n".join(f"# {line}" if line else "#" for line in license.splitlines()) + "\n"


class CodeFormatter(CustomCodeFormatter):
    def apply(self, code: str) -> str:
        code = license_text() + code

        # Swap "class JsonValue[RootValue]:" for the import from pydantic
        class JsonValueNodeRemover(cst.CSTTransformer):
            def leave_ImportFrom(
                self, original_node: cst.ImportFrom, updated_node: cst.ImportFrom
            ) -> cst.BaseSmallStatement | cst.FlattenSentinel[cst.BaseSmallStatement] | cst.RemovalSentinel:
                if original_node.module and original_node.module.value == "pydantic":
                    new_names = updated_node.names + (cst.ImportAlias(name=cst.Name("JsonValue")),)  # type: ignore[operator]
                    return updated_node.with_changes(names=new_names)
                return super().leave_ImportFrom(original_node, updated_node)

            def leave_ClassDef(
                self, original_node: cst.ClassDef, updated_node: cst.ClassDef
            ) -> cst.BaseStatement | cst.FlattenSentinel[cst.BaseStatement] | cst.RemovalSentinel:
                if original_node.name.value == "JsonValue":
                    return cst.RemoveFromParent()
                return super().leave_ClassDef(original_node, updated_node)

        class VersionConstInjtector(cst.CSTTransformer):
            handled = False

            def __init__(self, api_version: str) -> None:
                self.api_version = api_version
                super().__init__()

            def leave_ImportFrom(
                self, original_node: cst.ImportFrom, updated_node: cst.ImportFrom
            ) -> cst.BaseSmallStatement | cst.FlattenSentinel[cst.BaseSmallStatement] | cst.RemovalSentinel:
                # Ensure we have `from typing import Final`
                if original_node.module and original_node.module.value == "typing":
                    new_names = updated_node.names + (cst.ImportAlias(name=cst.Name("Final")),)  # type: ignore[operator]
                    return updated_node.with_changes(names=new_names)
                return super().leave_ImportFrom(original_node, updated_node)

            def leave_ClassDef(self, original_node: cst.ClassDef, updated_node: cst.ClassDef):
                if self.handled:
                    return super().leave_ClassDef(original_node, updated_node)

                self.handled = True

                const = parse_template_statement(
                    "API_VERSION: Final[str] = {api_version}",
                    api_version=cst.SimpleString(f'"{self.api_version}"'),
                )
                return cst.FlattenSentinel([const, updated_node])

        # Remove Task class that represent a tuple of (task_id, map_index)
        # for `TISkippedDownstreamTasksStatePayload`
        class ModifyTasksAnnotation(cst.CSTTransformer):
            def leave_ClassDef(
                self, original_node: cst.ClassDef, updated_node: cst.ClassDef
            ) -> cst.BaseStatement | cst.FlattenSentinel[cst.BaseStatement] | cst.RemovalSentinel:
                if original_node.name.value == "Tasks":
                    return cst.RemoveFromParent()
                return super().leave_ClassDef(original_node, updated_node)

            def leave_AnnAssign(
                self, original_node: cst.AnnAssign, updated_node: cst.AnnAssign
            ) -> cst.AnnAssign | cst.RemovalSentinel:
                """
                Replaces `tasks: Annotated[list[str | Tasks], Field(title="Tasks")]`
                with `tasks: Annotated[list[str | tuple[str, int]], Field(title="Tasks")]`
                only if inside `TISkippedDownstreamTasksStatePayload`.
                """
                # Check if the target is 'tasks'
                if not isinstance(updated_node.target, cst.Name) or updated_node.target.value != "tasks":
                    return updated_node

                if not isinstance(updated_node.annotation, cst.Annotation):
                    return updated_node

                # Create a replacement for 'Tasks' -> 'tuple[str, int]'
                tuple_type = cst.Subscript(
                    value=cst.Name("tuple"),
                    slice=[
                        cst.SubscriptElement(cst.Index(cst.Name("str"))),
                        cst.SubscriptElement(cst.Index(cst.Name("int"))),
                    ],
                )

                # Transformer to replace all instances of 'Tasks' with 'tuple[str, int]'
                class TasksReplacer(cst.CSTTransformer):
                    def leave_Name(
                        self, original_node: cst.Name, updated_node: cst.Name
                    ) -> cst.BaseExpression:
                        if original_node.value == "Tasks":
                            return tuple_type
                        return updated_node

                # Apply the transformation to the annotation part only
                new_annotation = updated_node.annotation.visit(TasksReplacer())
                return updated_node.with_changes(annotation=new_annotation)

        source_tree = cst.parse_module(code)
        modified_tree = source_tree.visit(JsonValueNodeRemover())
        if api_version := self.formatter_kwargs.get("api_version"):
            modified_tree = modified_tree.visit(VersionConstInjtector(api_version))
        modified_tree = modified_tree.visit(ModifyTasksAnnotation())
        code = modified_tree.code

        result = subprocess.check_output(
            ["ruff", "check", "--fix-only", "--unsafe-fixes", "--quiet", "--preview", "-"],
            input=code,
            text=True,
        )

        return result
