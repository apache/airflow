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


def license_text() -> str:
    license = (
        Path(__file__).parents[1].joinpath("scripts", "ci", "license-templates", "LICENSE.txt").read_text()
    )

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

        source_tree = cst.parse_module(code)
        modified_tree = source_tree.visit(JsonValueNodeRemover())
        code = modified_tree.code

        result = subprocess.check_output(
            ["ruff", "check", "--fix-only", "--unsafe-fixes", "--quiet", "--preview", "-"],
            input=code,
            text=True,
        )

        return result
