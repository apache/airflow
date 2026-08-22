#!/usr/bin/env python
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
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
"""Check that RuntimeTaskInstanceProtocol stays in sync with RuntimeTaskInstance.

Parses both the files via AST and checks for:
  1. Public members declared in RuntimeTaskInstance that are absent from
     RuntimeTaskInstanceProtocol aka incomplete protocol definition.
  2. Methods present in both classes whose positional parameter names differ.
"""

from __future__ import annotations

import ast
import dataclasses
import sys
from pathlib import Path

from common_prek_utils import AIRFLOW_TASK_SDK_SOURCES_PATH, console

IMPL_PATH = AIRFLOW_TASK_SDK_SOURCES_PATH / "airflow" / "sdk" / "execution_time" / "task_runner.py"
PROTOCOL_PATH = AIRFLOW_TASK_SDK_SOURCES_PATH / "airflow" / "sdk" / "types.py"

IMPL_CLASS = "RuntimeTaskInstance"
PROTOCOL_CLASS = "RuntimeTaskInstanceProtocol"

# Members present in RuntimeTaskInstance but intentionally absent from the Protocol.
# Each entry must carry a comment explaining why it is excluded.
IMPLEMENTATION_ONLY: set[str] = {
    # Worker internal: the DAG bundle used
    "bundle_instance",
    # Observability / Sentry integration string.
    "sentry_integration",
    # Internal helper for scheduler-side upstream map index resolution; not
    # part of the public task execution interface.
    "get_relevant_upstream_map_indexes",
}


@dataclasses.dataclass
class ClassMembers:
    """Public members declared directly in a class body."""

    fields: set[str] = dataclasses.field(default_factory=set)
    # method name mapped to list of positional param names
    methods: dict[str, list[str]] = dataclasses.field(default_factory=dict)
    properties: set[str] = dataclasses.field(default_factory=set)

    @property
    def all_names(self) -> set[str]:
        return self.fields | set(self.methods) | self.properties


def _is_property(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    for dec in node.decorator_list:
        if isinstance(dec, ast.Name) and dec.id == "property":
            return True
        if isinstance(dec, ast.Attribute) and dec.attr == "property":
            return True
    return False


def _is_public(name: str) -> bool:
    return not name.startswith("_")


def extract_class_members(source_path: Path, class_name: str) -> ClassMembers:
    tree = ast.parse(source_path.read_text(), filename=str(source_path))
    members = ClassMembers()

    for node in ast.walk(tree):
        if not (isinstance(node, ast.ClassDef) and node.name == class_name):
            continue
        for item in node.body:
            if isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name):
                name = item.target.id
                if _is_public(name):
                    members.fields.add(name)
            elif isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                name = item.name
                if not _is_public(name):
                    continue
                if _is_property(item):
                    members.properties.add(name)
                else:
                    params = [a.arg for a in item.args.args if a.arg != "self"]
                    members.methods[name] = params
        break

    return members


def check_sync(impl_path: Path, protocol_path: Path) -> bool:
    impl = extract_class_members(impl_path, IMPL_CLASS)
    proto = extract_class_members(protocol_path, PROTOCOL_CLASS)

    errors: list[str] = []

    # First check for members present in the RuntimeTaskInstance implementation but missing from the Protocol
    for field in sorted(impl.fields - IMPLEMENTATION_ONLY):
        if field not in proto.all_names:
            errors.append(f"  Field '{field}' is in {IMPL_CLASS} but missing from {PROTOCOL_CLASS}.")

    for prop in sorted(impl.properties - IMPLEMENTATION_ONLY):
        if prop not in proto.all_names:
            errors.append(f"  Property '{prop}' is in {IMPL_CLASS} but missing from {PROTOCOL_CLASS}.")

    for method in sorted(impl.methods.keys() - IMPLEMENTATION_ONLY):
        if method not in proto.all_names:
            errors.append(f"  Method '{method}' is in {IMPL_CLASS} but missing from {PROTOCOL_CLASS}.")

    # Check for methods that are present in both classes but have different positional parameter names
    for method, impl_params in impl.methods.items():
        if method in IMPLEMENTATION_ONLY:
            continue
        if method not in proto.methods:
            continue
        proto_params = proto.methods[method]
        if impl_params != proto_params:
            errors.append(
                f"  Method '{method}' parameter mismatch:\n"
                f"    {IMPL_CLASS}:         ({', '.join(impl_params)})\n"
                f"    {PROTOCOL_CLASS}: ({', '.join(proto_params)})"
            )

    if not errors:
        return True

    console.print(
        f"[bold red]{PROTOCOL_CLASS}[/bold red] has drifted from [bold red]{IMPL_CLASS}[/bold red]:\n"
    )
    for err in errors:
        console.print(err)
    console.print(
        f"\nUpdate [bold]{PROTOCOL_PATH.relative_to(AIRFLOW_TASK_SDK_SOURCES_PATH.parent.parent)}[/bold] "
        "to bring the Protocol back in sync with the implementation.\n"
        f"If a member is intentionally absent, add it to IMPLEMENTATION_ONLY in "
        f"[bold]scripts/ci/prek/check_runtime_ti_protocol_sync.py[/bold] with a comment."
    )
    return False


if __name__ == "__main__":
    if not check_sync(IMPL_PATH, PROTOCOL_PATH):
        sys.exit(1)
