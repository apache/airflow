#!/usr/bin/env python
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

import ast
import pathlib
import re
import sys
import typing

ROOT_DIR = pathlib.Path(__file__).resolve().parents[3]

TASKRUNNER_PY = ROOT_DIR.joinpath("task_sdk", "src", "airflow", "sdk", "execution_time", "task_runner.py")
CONTEXT_PY = ROOT_DIR.joinpath("airflow", "utils", "context.py")
CONTEXT_HINT = ROOT_DIR.joinpath("task_sdk", "src", "airflow", "sdk", "definitions", "context.py")
TEMPLATES_REF_RST = ROOT_DIR.joinpath("docs", "apache-airflow", "templates-ref.rst")

# These are only conditionally set
IGNORE = {
    "ds",
    "ds_nodash",
    "ts",
    "ts_nodash",
    "ts_nodash_with_tz",
    "logical_date",
    "data_interval_end",
    "data_interval_start",
    "prev_data_interval_start_success",
    "prev_data_interval_end_success",
}


def _iter_template_context_keys_from_original_return() -> typing.Iterator[str]:
    ti_mod = ast.parse(TASKRUNNER_PY.read_text("utf-8"), str(TASKRUNNER_PY))

    # Locate the RuntimeTaskInstance class definition
    runtime_task_instance_class = next(
        node
        for node in ast.iter_child_nodes(ti_mod)
        if isinstance(node, ast.ClassDef) and node.name == "RuntimeTaskInstance"
    )

    # Locate the get_template_context method in RuntimeTaskInstance
    fn_get_template_context = next(
        node
        for node in ast.iter_child_nodes(runtime_task_instance_class)
        if isinstance(node, ast.FunctionDef) and node.name == "get_template_context"
    )

    # Helper function to extract keys from a dictionary node
    def extract_keys_from_dict(node: ast.Dict) -> typing.Iterator[str]:
        for key in node.keys:
            if not isinstance(key, ast.Constant) or not isinstance(key.value, str):
                raise ValueError("Key in dictionary is not a string literal")
            yield key.value

    # Extract keys from the main `context` dictionary assignment
    context_assignment = next(
        stmt
        for stmt in fn_get_template_context.body
        if isinstance(stmt, ast.AnnAssign)
        and isinstance(stmt.target, ast.Name)
        and stmt.target.id == "context"
    )

    if not isinstance(context_assignment.value, ast.Dict):
        raise ValueError("'context' is not assigned a dictionary literal")
    yield from extract_keys_from_dict(context_assignment.value)

    # Handle keys added conditionally in `if x := self._ti_context_from_server`
    for stmt in fn_get_template_context.body:
        if (
            isinstance(stmt, ast.If)
            and isinstance(stmt.test, ast.NamedExpr)
            and isinstance(stmt.test.value, ast.Attribute)
            and stmt.test.value.attr == "_ti_context_from_server"
        ):
            for sub_stmt in stmt.body:
                # Get keys from `context_from_server` assignment
                if (
                    isinstance(sub_stmt, ast.AnnAssign)
                    and isinstance(sub_stmt.target, ast.Name)
                    and isinstance(sub_stmt.value, ast.Dict)
                    and sub_stmt.target.id == "context_from_server"
                ):
                    yield from extract_keys_from_dict(sub_stmt.value)


def _iter_template_context_keys_from_declaration() -> typing.Iterator[str]:
    context_mod = ast.parse(CONTEXT_PY.read_text("utf-8"), str(CONTEXT_PY))
    st_known_context_keys = next(
        stmt.value
        for stmt in context_mod.body
        if isinstance(stmt, ast.AnnAssign)
        and isinstance(stmt.target, ast.Name)
        and stmt.target.id == "KNOWN_CONTEXT_KEYS"
    )
    if not isinstance(st_known_context_keys, ast.Set):
        raise ValueError("'KNOWN_CONTEXT_KEYS' is not assigned a set literal")
    for expr in st_known_context_keys.elts:
        if not isinstance(expr, ast.Constant) or not isinstance(expr.value, str):
            raise ValueError("item in 'KNOWN_CONTEXT_KEYS' set is not a str literal")
        yield expr.value


def _iter_template_context_keys_from_type_hints() -> typing.Iterator[str]:
    context_mod = ast.parse(CONTEXT_HINT.read_text("utf-8"), str(CONTEXT_HINT))
    cls_context = next(
        node
        for node in ast.iter_child_nodes(context_mod)
        if isinstance(node, ast.ClassDef) and node.name == "Context"
    )
    for stmt in cls_context.body:
        if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Constant):
            # Skip docstring
            continue
        if not isinstance(stmt, ast.AnnAssign) or not isinstance(stmt.target, ast.Name):
            raise ValueError("key in 'Context' hint is not an annotated assignment")
        yield stmt.target.id


def _iter_template_context_keys_from_documentation() -> typing.Iterator[str]:
    # We can use docutils to actually parse, but regex is good enough for now.
    # This should find names in the "Variable" and "Deprecated Variable" tables.
    content = TEMPLATES_REF_RST.read_text("utf-8")
    for match in re.finditer(r"^``{{ (?P<name>\w+)(?P<subname>\.\w+)* }}`` ", content, re.MULTILINE):
        yield match.group("name")


def _compare_keys(retn_keys: set[str], decl_keys: set[str], hint_keys: set[str], docs_keys: set[str]) -> int:
    # Added by PythonOperator and commonly used.
    # Not listed in templates-ref (but in operator docs).
    retn_keys.add("templates_dict")
    docs_keys.add("templates_dict")

    # Compat shim for task-sdk, not actually designed for user use
    retn_keys.add("expanded_ti_count")

    # TODO: These are the keys that are yet to be ported over to the Task SDK.
    retn_keys.add("inlet_events")
    retn_keys.add("params")
    retn_keys.add("test_mode")
    retn_keys.add("triggering_asset_events")

    # Only present in callbacks. Not listed in templates-ref (that doc is for task execution).
    retn_keys.update(("exception", "reason", "try_number"))
    docs_keys.update(("exception", "reason", "try_number"))

    # Airflow 3 added:
    retn_keys.update(("start_date", "task_reschedule_count"))

    check_candidates = [
        ("get_template_context()", retn_keys),
        ("KNOWN_CONTEXT_KEYS", decl_keys),
        ("Context type hint", hint_keys),
        ("templates-ref", docs_keys),
    ]
    canonical_keys = set.union(*(s for _, s in check_candidates)) - IGNORE

    def _check_one(identifier: str, keys: set[str]) -> int:
        if missing := canonical_keys.difference(keys):
            print("Missing template variables from", f"{identifier}:", ", ".join(sorted(missing)))
        return len(missing)

    return sum(_check_one(identifier, keys) for identifier, keys in check_candidates)


def main() -> str | int | None:
    retn_keys = set(_iter_template_context_keys_from_original_return())
    decl_keys = set(_iter_template_context_keys_from_declaration())
    hint_keys = set(_iter_template_context_keys_from_type_hints())
    docs_keys = set(_iter_template_context_keys_from_documentation())
    return _compare_keys(retn_keys, decl_keys, hint_keys, docs_keys)


if __name__ == "__main__":
    sys.exit(main())
