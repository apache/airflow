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

TASKINSTANCE_PY = ROOT_DIR.joinpath("airflow", "models", "taskinstance.py")
CONTEXT_PY = ROOT_DIR.joinpath("airflow", "utils", "context.py")
CONTEXT_PYI = ROOT_DIR.joinpath("airflow", "utils", "context.pyi")
TEMPLATES_REF_RST = ROOT_DIR.joinpath("docs", "apache-airflow", "templates-ref.rst")


def _iter_template_context_keys_from_original_return() -> typing.Iterator[str]:
    ti_mod = ast.parse(TASKINSTANCE_PY.read_text("utf-8"), str(TASKINSTANCE_PY))
    fn_get_template_context = next(
        node
        for node in ast.iter_child_nodes(ti_mod)
        if isinstance(node, ast.FunctionDef) and node.name == "_get_template_context"
    )
    st_context_value = next(
        stmt.value
        for stmt in fn_get_template_context.body
        if isinstance(stmt, ast.AnnAssign)
        and isinstance(stmt.target, ast.Name)
        and stmt.target.id == "context"
    )
    if not isinstance(st_context_value, ast.Dict):
        raise ValueError("'context' is not assigned a dict literal")
    for expr in st_context_value.keys:
        if not isinstance(expr, ast.Constant) or not isinstance(expr.value, str):
            raise ValueError("key in 'context' dict is not a str literal")
        yield expr.value


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
    context_mod = ast.parse(CONTEXT_PYI.read_text("utf-8"), str(CONTEXT_PYI))
    cls_context = next(
        node
        for node in ast.iter_child_nodes(context_mod)
        if isinstance(node, ast.ClassDef) and node.name == "Context"
    )
    for stmt in cls_context.body:
        if not isinstance(stmt, ast.AnnAssign) or not isinstance(stmt.target, ast.Name):
            raise ValueError("key in 'Context' hint is not an annotated assignment")
        yield stmt.target.id


def _iter_template_context_keys_from_documentation() -> typing.Iterator[str]:
    # We can use docutils to actually parse, but regex is good enough for now.
    # This should find names in the "Variable" and "Deprecated Variable" tables.
    content = TEMPLATES_REF_RST.read_text("utf-8")
    for match in re.finditer(
        r"^``{{ (?P<name>\w+)(?P<subname>\.\w+)* }}`` ", content, re.MULTILINE
    ):
        yield match.group("name")


def _compare_keys(
    retn_keys: set[str], decl_keys: set[str], hint_keys: set[str], docs_keys: set[str]
) -> int:
    # Added by PythonOperator and commonly used.
    # Not listed in templates-ref (but in operator docs).
    retn_keys.add("templates_dict")
    docs_keys.add("templates_dict")

    # Only present in callbacks. Not listed in templates-ref (that doc is for task execution).
    retn_keys.update(("exception", "reason", "try_number"))
    docs_keys.update(("exception", "reason", "try_number"))

    check_candidates = [
        ("get_template_context()", retn_keys),
        ("KNOWN_CONTEXT_KEYS", decl_keys),
        ("Context type hint", hint_keys),
        ("templates-ref", docs_keys),
    ]
    canonical_keys = set.union(*(s for _, s in check_candidates))

    def _check_one(identifier: str, keys: set[str]) -> int:
        if missing := canonical_keys.difference(retn_keys):
            print(
                "Missing template variables from",
                f"{identifier}:",
                ", ".join(sorted(missing)),
            )
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
