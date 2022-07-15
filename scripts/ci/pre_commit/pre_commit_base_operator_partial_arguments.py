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
import itertools
import pathlib
import sys
import typing

ROOT_DIR = pathlib.Path(__file__).resolve().parents[3]

BASEOPERATOR_PY = ROOT_DIR.joinpath("airflow", "models", "baseoperator.py")
MAPPEDOPERATOR_PY = ROOT_DIR.joinpath("airflow", "models", "mappedoperator.py")

IGNORED = {
    # These are only used in the worker and thus mappable.
    "do_xcom_push",
    "email_on_failure",
    "email_on_retry",
    "post_execute",
    "pre_execute",
    # Doesn't matter, not used anywhere.
    "default_args",
    # Deprecated and is aliased to max_active_tis_per_dag.
    "task_concurrency",
    # attrs internals.
    "HIDE_ATTRS_FROM_UI",
    # Only on BaseOperator.
    "_dag",
    "mapped_arguments_validated_by_init",
    "output",
    "partial",
    "shallow_copy_attrs",
    # Only on MappedOperator.
    "expand_input",
    "partial_kwargs",
    "validate_upstream_return_value",
}


BO_MOD = ast.parse(BASEOPERATOR_PY.read_text("utf-8"), str(BASEOPERATOR_PY))
MO_MOD = ast.parse(MAPPEDOPERATOR_PY.read_text("utf-8"), str(MAPPEDOPERATOR_PY))

BO_CLS = next(
    node
    for node in ast.iter_child_nodes(BO_MOD)
    if isinstance(node, ast.ClassDef) and node.name == "BaseOperator"
)
BO_INIT = next(
    node
    for node in ast.iter_child_nodes(BO_CLS)
    if isinstance(node, ast.FunctionDef) and node.name == "__init__"
)
BO_PARTIAL = next(
    node
    for node in ast.iter_child_nodes(BO_MOD)
    if isinstance(node, ast.FunctionDef) and node.name == "partial"
)
MO_CLS = next(
    node
    for node in ast.iter_child_nodes(MO_MOD)
    if isinstance(node, ast.ClassDef) and node.name == "MappedOperator"
)


def _compare(a: set[str], b: set[str], *, excludes: set[str]) -> tuple[set[str], set[str]]:
    only_in_a = {n for n in a if n not in b and n not in excludes and n[0] != "_"}
    only_in_b = {n for n in b if n not in a and n not in excludes and n[0] != "_"}
    return only_in_a, only_in_b


def _iter_arg_names(func: ast.FunctionDef) -> typing.Iterator[str]:
    func_args = func.args
    for arg in itertools.chain(func_args.args, getattr(func_args, "posonlyargs", ()), func_args.kwonlyargs):
        yield arg.arg


def check_baseoperator_partial_arguments() -> bool:
    only_in_init, only_in_partial = _compare(
        set(itertools.islice(_iter_arg_names(BO_INIT), 1, None)),
        set(itertools.islice(_iter_arg_names(BO_PARTIAL), 1, None)),
        excludes=IGNORED,
    )
    if only_in_init:
        print("Arguments in BaseOperator missing from partial():", ", ".join(sorted(only_in_init)))
    if only_in_partial:
        print("Arguments in partial() missing from BaseOperator:", ", ".join(sorted(only_in_partial)))
    if only_in_init or only_in_partial:
        return False
    return True


def _iter_assignment_to_self_attributes(targets: typing.Iterable[ast.expr]) -> typing.Iterator[str]:
    for t in targets:
        if isinstance(t, ast.Attribute) and isinstance(t.value, ast.Name) and t.value.id == "self":
            yield t.attr  # Something like "self.foo = ...".
            continue
        # Recursively visit nodes in unpacking assignments like "a, b = ...".
        yield from _iter_assignment_to_self_attributes(getattr(t, "elts", ()))


def _iter_assignment_targets(func: ast.FunctionDef) -> typing.Iterator[str]:
    for stmt in func.body:
        if isinstance(stmt, ast.AnnAssign):
            yield from _iter_assignment_to_self_attributes([stmt.target])
        elif isinstance(stmt, ast.Assign):
            yield from _iter_assignment_to_self_attributes(stmt.targets)


def _is_property(f: ast.FunctionDef) -> bool:
    if len(f.decorator_list) != 1:
        return False
    decorator = f.decorator_list[0]
    if not isinstance(decorator, ast.Name):
        return False
    return decorator.id == "property"


def _iter_member_names(klass: ast.ClassDef) -> typing.Iterator[str]:
    for node in ast.iter_child_nodes(klass):
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            yield node.target.id
        elif isinstance(node, ast.FunctionDef) and _is_property(node):
            yield node.name


def check_operator_member_parity() -> bool:
    only_in_base, only_in_mapped = _compare(
        set(itertools.chain(_iter_assignment_targets(BO_INIT), _iter_member_names(BO_CLS))),
        set(_iter_member_names(MO_CLS)),
        excludes=IGNORED,
    )
    if only_in_base:
        print("Members on BaseOperator missing from MappedOperator:", ", ".join(sorted(only_in_base)))
    if only_in_mapped:
        print("Members on MappedOperator missing from BaseOperator:", ", ".join(sorted(only_in_mapped)))
    if only_in_base or only_in_mapped:
        return False
    return True


if __name__ == "__main__":
    results = [
        check_baseoperator_partial_arguments(),
        check_operator_member_parity(),
    ]
    sys.exit(not all(results))
