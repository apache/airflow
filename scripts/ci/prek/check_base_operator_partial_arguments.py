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
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
from __future__ import annotations

import ast
import itertools
import pathlib
import sys
import typing

sys.path.insert(0, str(pathlib.Path(__file__).parent.resolve()))  # make sure common_prek_utils is imported
from common_prek_utils import AIRFLOW_TASK_SDK_SOURCES_PATH, console

SDK_BASEOPERATOR_PY = AIRFLOW_TASK_SDK_SOURCES_PATH / "airflow" / "sdk" / "bases" / "operator.py"
SDK_MAPPEDOPERATOR_PY = (
    AIRFLOW_TASK_SDK_SOURCES_PATH / "airflow" / "sdk" / "definitions" / "mappedoperator.py"
)

IGNORED = {
    # These are only used in the worker and thus mappable.
    "do_xcom_push",
    "email_on_failure",
    "email_on_retry",
    "post_execute",
    "pre_execute",
    "multiple_outputs",
    "overwrite_rtif_after_execution",
    # Doesn't matter, not used anywhere.
    "default_args",
    # Deprecated and is aliased to max_active_tis_per_dag.
    "task_concurrency",
    # attrs internals.
    "HIDE_ATTRS_FROM_UI",
    # Only on BaseOperator.
    "_dag",
    "output",
    "partial",
    "shallow_copy_attrs",
    # Only on MappedOperator.
    "expand_input",
    "partial_kwargs",
    "operator_class",
    # Task-SDK migration ones.
    "deps",
    "downstream_task_ids",
    "operator_extra_links",
    "start_from_trigger",
    "start_trigger_args",
    "upstream_task_ids",
    "logger_name",
    "sla",
}


SDK_BO_MOD = ast.parse(SDK_BASEOPERATOR_PY.read_text("utf-8"), str(SDK_BASEOPERATOR_PY))
SDK_MO_MOD = ast.parse(SDK_MAPPEDOPERATOR_PY.read_text("utf-8"), str(SDK_MAPPEDOPERATOR_PY))

SDK_BO_CLS = next(
    node
    for node in ast.iter_child_nodes(SDK_BO_MOD)
    if isinstance(node, ast.ClassDef) and node.name == "BaseOperator"
)
SDK_BO_INIT = next(
    node
    for node in ast.iter_child_nodes(SDK_BO_CLS)
    if isinstance(node, ast.FunctionDef) and node.name == "__init__"
)

# We now define the signature in a type checking block, the runtime impl uses **kwargs
SDK_BO_TYPE_CHECKING_BLOCKS = (
    node
    for node in ast.iter_child_nodes(SDK_BO_MOD)
    if isinstance(node, ast.If) and node.test.id == "TYPE_CHECKING"  # type: ignore[attr-defined]
)
BO_PARTIAL = next(
    node
    for node in itertools.chain.from_iterable(map(ast.iter_child_nodes, SDK_BO_TYPE_CHECKING_BLOCKS))
    if isinstance(node, ast.FunctionDef) and node.name == "partial"
)
MO_CLS = next(
    node
    for node in ast.iter_child_nodes(SDK_MO_MOD)
    if isinstance(node, ast.ClassDef) and node.name == "MappedOperator"
)


def _compare(a: set[str], b: set[str]) -> tuple[set[str], set[str]]:
    only_in_a = a - b - IGNORED
    only_in_b = b - a - IGNORED
    return only_in_a, only_in_b


def _iter_arg_names(func: ast.FunctionDef) -> typing.Iterator[str]:
    func_args = func.args
    for arg in itertools.chain(func_args.args, getattr(func_args, "posonlyargs", ()), func_args.kwonlyargs):
        if arg.arg == "self" or arg.arg.startswith("_"):
            continue
        yield arg.arg


def check_baseoperator_partial_arguments() -> bool:
    only_in_init, only_in_partial = _compare(
        set(_iter_arg_names(SDK_BO_INIT)),
        set(_iter_arg_names(BO_PARTIAL)),
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
            if t.attr.startswith("_"):
                continue
            yield t.attr  # Something like "self.foo = ...".
        else:
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
    return isinstance(decorator, ast.Name) and decorator.id == "property"


def _iter_member_names(klass: ast.ClassDef) -> typing.Iterator[str]:
    for node in ast.iter_child_nodes(klass):
        name = ""
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            name = node.target.id
        elif isinstance(node, ast.FunctionDef) and _is_property(node):
            name = node.name
        elif isinstance(node, ast.Assign):
            if len(node.targets) == 1 and isinstance(target := node.targets[0], ast.Name):
                name = target.id
        else:
            continue
        if not name.startswith("_"):
            yield name


def check_operator_member_parity() -> bool:
    only_in_base, only_in_mapped = _compare(
        set(itertools.chain(_iter_assignment_targets(SDK_BO_INIT), _iter_member_names(SDK_BO_CLS))),
        set(_iter_member_names(MO_CLS)),
    )
    if only_in_base:
        console.print("[red]Members on BaseOperator missing from MappedOperator:")
        console.print(sorted(only_in_base))
    if only_in_mapped:
        console.print("[red]Members on MappedOperator missing from BaseOperator:")
        console.print(sorted(only_in_mapped))
    if only_in_base or only_in_mapped:
        return False
    return True


if __name__ == "__main__":
    results = [
        check_baseoperator_partial_arguments(),
        check_operator_member_parity(),
    ]
    sys.exit(not all(results))
