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
import collections.abc
import itertools
import pathlib
import sys

PACKAGE_ROOT = pathlib.Path(__file__).resolve().parents[3].joinpath("airflow")
DAG_PY = PACKAGE_ROOT.joinpath("models", "dag.py")
UTILS_TG_PY = PACKAGE_ROOT.joinpath("utils", "task_group.py")
DECOS_TG_PY = PACKAGE_ROOT.joinpath("decorators", "task_group.py")


def _find_dag_init(mod: ast.Module) -> ast.FunctionDef:
    """Find definition of the ``DAG`` class's ``__init__``."""
    dag_class = next(n for n in ast.iter_child_nodes(mod) if isinstance(n, ast.ClassDef) and n.name == "DAG")
    return next(
        node
        for node in ast.iter_child_nodes(dag_class)
        if isinstance(node, ast.FunctionDef) and node.name == "__init__"
    )


def _find_dag_deco(mod: ast.Module) -> ast.FunctionDef:
    """Find definition of the ``@dag`` decorator."""
    return next(n for n in ast.iter_child_nodes(mod) if isinstance(n, ast.FunctionDef) and n.name == "dag")


def _find_tg_init(mod: ast.Module) -> ast.FunctionDef:
    """Find definition of the ``TaskGroup`` class's ``__init__``."""
    task_group_class = next(
        node
        for node in ast.iter_child_nodes(mod)
        if isinstance(node, ast.ClassDef) and node.name == "TaskGroup"
    )
    return next(
        node
        for node in ast.iter_child_nodes(task_group_class)
        if isinstance(node, ast.FunctionDef) and node.name == "__init__"
    )


def _find_tg_deco(mod: ast.Module) -> ast.FunctionDef:
    """Find definition of the ``@task_group`` decorator.

    The decorator has multiple overloads, but we want the first one, which
    contains task group init arguments.
    """
    return next(
        node
        for node in ast.iter_child_nodes(mod)
        if isinstance(node, ast.FunctionDef) and node.name == "task_group"
    )


# The new unparse() output is much more readable; fallback to dump() otherwise.
if sys.version_info >= (3, 8):
    _reveal = ast.unparse  # type: ignore[attr-defined]
else:
    _reveal = ast.dump


def _match_arguments(
    init_def: tuple[str, list[ast.arg]],
    deco_def: tuple[str, list[ast.arg]],
) -> collections.abc.Iterator[str]:
    init_name, init_args = init_def
    deco_name, deco_args = deco_def
    for i, (ini, dec) in enumerate(itertools.zip_longest(init_args, deco_args, fillvalue=None)):
        if ini is None and dec is not None:
            yield f"Argument present in @{deco_name} but missing from {init_name}: {dec.arg}"
            return
        if dec is None and ini is not None:
            yield f"Argument present in {init_name} but missing from @{deco_name}: {ini.arg}"
            return
        assert ini is not None and dec is not None  # Because None is only possible as fillvalue.

        if ini.arg != dec.arg:
            yield f"Argument {i + 1} mismatch: {init_name} has {ini.arg} but @{deco_name} has {dec.arg}"
            return

        if getattr(ini, "type_comment", None):  # 3.8+
            yield f"Do not use type comments on {init_name} argument: {ini.arg}"
        if getattr(dec, "type_comment", None):  # 3.8+
            yield f"Do not use type comments on @{deco_name} argument: {dec.arg}"

        # Poorly implemented node equality check.
        if ini.annotation and dec.annotation and ast.dump(ini.annotation) != ast.dump(dec.annotation):
            yield (
                f"Type annotations differ on argument {ini.arg} between {init_name} and @{deco_name}: "
                f"{_reveal(ini.annotation)} != {_reveal(dec.annotation)}"
            )
        else:
            if not ini.annotation:
                yield f"Type annotation missing on {init_name} argument: {ini.arg}"
            if not dec.annotation:
                yield f"Type annotation missing on @{deco_name} argument: {ini.arg}"


def _match_defaults(
    arg_names: list[str],
    init_def: tuple[str, list[ast.expr]],
    deco_def: tuple[str, list[ast.expr]],
) -> collections.abc.Iterator[str]:
    init_name, init_defaults = init_def
    deco_name, deco_defaults = deco_def
    for i, (ini, dec) in enumerate(zip(init_defaults, deco_defaults), 1):
        if ast.dump(ini) != ast.dump(dec):  # Poorly implemented equality check.
            yield (
                f"Argument {arg_names[i]!r} default mismatch: "
                f"{init_name} has {_reveal(ini)} but @{deco_name} has {_reveal(dec)}"
            )


def check_dag_init_decorator_arguments() -> int:
    dag_mod = ast.parse(DAG_PY.read_text("utf-8"), str(DAG_PY))

    utils_tg = ast.parse(UTILS_TG_PY.read_text("utf-8"), str(UTILS_TG_PY))
    decos_tg = ast.parse(DECOS_TG_PY.read_text("utf-8"), str(DECOS_TG_PY))

    items_to_check = [
        ("DAG", _find_dag_init(dag_mod), "dag", _find_dag_deco(dag_mod), "dag_id", ""),
        ("TaskGroup", _find_tg_init(utils_tg), "task_group", _find_tg_deco(decos_tg), "group_id", None),
    ]

    for init_name, init, deco_name, deco, id_arg, id_default in items_to_check:
        if getattr(init.args, "posonlyargs", None) or getattr(deco.args, "posonlyargs", None):
            print(f"{init_name} and @{deco_name} should not declare positional-only arguments")
            return -1
        if init.args.vararg or init.args.kwarg or deco.args.vararg or deco.args.kwarg:
            print(f"{init_name} and @{deco_name} should not declare *args and **kwargs")
            return -1

        # Feel free to change this and make some of the arguments keyword-only!
        if init.args.kwonlyargs or deco.args.kwonlyargs:
            print(f"{init_name}() and @{deco_name}() should not declare keyword-only arguments")
            return -2
        if init.args.kw_defaults or deco.args.kw_defaults:
            print(f"{init_name}() and @{deco_name}() should not declare keyword-only arguments")
            return -2

        init_arg_names = [a.arg for a in init.args.args]
        deco_arg_names = [a.arg for a in deco.args.args]

        if init_arg_names[0] != "self":
            print(f"First argument in {init_name} must be 'self'")
            return -3
        if init_arg_names[1] != id_arg:
            print(f"Second argument in {init_name} must be {id_arg!r}")
            return -3
        if deco_arg_names[0] != id_arg:
            print(f"First argument in @{deco_name} must be {id_arg!r}")
            return -3

        if len(init.args.defaults) != len(init_arg_names) - 2:
            print(f"All arguments on {init_name} except self and {id_arg} must have defaults")
            return -4
        if len(deco.args.defaults) != len(deco_arg_names):
            print(f"All arguments on @{deco_name} must have defaults")
            return -4
        if isinstance(deco.args.defaults[0], ast.Constant) and deco.args.defaults[0].value != id_default:
            print(f"Default {id_arg} on @{deco_name} must be {id_default!r}")
            return -4

    for init_name, init, deco_name, deco, _, _ in items_to_check:
        errors = list(_match_arguments((init_name, init.args.args[1:]), (deco_name, deco.args.args)))
        if errors:
            break
        init_defaults_def = (init_name, init.args.defaults)
        deco_defaults_def = (deco_name, deco.args.defaults[1:])
        errors = list(_match_defaults(deco_arg_names, init_defaults_def, deco_defaults_def))
        if errors:
            break

    for error in errors:
        print(error)
    return len(errors)


if __name__ == "__main__":
    sys.exit(check_dag_init_decorator_arguments())
