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

DAG_PY = pathlib.Path(__file__).resolve().parents[3].joinpath("airflow", "models", "dag.py")


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


# The new unparse() output is much more readable; fallback to dump() otherwise.
if sys.version_info >= (3, 8):
    _reveal = ast.unparse  # type: ignore[attr-defined]
else:
    _reveal = ast.dump


def _match_arguments(init_args: list[ast.arg], deco_args: list[ast.arg]) -> collections.abc.Iterator[str]:
    for i, (ini, dec) in enumerate(itertools.zip_longest(init_args, deco_args, fillvalue=None)):
        if ini is None and dec is not None:
            yield f"Argument present in decorator but missing from DAG: {dec.arg}"
            return
        if dec is None and ini is not None:
            yield f"Argument present in DAG but missing from decorator: {ini.arg}"
            return
        assert ini is not None and dec is not None  # Because None is only possible as fillvalue.

        if ini.arg != dec.arg:
            yield f"Arguments at {i + 1} do not match: DAG has {ini.arg} but @dag has {dec.arg}"
            return

        if getattr(ini, "type_comment", None):  # 3.8+
            yield f"Do not use type comments on DAG argument: {ini.arg}"
        if getattr(dec, "type_comment", None):  # 3.8+
            yield f"Do not use type comments on @dag argument: {dec.arg}"

        # Poorly implemented node equality check.
        if ini.annotation and dec.annotation and ast.dump(ini.annotation) != ast.dump(dec.annotation):
            yield (
                f"Type annotations differ on argument {ini.arg}: "
                f"{_reveal(ini.annotation)} != {_reveal(dec.annotation)}"
            )
        else:
            if not ini.annotation:
                yield f"Type annotation missing on DAG argument: {ini.arg}"
            if not dec.annotation:
                yield f"Type annotation missing on @dag argument: {ini.arg}"


def _match_defaults(
    names: list[str],
    init_defaults: list[ast.expr],
    deco_defaults: list[ast.expr],
) -> collections.abc.Iterator[str]:
    for i, (ini, dec) in enumerate(zip(init_defaults, deco_defaults), 1):
        if ast.dump(ini) != ast.dump(dec):  # Poorly implemented equality check.
            yield f"Defaults differ on argument {names[i]!r}: {_reveal(ini)} != {_reveal(dec)}"


def check_dag_init_decorator_arguments() -> int:
    dag_mod = ast.parse(DAG_PY.read_text("utf-8"), str(DAG_PY))

    init = _find_dag_init(dag_mod)
    deco = _find_dag_deco(dag_mod)

    if getattr(init.args, "posonlyargs", None) or getattr(deco.args, "posonlyargs", None):
        print("DAG.__init__() and @dag() should not declare positional-only arguments")
        return -1
    if init.args.vararg or init.args.kwarg or deco.args.vararg or deco.args.kwarg:
        print("DAG.__init__() and @dag() should not declare *args and **kwargs")
        return -1

    # Feel free to change this and make some of the arguments keyword-only!
    if init.args.kwonlyargs or deco.args.kwonlyargs:
        print("DAG.__init__() and @dag() should not declare keyword-only arguments")
        return -2
    if init.args.kw_defaults or deco.args.kw_defaults:
        print("DAG.__init__() and @dag() should not declare keyword-only arguments")
        return -2

    init_arg_names = [a.arg for a in init.args.args]
    deco_arg_names = [a.arg for a in deco.args.args]

    if init_arg_names[0] != "self":
        print("First argument in DAG must be 'self'")
        return -3
    if init_arg_names[1] != "dag_id":
        print("Second argument in DAG must be 'dag_id'")
        return -3
    if deco_arg_names[0] != "dag_id":
        print("First argument in @dag must be 'dag_id'")
        return -3

    if len(init.args.defaults) != len(init_arg_names) - 2:
        print("All arguments on DAG except self and dag_id must have defaults")
        return -4
    if len(deco.args.defaults) != len(deco_arg_names):
        print("All arguments on @dag must have defaults")
        return -4
    if isinstance(deco.args.defaults[0], ast.Constant) and deco.args.defaults[0].value != "":
        print("Default dag_id on @dag must be an empty string")
        return -4

    errors = list(_match_arguments(init.args.args[1:], deco.args.args))
    if not errors:
        errors = list(_match_defaults(deco_arg_names, init.args.defaults, deco.args.defaults[1:]))

    for error in errors:
        print(error)
    return len(errors)


if __name__ == "__main__":
    sys.exit(check_dag_init_decorator_arguments())
