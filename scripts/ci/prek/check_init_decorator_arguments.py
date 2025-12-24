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
import collections.abc
import itertools
import pathlib
import sys
from typing import TYPE_CHECKING

sys.path.insert(0, str(pathlib.Path(__file__).parent.resolve()))  # make sure common_prek_utils is imported
from common_prek_utils import AIRFLOW_CORE_SOURCES_PATH, AIRFLOW_TASK_SDK_SOURCES_PATH, console

SDK_DEFINITIONS_PKG = AIRFLOW_TASK_SDK_SOURCES_PATH / "airflow" / "sdk" / "definitions"
DAG_PY = SDK_DEFINITIONS_PKG / "dag.py"
TG_PY = SDK_DEFINITIONS_PKG / "taskgroup.py"
AIRFLOW_CORE_PACKAGE_ROOT = AIRFLOW_CORE_SOURCES_PATH / "airflow"
DECOS_TG_PY = SDK_DEFINITIONS_PKG / "decorators" / "task_group.py"


def _name(node: ast.expr) -> str:
    if not isinstance(node, ast.Name):
        raise TypeError("node was not an ast.Name node")
    return node.id


def _find_cls_attrs(
    mod: ast.Module, class_name: str, ignore: list[str] | None = None
) -> collections.abc.Iterable[ast.AnnAssign]:
    """Find the type-annotated/attrs properties in the body of the specified class."""
    dag_class = next(
        n for n in ast.iter_child_nodes(mod) if isinstance(n, ast.ClassDef) and n.name == class_name
    )

    ignore = ignore or []

    for node in ast.iter_child_nodes(dag_class):
        if not isinstance(node, ast.AnnAssign) or not node.annotation:
            continue

        # ClassVar[Any]
        if isinstance(node.annotation, ast.Subscript) and _name(node.annotation.value) == "ClassVar":
            continue

        # Skip private attrs fields, ones with `attrs.field(init=False)` kwargs
        if isinstance(node.value, ast.Call):
            # Lazy coding: since init=True is the default, we're just looking for the presence of the init
            # arg name
            if TYPE_CHECKING:
                assert isinstance(node.value.func, ast.Attribute)
            if (
                node.value.func.attr == "field"
                and _name(node.value.func.value) == "attrs"
                and any(arg.arg == "init" for arg in node.value.keywords)
            ):
                continue
        if _name(node.target) in ignore:
            continue

        # Attrs treats `_group_id: str` as `group_id` arg to __init__
        if _name(node.target).startswith("_"):
            node.target.id = node.target.id[1:]  # type: ignore[union-attr]
        yield node


def _find_dag_deco(mod: ast.Module) -> ast.FunctionDef:
    """Find definition of the ``@dag`` decorator."""
    # We now define the signature in a type checking block, the runtime impl uses **kwargs
    type_checking_blocks = (
        node
        for node in ast.iter_child_nodes(mod)
        if isinstance(node, ast.If) and node.test.id == "TYPE_CHECKING"  # type: ignore[attr-defined]
    )
    return next(
        n
        for n in itertools.chain.from_iterable(map(ast.iter_child_nodes, type_checking_blocks))
        if isinstance(n, ast.FunctionDef) and n.name == "dag" and n.args.args[0].arg != "func"
        # Ignore overload from `dag` decorator with `func` as first arg
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


# Hard-code some specific examples of allowable decorate type annotation -> class type annotation mappings
# where they don't match exactly


def _expr_to_ast_dump(expr: str) -> str:
    return ast.dump(ast.parse(expr).body[0].value)  # type: ignore[attr-defined]


ALLOWABLE_TYPE_ANNOTATIONS = {
    # Mapping of allowble Decorator type -> Class attribute type
    _expr_to_ast_dump("Collection[str] | None"): _expr_to_ast_dump("MutableSet[str]"),
    _expr_to_ast_dump("ParamsDict | dict[str, Any] | None"): _expr_to_ast_dump("ParamsDict"),
    # TODO: This one is legacy access control. Remove it in 3.0. RemovedInAirflow3Warning
    _expr_to_ast_dump(
        "dict[str, dict[str, Collection[str]]] | dict[str, Collection[str]] | None"
    ): _expr_to_ast_dump("dict[str, dict[str, Collection[str]]] | None"),
}


def _match_arguments(
    init_def: tuple[str, list[ast.AnnAssign]],
    deco_def: tuple[str, list[ast.arg]],
) -> collections.abc.Iterator[str]:
    init_name, init_args = init_def
    deco_name, deco_args = deco_def
    init_args.sort(key=lambda a: _name(a.target))
    deco_args.sort(key=lambda a: a.arg)
    for i, (ini, dec) in enumerate(itertools.zip_longest(init_args, deco_args, fillvalue=None)):
        if ini is None and dec is not None:
            yield f"[red]Argument present in @{deco_name} but missing from {init_name}: {dec.arg}"
            return
        if dec is None and ini is not None:
            yield f"[red]Argument present in {init_name} but missing from @{deco_name}: {_name(ini.target)}"
            return
        if TYPE_CHECKING:
            # Mypy can't work out that zip_longest means one of ini or dec must be non None
            assert ini is not None

        if not isinstance(ini.target, ast.Name):
            raise RuntimeError(f"Don't know how to examine {ast.unparse(ini)!r}")
        attr_name = _name(ini.target)

        if TYPE_CHECKING:
            assert ini is not None and dec is not None  # Because None is only possible as fillvalue.

        if attr_name != dec.arg:
            yield f"[red]Argument {i + 1} mismatch: {init_name} has {attr_name} but @{deco_name} has {dec.arg}"
            return

        if getattr(ini, "type_comment", None):  # 3.8+
            yield f"[red]Do not use type comments on {init_name} argument: {ini}"
        if getattr(dec, "type_comment", None):  # 3.8+
            yield f"[red]Do not use type comments on @{deco_name} argument: {dec.arg}"

        # Poorly implemented node equality check.
        if ini.annotation and dec.annotation:
            ini_anno = ast.dump(ini.annotation)
            dec_anno = ast.dump(dec.annotation)
            if (
                ini_anno != dec_anno
                # The decorator can have `| None` type in addaition to the base attribute
                and dec_anno != f"BinOp(left={ini_anno}, op=BitOr(), right=Constant(value=None))"
                and ALLOWABLE_TYPE_ANNOTATIONS.get(dec_anno) != ini_anno
            ):
                yield (
                    f"[red]Type annotations differ on argument {attr_name!r} between {init_name} and @{deco_name}: "
                    f"{ast.unparse(ini.annotation)} != {ast.unparse(dec.annotation)}"
                )
        elif not ini.annotation:
            yield f"[red]Type annotation missing on {init_name} argument: {attr_name}"
        elif not dec.annotation:
            yield f"[red]Type annotation missing on @{deco_name} argument: {attr_name}"


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
                f"[red]Argument {arg_names[i]!r} default mismatch: "
                f"{init_name} has {ast.unparse(ini)} but @{deco_name} has {ast.unparse(dec)}"
            )


def check_dag_init_decorator_arguments() -> int:
    dag_mod = ast.parse(DAG_PY.read_text("utf-8"), str(DAG_PY))
    tg_mod = ast.parse(TG_PY.read_text("utf-8"), str(TG_PY))
    decos_tg = ast.parse(DECOS_TG_PY.read_text("utf-8"), str(DECOS_TG_PY))

    items_to_check = [
        (
            "DAG",
            list(
                _find_cls_attrs(dag_mod, "DAG", ignore=["full_filepath", "task_group", "sla_miss_callback"])
            ),
            "dag",
            _find_dag_deco(dag_mod),
            "dag_id",
            "",
        ),
        (
            "TaskGroup",
            list(_find_cls_attrs(tg_mod, "TaskGroup")),
            "_task_group",
            _find_tg_deco(decos_tg),
            "group_id",
            None,
        ),
    ]

    for init_name, cls_attrs, deco_name, deco, id_arg, id_default in items_to_check:
        deco_arg_names = [a.arg for a in deco.args.args]

        if _name(cls_attrs[0].target) != id_arg:
            console.print(f"[red]First attribute in {init_name} must be {id_arg!r} (got {cls_attrs[0]!r})")
            return -3
        if deco_arg_names[0] != id_arg:
            console.print(
                f"[red]First argument in @{deco_name} must be {id_arg!r} (got {deco_arg_names[0]!r})"
            )
            return -3

        if len(deco.args.defaults) != len(deco_arg_names):
            console.print(f"[red]All arguments on @{deco_name} must have defaults")
            return -4
        if isinstance(deco.args.defaults[0], ast.Constant) and deco.args.defaults[0].value != id_default:
            console.print(f"[red]Default {id_arg} on @{deco_name} must be {id_default!r}")
            return -4

    errors = []
    for init_name, cls_attrs, deco_name, deco, _, _ in items_to_check:
        errors = list(
            _match_arguments((init_name, cls_attrs), (deco_name, deco.args.args + deco.args.kwonlyargs))
        )
        if errors:
            break

    for error in errors:
        console.print(error)
    return len(errors)


if __name__ == "__main__":
    sys.exit(check_dag_init_decorator_arguments())
