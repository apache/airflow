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
from __future__ import annotations

import ast
import difflib
import os
import sys
import textwrap
from _ast import AST, BitOr, USub
from enum import Enum
from pathlib import Path
from typing import Generator

import astor as astor
from rich.console import Console

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To execute this script, run ./{__file__} [FILE] ..."
    )

AIRFLOW_SOURCES_ROOT = Path(__file__).parents[3].resolve()
COMMON_SQL_ROOT = AIRFLOW_SOURCES_ROOT / "airflow" / "providers" / "common" / "sql"

COMMON_SQL_API_FILE = COMMON_SQL_ROOT / "api.txt"

COMMON_SQL_PACKAGE_PREFIX = "airflow.providers.common.sql."

console = Console(width=400, color_system="standard")


def should_be_excluded(parent: str, name: str):
    if parent == "airflow.providers.common.sql.operators.sql" and name == "_parse_boolean":
        # special exception for already "public" _parse_boolean in operators.sql submodule
        return False
    return name.startswith("_") and name != "__init__"


def recurse_node(node) -> str:
    if hasattr(node, "op"):
        if isinstance(node.op, BitOr):
            return recurse_node(node.left) + " | " + recurse_node(node.right)
        elif isinstance(node.op, USub):
            return f"-{recurse_node(node.operand)}"
        else:
            raise Exception(f"Unhandled op type {node}. Attributes: {node.__dict__}")
    elif hasattr(node, "id"):
        return str(node.id) if node.id is not None else "None"
    elif hasattr(node, "value"):
        if isinstance(node.value, AST):
            return recurse_node(node.value)
        return str(node.value) if node.value is not None else "None"
    elif hasattr(node, "slice"):
        return f"{node.value.id}[{recurse_node(node.slice)}]"
    elif hasattr(node, "elts"):
        return ",".join([recurse_node(el) for el in node.elts])
    elif hasattr(node, "s"):
        # Python 3.7 has legacy `_ast.Str` type that is deprecated in 3.8
        # We can remove it when 3.7 is not our default version
        return node.s
    elif hasattr(node, "n"):
        # Python 3.7 has legacy `_ast.Num` type that is deprecated in 3.8
        # We can remove it when 3.7 is not our default version
        return node.n
    raise Exception(f"I do not know how to handle node: {node}. Attributes: {node.__dict__}")


class ARGTYPE(Enum):
    REGULAR = ""
    POSITION_ONLY = "<POS-ONLY>"
    KEYWORD_ONLY = "<KW-ONLY>"


def arg_generator(args: ast.arguments) -> Generator[tuple[ast.arg, ast.expr | None, ARGTYPE], None, None]:
    if hasattr(args, "args"):
        for arg, def_arg in zip(args.args, args.defaults):
            yield arg, def_arg, ARGTYPE.REGULAR
    if hasattr(args, "posonlyargs"):
        # Python 3.8 introduced position-only args - until we switch to 3.8 we need to ignore typing issue
        # We will be able to remove the `type: ignore` when we switch to 3.8
        for arg in args.posonlyargs:  # type: ignore
            yield arg, None, ARGTYPE.POSITION_ONLY
    if hasattr(args, "kwonlyargs"):
        for arg, def_kwarg in zip(args.kwonlyargs, args.kw_defaults):
            yield arg, def_kwarg, ARGTYPE.KEYWORD_ONLY


class PublicUsageFinder(astor.TreeWalk):
    package_path: str
    functions: set[str] = set()
    classes: set[str] = set()
    methods: set[str] = set()
    current_class: str | None = None

    def get_current_node_args(self) -> str:
        result = ""
        separator = ""
        if hasattr(self.cur_node, "args"):
            for arg, default_value, arg_type in arg_generator(self.cur_node.args):
                if arg is not None:
                    result += f"{separator}{arg.arg}{arg_type.value}"
                    if hasattr(arg, "annotation") and arg.annotation is not None:
                        result += f": {recurse_node(arg.annotation)}"
                    if default_value:
                        result += f" = {recurse_node(default_value)}"
                    separator = ", "
        if self.cur_node.args.vararg:
            result += f"{separator}*args"
        if self.cur_node.args.kwarg:
            result += f"{separator}**kwargs"
        return result

    def get_current_return(self) -> str:
        if self.cur_node.returns:
            return f" -> {recurse_node(self.cur_node.returns)}"
        return ""

    def pre_ClassDef(self):
        class_name = f"{self.package_path}.{self.cur_node.name}"
        if not should_be_excluded(class_name, self.cur_node.name):
            self.classes.add(class_name)
        self.current_class = class_name

    def post_ClassDef(self):
        self.current_class = None

    def pre_FunctionDef(self):
        if not self.current_class:
            if not should_be_excluded(self.package_path, self.cur_node.name):
                self.functions.add(
                    f"{self.package_path}.{self.cur_node.name}("
                    f"{self.get_current_node_args()}){self.get_current_return()}"
                )
            return
        if not should_be_excluded(self.current_class, self.cur_node.name):
            self.methods.add(
                f"{self.current_class}.{self.cur_node.name}("
                f"{self.get_current_node_args()}){self.get_current_return()}"
            )


def get_current_apis_of_common_sql_provider() -> list[str]:
    """
    Extracts API from the common.sql provider.

    The file generated is a text file of all classes and methods that should be considered as "Public API"
    of the common.sql provider.

    All "users" of common.sql API are verified if they are only using those API.
    """
    results = set()
    all_common_sql_files = COMMON_SQL_ROOT.rglob("**/*.py")
    for module_source_path in all_common_sql_files:
        module = ast.parse(module_source_path.read_text("utf-8"), str(module_source_path))
        finder = PublicUsageFinder()
        finder.package_path = str(module_source_path.relative_to(AIRFLOW_SOURCES_ROOT)).replace(os.sep, ".")[
            :-3
        ]
        finder.walk(node=module)
        results.update(finder.classes)
        results.update(finder.methods)
        results.update(finder.functions)
    return list(sorted(results))


class ConsoleDiff(difflib.Differ):
    def _dump(self, tag, x, lo, hi):
        """Generate comparison results for a same-tagged range."""
        for i in range(lo, hi):
            if tag == "+":
                yield f"[green]{tag} {x[i]}[/]"
            elif tag == "-":
                yield f"[red]{tag} {x[i]}[/]"
            else:
                yield f"[white]{tag} {x[i]}[/]"


WHAT_TO_CHECK = """Those are the changes you should check:

    * for every removed method/class/function, check that is not used and that ALL ACTIVE PAST VERSIONS of
      released SQL providers do not use the method.

    * for every changed parameter of a method/function/class constructor, check that changes in the parameters
      are backwards compatible.
"""

PREAMBLE = """# This is a common.sql provider API dump
#
# This file is generated automatically by the update-common-sql-api pre-commit
# and it shows the current API of the common.sql provider
#
# Any, potentially breaking change in the API will require deliberate manual action from the contributor
# making a change to the common.sql provider. This file is also used in checking if only public API of
# the common.sql provider is used by all the other providers.
#
# You can read more in the README_API.md file
#
"""


def summarize_changes(results: list[str]) -> tuple[int, int]:
    """
    Returns summary of the changes.

    :param results: results of comparison in the form of line of strings
    :return: Tuple: [number of removed lines, number of added lines]
    """
    removals, additions = 0, 0
    for line in results:
        if line.startswith("+") or line.startswith("[green]+"):
            additions += 1
        if line.startswith("-") or line.startswith("[red]-"):
            removals += 1
    return removals, additions


if __name__ == "__main__":
    extracted_api_list = get_current_apis_of_common_sql_provider()
    # Exclude empty lines and comments
    current_api_list = [
        line
        for line in COMMON_SQL_API_FILE.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    perform_update = False
    if current_api_list != extracted_api_list:
        diff = ConsoleDiff()
        comparison_results = list(diff.compare(current_api_list, extracted_api_list))
        _removals, _additions = summarize_changes(comparison_results)
        console.print("[bright_blue]Summary of the changes in common.sql API that this change brings:[/]\n")
        console.print(textwrap.indent("\n".join(comparison_results), " " * 4))
        if _removals:
            if not os.environ.get("UPDATE_COMMON_SQL_API") == "1":
                console.print(
                    f"\n[red]ERROR! As you can see above, there are changes in the common.sql API:[/]\n\n"
                    f"[red]* additions: {_additions}[/]\n"
                    f"[red]* removals: {_removals}[/]\n"
                )
                console.print(
                    "[bright_blue]Make sure to review the removals and changes for back-compatibility.[/]\n"
                    "[bright_blue]If you are sure all the changes are justified, run:[/]"
                )
                console.print("\n[magenta]UPDATE_COMMON_SQL_API=1 pre-commit run update-common-sql-api[/]\n")
                console.print(WHAT_TO_CHECK)
                console.print("\n[yellow]Make sure to commit the changes after you updating the API.[/]")
            else:
                console.print(
                    f"\n[bright_blue]As you can see above, there are changes in the common.sql API:[/]\n\n"
                    f"[bright_blue]* additions: {_additions}[/]\n"
                    f"[bright_blue]* removals: {_removals}[/]\n"
                )
                console.print("[bright_blue]You've set UPDATE_COMMON_SQL_API to 1 to update the API.[/]\n\n")
                perform_update = True
        else:
            perform_update = True
            console.print(
                f"\n[yellow]There are only additions in the API extracted from the common.sql code[/]\n\n"
                f"[bright_blue]* additions: {_additions}[/]\n"
            )
        if perform_update:
            COMMON_SQL_API_FILE.write_text(PREAMBLE + "\n".join(extracted_api_list) + "\n")
            console.print(
                f"\n[bright_blue]The {COMMON_SQL_API_FILE} file is updated automatically.[/]\n"
                "\n[yellow]Make sure to commit the changes.[/]"
            )
        sys.exit(1)
    else:
        console.print("\n[green]All OK. The common.sql APIs did not change[/]")
