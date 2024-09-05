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
import glob
import itertools
import os
import sys
from typing import Iterator

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir))

DEFERRABLE_DOC = (
    "https://github.com/apache/airflow/blob/main/docs/apache-airflow/"
    "authoring-and-scheduling/deferring.rst#writing-deferrable-operators"
)


def _is_valid_deferrable_default(default: ast.AST) -> bool:
    """Check whether default is 'conf.getboolean("operators", "default_deferrable", fallback=False)'"""
    if not isinstance(default, ast.Call):
        return False  # Not a function call.

    # Check the function callee is exactly 'conf.getboolean'.
    call_to_conf_getboolean = (
        isinstance(default.func, ast.Attribute)
        and isinstance(default.func.value, ast.Name)
        and default.func.value.id == "conf"
        and default.func.attr == "getboolean"
    )
    if not call_to_conf_getboolean:
        return False

    # Check arguments.
    return (
        len(default.args) == 2
        and isinstance(default.args[0], ast.Constant)
        and default.args[0].value == "operators"
        and isinstance(default.args[1], ast.Constant)
        and default.args[1].value == "default_deferrable"
        and len(default.keywords) == 1
        and default.keywords[0].arg == "fallback"
        and isinstance(default.keywords[0].value, ast.Constant)
        and default.keywords[0].value.value is False
    )


def iter_check_deferrable_default_errors(module_filename: str) -> Iterator[str]:
    ast_obj = ast.parse(open(module_filename).read())
    cls_nodes = (node for node in ast.iter_child_nodes(ast_obj) if isinstance(node, ast.ClassDef))
    init_method_nodes = (
        node
        for cls_node in cls_nodes
        for node in ast.iter_child_nodes(cls_node)
        if isinstance(node, ast.FunctionDef) and node.name == "__init__"
    )

    for node in init_method_nodes:
        args = node.args
        arguments = reversed([*args.args, *args.posonlyargs, *args.kwonlyargs])
        defaults = reversed([*args.defaults, *args.kw_defaults])
        for argument, default in itertools.zip_longest(arguments, defaults):
            # argument is not deferrable
            if argument is None or argument.arg != "deferrable":
                continue

            # argument is deferrable, but comes with no default value
            if default is None:
                yield f"{module_filename}:{argument.lineno}"
                continue

            # argument is deferrable, but the default value is not valid
            if not _is_valid_deferrable_default(default):
                yield f"{module_filename}:{default.lineno}"


def main() -> int:
    modules = itertools.chain(
        glob.glob(f"{ROOT_DIR}/airflow/**/sensors/**.py", recursive=True),
        glob.glob(f"{ROOT_DIR}/airflow/**/operators/**.py", recursive=True),
    )

    errors = [error for module in modules for error in iter_check_deferrable_default_errors(module)]
    if errors:
        print("Incorrect deferrable default values detected at:")
        for error in errors:
            print(f"  {error}")
        print(
            """Please set the default value of deferrbale to """
            """"conf.getboolean("operators", "default_deferrable", fallback=False)"\n"""
            f"See: {DEFERRABLE_DOC}\n"
        )

    return len(errors)


if __name__ == "__main__":
    sys.exit(main())
