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

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir))


def _is_valid_deferrable_default(default: ast.AST) -> bool:
    """Check whether default is 'conf.getboolean("operators", "default_deferrable", fallback=False)'"""

    return (
        isinstance(default, ast.Call)
        and default.func.value.id == "conf"  # type: ignore[attr-defined]
        and default.func.attr == "getboolean"  # type: ignore[attr-defined]
        and len(default.args) == 2
        and default.args[0].value == "operators"  # type: ignore[attr-defined]
        and default.args[1].value == "default_deferrable"  # type: ignore[attr-defined]
        and len(default.keywords) == 1
        and default.keywords[0].value.value is False  # type: ignore[attr-defined]
    )


def check_deferrable_default(module_filename: str) -> bool:
    ast_obj = ast.parse(open(module_filename).read())
    cls_nodes = (node for node in ast.iter_child_nodes(ast_obj) if isinstance(node, ast.ClassDef))
    init_method_nodes = (
        node
        for cls_node in cls_nodes
        for node in ast.iter_child_nodes(cls_node)
        if isinstance(node, ast.FunctionDef) and node.name == "__init__"
    )

    invalid_value_exists: bool = False

    for node in init_method_nodes:
        args = node.args
        arguments = reversed([*args.args, *args.kwonlyargs])
        defaults = reversed([*args.defaults, *args.kw_defaults])
        for argument, default in itertools.zip_longest(arguments, defaults, fillvalue=None):
            if argument is None or default is None:
                continue
            if argument.arg == "deferrable":
                if not _is_valid_deferrable_default(default):
                    print(
                        f"{module_filename}:{default.lineno}\n"
                        "Incorrect deferrable default value\n"
                        "Please set the default value of deferrbale to "
                        """"conf.getboolean("operators", "default_deferrable", fallback=False)"\n"""
                        "See: https://github.com/apache/airflow/blob/main/docs/apache-airflow/authoring-and-scheduling/deferring.rst#writing-deferrable-operators\n"
                    )
                    invalid_value_exists = True
    return invalid_value_exists


if __name__ == "__main__":
    modules = itertools.chain(
        glob.glob(f"{ROOT_DIR}/airflow/**/sensors/**.py", recursive=True),
        glob.glob(f"{ROOT_DIR}/airflow/**/operators/**.py", recursive=True),
    )

    invalid_value_exists: bool = False
    for module_path in filter(lambda module_path: "__init__.py" not in module_path, modules):
        if check_deferrable_default(module_path):
            invalid_value_exists = True

    if invalid_value_exists:
        sys.exit(1)
