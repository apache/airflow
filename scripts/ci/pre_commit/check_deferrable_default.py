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

import libcst as cst
from libcst.codemod import CodemodContext
from libcst.codemod.visitors import AddImportsVisitor

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir))

DEFERRABLE_DOC = (
    "https://github.com/apache/airflow/blob/main/docs/apache-airflow/"
    "authoring-and-scheduling/deferring.rst#writing-deferrable-operators"
)


class DefaultDeferrableVisitor(ast.NodeVisitor):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, *kwargs)
        self.error_linenos: list[int] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> ast.FunctionDef:
        if node.name == "__init__":
            args = node.args
            arguments = reversed([*args.args, *args.posonlyargs, *args.kwonlyargs])
            defaults = reversed([*args.defaults, *args.kw_defaults])
            for argument, default in itertools.zip_longest(arguments, defaults):
                # argument is not deferrable
                if argument is None or argument.arg != "deferrable":
                    continue

                # argument is deferrable, but comes with no default value
                if default is None:
                    self.error_linenos.append(argument.lineno)
                    continue

                # argument is deferrable, but the default value is not valid
                if not _is_valid_deferrable_default(default):
                    self.error_linenos.append(default.lineno)
        return node


class DefaultDeferrableTransformer(cst.CSTTransformer):
    def leave_Param(self, original_node: cst.Param, updated_node: cst.Param) -> cst.Param:
        if original_node.name.value == "deferrable":
            expected_default_cst = cst.parse_expression(
                'conf.getboolean("operators", "default_deferrable", fallback=False)'
            )

            if updated_node.default and updated_node.default.deep_equals(expected_default_cst):
                return updated_node
            return updated_node.with_changes(default=expected_default_cst)
        return updated_node


def _is_valid_deferrable_default(default: ast.AST) -> bool:
    """Check whether default is 'conf.getboolean("operators", "default_deferrable", fallback=False)'"""
    return ast.unparse(default) == "conf.getboolean('operators', 'default_deferrable', fallback=False)"


def iter_check_deferrable_default_errors(module_filename: str) -> Iterator[str]:
    ast_tree = ast.parse(open(module_filename).read())
    visitor = DefaultDeferrableVisitor()
    visitor.visit(ast_tree)
    # We check the module using the ast once and then fix it through cst if needed.
    # The primary reason we don't do it all through cst is performance.
    if visitor.error_linenos:
        _fix_invalide_deferrable_default_value(module_filename)
    yield from (f"{module_filename}:{lineno}" for lineno in visitor.error_linenos)


def _fix_invalide_deferrable_default_value(module_filename: str) -> None:
    context = CodemodContext(filename=module_filename)
    AddImportsVisitor.add_needed_import(context, "airflow.configuration", "conf")
    transformer = DefaultDeferrableTransformer()

    source_cst_tree = cst.parse_module(open(module_filename).read())
    modified_cst_tree = AddImportsVisitor(context).transform_module(source_cst_tree.visit(transformer))
    if not source_cst_tree.deep_equals(modified_cst_tree):
        with open(module_filename, "w") as writer:
            writer.write(modified_cst_tree.code)


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
            """Please set the default value of deferrable to """
            """"conf.getboolean("operators", "default_deferrable", fallback=False)"\n"""
            f"See: {DEFERRABLE_DOC}\n"
        )

    return len(errors)


if __name__ == "__main__":
    sys.exit(main())
