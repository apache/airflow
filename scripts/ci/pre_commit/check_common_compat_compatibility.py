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

import argparse
import ast
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass


@dataclass
class Argument:
    name: str
    has_default: bool
    type_annotation: str | None = None

    def __hash__(self):
        return hash((self.name, self.has_default, self.type_annotation))


@dataclass
class MethodSignature:
    name: str
    args: list[Argument]
    returns: str | None
    is_class_method: bool
    parent_class: str | None

    def __hash__(self):
        return hash((self.name, tuple(self.args), self.returns, self.is_class_method, self.parent_class))


class MethodVisitor(ast.NodeVisitor):
    def __init__(self):
        self.methods: dict[str, MethodSignature] = {}
        self.current_class = None

    def visit_ClassDef(self, node: ast.ClassDef):
        previous_class = self.current_class
        self.current_class = node.name
        self.generic_visit(node)
        self.current_class = previous_class

    def _get_return_annotation(self, node: ast.FunctionDef) -> str | None:
        if node.returns:
            return ast.unparse(node.returns)
        return None

    def _get_args(self, node: ast.FunctionDef) -> list[Argument]:
        args = []
        for arg in node.args.args:
            type_annotation = ast.unparse(arg.annotation) if arg.annotation else None
            args.append(Argument(name=arg.arg, has_default=False, type_annotation=type_annotation))

        # Handle defaults
        defaults_start = len(node.args.args) - len(node.args.defaults)
        for i, _ in enumerate(node.args.defaults):
            args[defaults_start + i].has_default = True

        return args

    def _is_class_method(self, node: ast.FunctionDef) -> bool:
        return any(
            isinstance(decorator, ast.Name) and decorator.id == "classmethod"
            for decorator in node.decorator_list
        )

    def visit_FunctionDef(self, node: ast.FunctionDef):
        qualified_name = f"{self.current_class}.{node.name}" if self.current_class else node.name
        self.methods[qualified_name] = MethodSignature(
            name=node.name,
            args=self._get_args(node),
            returns=self._get_return_annotation(node),
            is_class_method=self._is_class_method(node),
            parent_class=self.current_class,
        )

    def visit_AsyncFuncztionDef(self, node: ast.AsyncFunctionDef):
        self.visit_FunctionDef(node)  # type: ignore[arg-type]


def compare_method_signatures(original: MethodSignature, staged: MethodSignature) -> list[str]:
    errors = []

    # Skip self/cls parameter for class methods
    orig_args = original.args[1:] if original.is_class_method else original.args
    staged_args = staged.args[1:] if staged.is_class_method else staged.args

    # Create maps of argument names to their properties
    orig_arg_map = {arg.name: arg for arg in orig_args}
    staged_arg_map = {arg.name: arg for arg in staged_args}

    # Check for removed arguments
    for arg_name, _ in orig_arg_map.items():
        if arg_name not in staged_arg_map:
            errors.append(f"Removed argument '{arg_name}'")

    # Check for added arguments without defaults
    for arg_name, arg in staged_arg_map.items():
        if arg_name not in orig_arg_map and not arg.has_default:
            errors.append(f"Added required argument '{arg_name}' without default value")

    return errors


def get_methods(content: str) -> dict[str, MethodSignature]:
    try:
        tree = ast.parse(content)
        visitor = MethodVisitor()
        visitor.visit(tree)
        return visitor.methods
    except SyntaxError:
        return {}


def get_staged_and_original_content(filename: str) -> tuple[str, str]:
    staged_content = subprocess.check_output(["git", "show", ":" + filename], text=True)

    try:
        original_content = subprocess.check_output(
            ["git", "show", "HEAD:" + filename], text=True, stderr=subprocess.DEVNULL
        )
    except subprocess.CalledProcessError:
        original_content = ""

    return staged_content, original_content


def format_method_signature(method: MethodSignature) -> str:
    class_prefix = f"{method.parent_class}." if method.parent_class else ""
    decorator = "@classmethod " if method.is_class_method else ""
    args_str = ", ".join(
        f"{arg.name}: {arg.type_annotation if arg.type_annotation else 'Any'}"
        f"{' = ...' if arg.has_default else ''}"
        for arg in method.args
    )
    returns = f" -> {method.returns}" if method.returns else ""
    return f"{decorator}{class_prefix}{method.name}({args_str}){returns}"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("filenames", nargs="*", help="Filenames to check")
    args = parser.parse_args(argv)

    retval = 0

    for filename in args.filenames:
        if not filename.endswith(".py"):
            continue

        try:
            staged_content, original_content = get_staged_and_original_content(filename)

            original_methods = get_methods(original_content)
            staged_methods = get_methods(staged_content)

            # Check for removed methods
            removed_methods = set(original_methods.keys()) - set(staged_methods.keys())

            # Check for signature changes in remaining methods
            signature_errors = {}
            for method_name in set(original_methods.keys()) & set(staged_methods.keys()):
                errors = compare_method_signatures(original_methods[method_name], staged_methods[method_name])
                if errors:
                    signature_errors[method_name] = errors

            if removed_methods or signature_errors:
                print(f"\nIssues found in {filename}:")

                if removed_methods:
                    print("\nRemoved methods:")
                    for method_name in removed_methods:
                        print(f"  - {format_method_signature(original_methods[method_name])}")

                if signature_errors:
                    print("\nSignature changes:")
                    for method_name, errors in signature_errors.items():
                        print(f"\n  {format_method_signature(staged_methods[method_name])}:")
                        for error in errors:
                            print(f"    - {error}")

                retval = 1

        except Exception as e:
            print(f"Error processing {filename}: {str(e)}")
            retval = 1

    return retval


if __name__ == "__main__":
    exit(main())
