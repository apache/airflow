#!/usr/bin/env python3

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

"""
Check for problematic context manager decorators on test classes.

Context managers (ContextDecorator, @contextlib.contextmanager) when used as class decorators
transform the class into a callable wrapper, which prevents pytest from collecting the class.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path


class ContextManagerClassDecoratorChecker(ast.NodeVisitor):
    """AST visitor to check for context manager decorators on test classes."""

    def __init__(self, filename: str):
        self.filename = filename
        self.errors: list[str] = []

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        """Check class definitions for problematic decorators."""
        if not node.name.startswith("Test"):
            self.generic_visit(node)
            return

        for decorator in node.decorator_list:
            decorator_name = self._get_decorator_name(decorator)
            if self._is_problematic_decorator(decorator_name):
                self.errors.append(
                    f"{self.filename}:{node.lineno}: Class '{node.name}' uses @{decorator_name} "
                    f"decorator which prevents pytest collection. Use @pytest.mark.usefixtures instead."
                )

        self.generic_visit(node)

    def _get_decorator_name(self, decorator: ast.expr) -> str:
        """Extract decorator name from AST node."""
        if isinstance(decorator, ast.Name):
            return decorator.id
        if isinstance(decorator, ast.Call):
            if isinstance(decorator.func, ast.Name):
                return decorator.func.id
            if isinstance(decorator.func, ast.Attribute):
                return f"{self._get_attr_chain(decorator.func)}"
        elif isinstance(decorator, ast.Attribute):
            return f"{self._get_attr_chain(decorator)}"
        return "unknown"

    def _get_attr_chain(self, node: ast.Attribute) -> str:
        """Get the full attribute chain (e.g., 'contextlib.contextmanager')."""
        if isinstance(node.value, ast.Name):
            return f"{node.value.id}.{node.attr}"
        if isinstance(node.value, ast.Attribute):
            return f"{self._get_attr_chain(node.value)}.{node.attr}"
        return node.attr

    def _is_problematic_decorator(self, decorator_name: str) -> bool:
        """Check if decorator is known to break pytest class collection."""
        problematic_decorators = {
            "conf_vars",
            "env_vars",
            "contextlib.contextmanager",
            "contextmanager",
        }
        return decorator_name in problematic_decorators


def check_file(filepath: Path) -> list[str]:
    """Check a single file for problematic decorators."""
    try:
        with open(filepath, encoding="utf-8") as f:
            content = f.read()

        tree = ast.parse(content, filename=str(filepath))
        checker = ContextManagerClassDecoratorChecker(str(filepath))
        checker.visit(tree)
        return checker.errors
    except Exception as e:
        return [f"{filepath}: Error parsing file: {e}"]


def main() -> int:
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: check_contextmanager_class_decorators.py <file_or_directory>...")
        return 1

    all_errors = []

    for arg in sys.argv[1:]:
        path = Path(arg)
        if path.is_file() and path.suffix == ".py":
            if "test" in str(path):  # Only check test files
                all_errors.extend(check_file(path))
            else:
                print(f"Skipping non-test file: {path}")
        elif path.is_dir():
            for py_file in path.rglob("*.py"):
                if "test" in str(py_file):  # Only check test files
                    all_errors.extend(check_file(py_file))

    if all_errors:
        print("Found problematic context manager class decorators:")
        for error in all_errors:
            print(f"  {error}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
