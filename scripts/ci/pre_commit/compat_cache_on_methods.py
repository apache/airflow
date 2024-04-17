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
import pathlib
import sys

COMPAT_MODULE = "airflow.compat.functools"


def check_test_file(file: str) -> int:
    node = ast.parse(pathlib.Path(file).read_text("utf-8"), file)
    if not (classes := [c for c in node.body if isinstance(c, ast.ClassDef)]):
        # Exit early if module doesn't contain any classes
        return 0

    compat_cache_aliases = []
    for stmt in node.body:
        if not isinstance(stmt, ast.ImportFrom) or stmt.module != COMPAT_MODULE:
            continue
        for alias in stmt.names:
            if "cache" in alias.name:
                compat_cache_aliases.append(alias.asname or alias.name)
    if not compat_cache_aliases:
        # Exit early in case if there are no imports from `airflow.compat.functools.cache`
        return 0

    found = 0
    for klass in classes:
        for cls_stmt in klass.body:
            if not isinstance(cls_stmt, ast.FunctionDef) or not cls_stmt.decorator_list:
                continue
            for decorator in cls_stmt.decorator_list:
                if (isinstance(decorator, ast.Name) and decorator.id in compat_cache_aliases) or (
                    isinstance(decorator, ast.Attribute) and decorator.attr in compat_cache_aliases
                ):
                    found += 1
                    prefix = f"{file}:{decorator.lineno}:"
                    print(f"{prefix} Use of `{COMPAT_MODULE}.cache` on methods can lead to memory leaks")

    return found


def main(*args: str) -> int:
    errors = sum(check_test_file(file) for file in args[1:])
    if not errors:
        return 0
    print(f"Found {errors} error{'s' if errors > 1 else ''}.")
    return 1


if __name__ == "__main__":
    sys.exit(main(*sys.argv))
