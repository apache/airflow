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
import functools
import itertools
import os
import pathlib
import sys
import typing

import yaml

if typing.TYPE_CHECKING:
    import typing_extensions

AIRFLOW_PROVIDERS_ROOT = pathlib.Path("airflow/providers")

CheckType = typing.Union[
    None,  # Checking for the module only.
    typing.Type[ast.ClassDef],
    typing.Type[ast.FunctionDef],
]

# Tuple items are:
# - Section: Which top-level section in provider.yaml to check.
# - Subkey: If None, each value in the section is treated as an import path.
#     If not None, each value is a dict, with the value to the specified key
#     being an import path.
# - Check type: If None, the value is a module path. Otherwise the value is a
#     (module-global) object's full name and this specifies its AST type.
SECTIONS_TO_CHECK: list[tuple[str, str | None, CheckType]] = [
    ("auth-backends", None, None),
    ("connection-types", "hook-class-name", ast.ClassDef),
    ("executors", None, ast.ClassDef),
    ("extra-links", None, ast.ClassDef),
    ("secrets-backends", None, ast.ClassDef),
    ("logging", None, ast.ClassDef),
    ("notifications", None, ast.ClassDef),
    ("task-decorators", "class-name", ast.FunctionDef),
]


@functools.lru_cache
def _get_module(module_name: str) -> ast.Module | None:
    path = pathlib.Path(*module_name.split(".")).with_suffix(".py")
    if not path.is_file():
        return None
    return ast.parse(path.read_text("utf-8"), os.fspath(path))


def _is_check_type(v: ast.AST, t: type) -> typing_extensions.TypeGuard[ast.ClassDef | ast.FunctionDef]:
    return isinstance(v, t)


def _iter_unimportable_paths(
    data: dict,
    section: str,
    subkey: str | None,
    check_type: CheckType,
) -> typing.Iterator[str]:
    for item in data.get(section, ()):
        if subkey is None:
            import_path = item
        else:
            import_path = item[subkey]

        if check_type is None:
            module_name = import_path
            class_name = ""
        else:
            module_name, class_name = import_path.rsplit(".", 1)

        if (module := _get_module(module_name)) is None:
            yield import_path
            continue

        if check_type is None:
            continue

        has_class = any(
            _is_check_type(child, check_type) and child.name == class_name
            for child in ast.iter_child_nodes(module)
        )
        if not has_class:
            yield import_path


def _iter_provider_yaml_errors(path: pathlib.Path) -> typing.Iterator[tuple[pathlib.Path, str, str]]:
    with path.open() as f:
        data = yaml.safe_load(f)
    for section, subkey, check_type in SECTIONS_TO_CHECK:
        for error in _iter_unimportable_paths(data, section, subkey, check_type):
            yield path, section, error


def _iter_provider_paths(argv: list[str]) -> typing.Iterator[pathlib.Path]:
    for argument in argv:
        parents = list(pathlib.Path(argument).parents)
        try:
            root_index = parents.index(AIRFLOW_PROVIDERS_ROOT)
        except ValueError:  # Not in providers directory.
            continue
        for parent in parents[:root_index]:
            if (provider_yaml := parent.joinpath("provider.yaml")).is_file():
                yield provider_yaml


def main(argv: list[str]) -> int:
    providers_to_check = sorted(set(_iter_provider_paths(argv)))
    errors = list(itertools.chain.from_iterable(_iter_provider_yaml_errors(p) for p in providers_to_check))
    for provider, section, value in errors:
        print(f"Broken {provider.relative_to(AIRFLOW_PROVIDERS_ROOT)} {section!r}: {value}")
    return len(errors)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
