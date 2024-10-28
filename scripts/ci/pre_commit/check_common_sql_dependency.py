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
import pathlib
import sys
from typing import Iterable

import yaml
from packaging.specifiers import SpecifierSet
from rich.console import Console

console = Console(color_system="standard", width=200)


COMMON_SQL_PROVIDER_NAME: str = "apache-airflow-providers-common-sql"
COMMON_SQL_PROVIDER_MIN_COMPATIBLE_VERSIONS: str = "1.9.1"
COMMON_SQL_PROVIDER_LATEST_INCOMPATIBLE_VERSION: str = "1.9.0"
MAKE_COMMON_METHOD_NAME: str = "_make_common_data_structure"


def get_classes(file_path: str) -> Iterable[ast.ClassDef]:
    """Return a list of class declared in the given python file."""
    pathlib_path = pathlib.Path(file_path)
    module = ast.parse(pathlib_path.read_text("utf-8"), str(pathlib_path))
    for node in ast.walk(module):
        if isinstance(node, ast.ClassDef):
            yield node


def is_subclass_of_dbapihook(node: ast.ClassDef) -> bool:
    """Return the subclass's name of a given class definition."""
    for base in node.bases:
        if isinstance(base, ast.Name) and base.id == "DbApiHook":
            return True
    return False


def has_make_serializable_method(node: ast.ClassDef) -> bool:
    """Return True if the given class implements `_make_common_data_structure` method."""
    for body_element in node.body:
        if isinstance(body_element, ast.FunctionDef) and (
            body_element.name == MAKE_COMMON_METHOD_NAME
        ):
            return True
    return False


def determine_provider_yaml_path(file_path: str) -> str:
    """Determine the path of the provider.yaml file related to the given python file."""
    return f"{file_path.split('/hooks')[0]}/provider.yaml"


def get_yaml_content(file_path: str) -> dict:
    """Load content of a yaml files."""
    with open(file_path) as file:
        return yaml.safe_load(file)


def get_common_sql_constraints(provider_metadata: dict) -> str | None:
    """Return the version constraints of `apache-airflow-providers-common-sql`."""
    dependencies: list[str] = provider_metadata["dependencies"]
    for dependency in dependencies:
        if dependency.startswith(COMMON_SQL_PROVIDER_NAME):
            return dependency[len(COMMON_SQL_PROVIDER_NAME) :]
    return None


def do_version_satisfies_constraints(
    version: str,
    max_incompatible_version=COMMON_SQL_PROVIDER_LATEST_INCOMPATIBLE_VERSION,
) -> bool:
    """Check if the `version_string` is constrained to at least >= 1.8.1."""
    constraints: list[str] = [constraint.strip() for constraint in version.split(",")]
    specifier_set = SpecifierSet(",".join(constraints))
    return not specifier_set.contains(max_incompatible_version)


def check_sql_providers_dependency():
    error_count: int = 0
    for path in sys.argv[1:]:
        if not path.startswith("airflow/providers/"):
            continue

        for clazz in get_classes(path):
            if is_subclass_of_dbapihook(node=clazz) and has_make_serializable_method(
                node=clazz
            ):
                provider_yaml_path: str = determine_provider_yaml_path(file_path=path)
                provider_metadata: dict = get_yaml_content(file_path=provider_yaml_path)

                if version_constraint := get_common_sql_constraints(
                    provider_metadata=provider_metadata
                ):
                    if not do_version_satisfies_constraints(version=version_constraint):
                        error_count += 1
                        console.print(
                            f"\n[yellow]Provider {provider_metadata['name']} must have "
                            f"'{COMMON_SQL_PROVIDER_NAME}>={COMMON_SQL_PROVIDER_MIN_COMPATIBLE_VERSIONS}' as "
                            f"dependency, because `{clazz.name}` overrides the "
                            f"`{MAKE_COMMON_METHOD_NAME}` method."
                        )
    if error_count:
        console.print(
            f"The `{MAKE_COMMON_METHOD_NAME}` method was introduced in {COMMON_SQL_PROVIDER_NAME} "
            f"{COMMON_SQL_PROVIDER_MIN_COMPATIBLE_VERSIONS}. You cannot rely on an older version of this "
            "provider to override this method."
        )
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(check_sql_providers_dependency())
