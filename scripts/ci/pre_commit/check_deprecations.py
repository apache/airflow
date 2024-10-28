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
import sys
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Any

from dateutil.relativedelta import relativedelta
from rich.console import Console

console = Console(color_system="standard", width=200)


allowed_warnings: dict[str, tuple[str, ...]] = {
    "airflow": (
        "airflow.exceptions.RemovedInAirflow3Warning",
        "airflow.utils.context.AirflowContextDeprecationWarning",
    ),
    "providers": ("airflow.exceptions.AirflowProviderDeprecationWarning",),
}
compatible_decorators: frozenset[tuple[str, ...]] = frozenset(
    [
        # PEP 702 decorators
        ("warnings", "deprecated"),
        ("typing_extensions", "deprecated"),
        # `Deprecated` package decorators
        ("deprecated", "deprecated"),
        ("deprecated", "classic", "deprecated"),
        # Experimental Google Provider's decorator with structured deprecation details
        ("airflow", "providers", "google", "common", "deprecated", "deprecated"),
    ]
)
end_of_life_deprecation_warnings: dict[str, tuple[str, ...]] = {
    "airflow/providers/google/": ("AirflowProviderDeprecationWarning",),
}


def is_file_under_eol_deprecation(file_path: str, warning_class: str) -> bool:
    for prefix, warnings in end_of_life_deprecation_warnings.items():
        if file_path.startswith(prefix):
            return bool(warning_class in warnings)
    return False


def validate_end_of_life_deprecation_warnings(
    file_path: str, decorator: Any, warning_class: str
) -> int:
    _errors = 0
    if not is_file_under_eol_deprecation(
        file_path=file_path, warning_class=warning_class
    ):
        return 0

    if not get_decorator_argument(decorator, "planned_removal_date"):
        expected_date = date.today() + relativedelta(months=6)
        if expected_date.day > 1:
            _date = date.today() + relativedelta(months=7)
            expected_date = date(day=1, month=_date.month, year=_date.year)
        expected_date_str: str = expected_date.strftime("%B %d, %Y")

        _errors += 1
        console.print(
            f"{file_path}:{decorator.lineno}: "
            "The 'planned_removal_date' parameter is missing in the `@deprecated(...)` call.\n"
            "Please provide the date in the format 'Month DD, YYYY' "
            "after which the deprecated object should be removed.\n"
            "The recommended date is at least six months ahead: "
            f"'planned_removal_date=\"{expected_date_str}\"'"
        )
    return _errors


def get_decorator_argument(decorator: ast.Call, argument_name: str) -> ast.keyword | None:
    return next(filter(lambda k: k and k.arg == argument_name, decorator.keywords), None)  # type: ignore[arg-type]


@lru_cache(maxsize=None)
def allowed_group_warnings(group: str) -> tuple[str, tuple[str, ...]]:
    group_warnings = allowed_warnings[group]
    if len(group_warnings) == 1:
        return f"expected {group_warnings[0]} type", group_warnings
    else:
        return f"expected one of {', '.join(group_warnings)} types", group_warnings


def built_import_from(import_from: ast.ImportFrom) -> list[str]:
    result: list[str] = []
    module_name = import_from.module
    if not module_name:
        return result

    imports_levels = module_name.count(".") + 1
    for import_path in compatible_decorators:
        if imports_levels >= len(import_path):
            continue
        if module_name != ".".join(import_path[:imports_levels]):
            continue
        for name in import_from.names:
            if name.name == import_path[imports_levels]:
                alias: str = name.asname or name.name
                remaining_part = len(import_path) - imports_levels - 1
                if remaining_part > 0:
                    alias = ".".join([alias, *import_path[-remaining_part:]])
                result.append(alias)
    return result


def built_import(import_clause: ast.Import) -> list[str]:
    result = []
    for name in import_clause.names:
        module_name = name.name
        imports_levels = module_name.count(".") + 1
        for import_path in compatible_decorators:
            if imports_levels > len(import_path):
                continue
            if module_name != ".".join(import_path[:imports_levels]):
                continue

            alias: str = name.asname or module_name
            remaining_part = len(import_path) - imports_levels
            if remaining_part > 0:
                alias = ".".join([alias, *import_path[-remaining_part:]])
            result.append(alias)
    return result


def found_compatible_decorators(mod: ast.Module) -> tuple[str, ...]:
    result = []
    for node in mod.body:
        if not isinstance(node, (ast.ImportFrom, ast.Import)):
            continue
        result.extend(
            built_import_from(node)
            if isinstance(node, ast.ImportFrom)
            else built_import(node)
        )
    return tuple(sorted(set(result)))


def resolve_name(obj: ast.Attribute | ast.Name) -> str:
    name = ""
    while True:
        if isinstance(obj, ast.Name):
            name = f"{obj.id}.{name}" if name else obj.id
            break
        elif isinstance(obj, ast.Attribute):
            name = f"{obj.attr}.{name}" if name else obj.attr
            obj = obj.value  # type: ignore[assignment]
        else:
            msg = f"Expected to got ast.Name or ast.Attribute but got {type(obj).__name__!r}."
            raise SystemExit(msg)

    return name


def resolve_decorator_name(obj: ast.Call | ast.Attribute | ast.Name) -> str:
    return resolve_name(obj.func if isinstance(obj, ast.Call) else obj)  # type: ignore[arg-type]


def check_decorators(mod: ast.Module, file: str, file_group: str) -> int:
    if not (decorators_names := found_compatible_decorators(mod)):
        # There are no expected decorators into module, exit early
        return 0

    errors = 0
    for node in ast.walk(mod):
        if not hasattr(node, "decorator_list"):
            continue

        for decorator in node.decorator_list:
            decorator_name = resolve_decorator_name(decorator)
            if decorator_name not in decorators_names:
                continue

            expected_types, warns_types = allowed_group_warnings(file_group)
            category_keyword = get_decorator_argument(decorator, "category")
            if category_keyword is None:
                errors += 1
                print(
                    f"{file}:{decorator.lineno}: Missing `category` keyword on decorator @{decorator_name}, "
                    f"{expected_types}"
                )
                continue
            elif not hasattr(category_keyword, "value"):
                continue
            category_value_ast = category_keyword.value

            warns_types = allowed_warnings[file_group]
            if isinstance(category_value_ast, (ast.Name, ast.Attribute)):
                category_value = resolve_name(category_value_ast)
                if not any(cv.endswith(category_value) for cv in warns_types):
                    errors += 1
                    print(
                        f"{file}:{category_keyword.lineno}: "
                        f"category={category_value}, but {expected_types}"
                    )
                errors += validate_end_of_life_deprecation_warnings(
                    file_path=file, decorator=decorator, warning_class=category_value
                )
            elif isinstance(category_value_ast, ast.Constant):
                errors += 1
                print(
                    f"{file}:{category_keyword.lineno}: "
                    f"category=Literal[{category_value_ast.value!r}], but {expected_types}"
                )

    return errors


def check_file(file: str) -> int:
    file_path = Path(file)
    if not file_path.as_posix().startswith("airflow"):
        # Not expected file, exit early
        return 0
    file_group = (
        "providers" if file_path.as_posix().startswith("airflow/providers") else "airflow"
    )
    ast_module = ast.parse(file_path.read_text("utf-8"), file)
    errors = 0
    errors += check_decorators(ast_module, file, file_group)
    return errors


def main(*args: str) -> int:
    errors = sum(check_file(file) for file in args[1:])
    if not errors:
        return 0
    print(f"Found {errors} error{'s' if errors > 1 else ''}.")
    return 1


if __name__ == "__main__":
    sys.exit(main(*sys.argv))
