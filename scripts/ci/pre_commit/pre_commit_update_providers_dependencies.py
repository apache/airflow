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

import json
import os
import sys
from ast import Import, ImportFrom, NodeVisitor, parse
from collections import defaultdict
from pathlib import Path
from typing import Any, List

import yaml
from rich.console import Console

console = Console(color_system="standard", width=200)

AIRFLOW_PROVIDERS_IMPORT_PREFIX = "airflow.providers."

AIRFLOW_SOURCES_ROOT = Path(__file__).parents[3].resolve()

AIRFLOW_PROVIDERS_DIR = AIRFLOW_SOURCES_ROOT / "airflow" / "providers"
AIRFLOW_TESTS_PROVIDERS_DIR = AIRFLOW_SOURCES_ROOT / "tests" / "providers"
AIRFLOW_SYSTEM_TESTS_PROVIDERS_DIR = AIRFLOW_SOURCES_ROOT / "system" / "tests" / "providers"

DEPENDENCIES_JSON_FILE_PATH = AIRFLOW_SOURCES_ROOT / "generated" / "provider_dependencies.json"

sys.path.insert(0, str(AIRFLOW_SOURCES_ROOT))  # make sure setup is imported from Airflow

warnings: list[str] = []
errors: list[str] = []

suspended_paths: list[str] = []

ALL_DEPENDENCIES: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))

ALL_PROVIDERS: dict[str, dict[str, Any]] = defaultdict(lambda: defaultdict())
ALL_PROVIDER_FILES: list[Path] = []

# Allow AST to parse the files.
sys.path.append(str(AIRFLOW_SOURCES_ROOT))


class ImportFinder(NodeVisitor):
    """
    AST visitor that collects all imported names in its imports
    """

    def __init__(self) -> None:
        self.imports: list[str] = []
        self.handled_import_exception = List[str]
        self.tried_imports: list[str] = []

    def process_import(self, import_name: str) -> None:
        self.imports.append(import_name)

    def get_import_name_from_import_from(self, node: ImportFrom) -> list[str]:
        import_names: list[str] = []
        for alias in node.names:
            name = alias.name
            fullname = f"{node.module}.{name}" if node.module else name
            import_names.append(fullname)
        return import_names

    def visit_Import(self, node: Import):
        for alias in node.names:
            self.process_import(alias.name)

    def visit_ImportFrom(self, node: ImportFrom):
        if node.module == "__future__":
            return
        for fullname in self.get_import_name_from_import_from(node):
            self.process_import(fullname)


def find_all_providers_and_provider_files():
    for root, _, filenames in os.walk(AIRFLOW_PROVIDERS_DIR):
        for filename in filenames:
            if filename == "provider.yaml":
                provider_file = Path(root, filename)
                provider_name = str(provider_file.parent.relative_to(AIRFLOW_PROVIDERS_DIR)).replace(
                    os.sep, "."
                )
                provider_info = yaml.safe_load(provider_file.read_text())
                if not provider_info["suspended"]:
                    ALL_PROVIDERS[provider_name] = provider_info
                else:
                    suspended_paths.append(provider_file.parent.relative_to(AIRFLOW_PROVIDERS_DIR).as_posix())
            path = Path(root, filename)
            if path.is_file() and path.name.endswith(".py"):
                ALL_PROVIDER_FILES.append(Path(root, filename))


def get_provider_id_from_relative_import_or_file(relative_path_or_file: str) -> str | None:
    provider_candidate = relative_path_or_file.replace(os.sep, ".").split(".")
    while provider_candidate:
        candidate_provider_id = ".".join(provider_candidate)
        if "google_vendor" in candidate_provider_id:
            candidate_provider_id = candidate_provider_id.replace("google_vendor", "google")
        if candidate_provider_id in ALL_PROVIDERS:
            return candidate_provider_id
        provider_candidate = provider_candidate[:-1]
    return None


def get_provider_id_from_import(import_name: str, file_path: Path) -> str | None:
    if not import_name.startswith(AIRFLOW_PROVIDERS_IMPORT_PREFIX):
        # skip silently - it's OK to get non-provider imports
        return None
    relative_provider_import = import_name[len(AIRFLOW_PROVIDERS_IMPORT_PREFIX) :]
    provider_id = get_provider_id_from_relative_import_or_file(relative_provider_import)
    if provider_id is None:
        relative_path_from_import = relative_provider_import.replace(".", os.sep)
        if relative_path_from_import.startswith(tuple(suspended_paths)):
            return None
        warnings.append(f"We could not determine provider id from import {import_name} in {file_path}")
    return provider_id


def get_imports_from_file(file_path: Path) -> list[str]:
    root = parse(file_path.read_text(), file_path.name)
    visitor = ImportFinder()
    visitor.visit(root)
    return visitor.imports


def get_provider_id_from_file_name(file_path: Path) -> str | None:
    # is_relative_to is only available in Python 3.9 - we should simplify this check when we are Python 3.9+
    try:
        relative_path = file_path.relative_to(AIRFLOW_PROVIDERS_DIR)
    except ValueError:
        try:
            relative_path = file_path.relative_to(AIRFLOW_SYSTEM_TESTS_PROVIDERS_DIR)
        except ValueError:
            try:
                relative_path = file_path.relative_to(AIRFLOW_TESTS_PROVIDERS_DIR)
            except ValueError:
                errors.append(f"Wrong file not in the providers package = {file_path}")
                return None
    provider_id = get_provider_id_from_relative_import_or_file(str(relative_path))
    if provider_id is None and file_path.name not in ["__init__.py", "get_provider_info.py"]:
        if relative_path.as_posix().startswith(tuple(suspended_paths)):
            return None
        else:
            warnings.append(f"We had a problem to classify the file {file_path} to a provider")
    return provider_id


def check_if_different_provider_used(file_path: Path) -> None:
    file_provider = get_provider_id_from_file_name(file_path)
    if not file_provider:
        return
    imports = get_imports_from_file(file_path)
    for import_name in imports:
        imported_provider = get_provider_id_from_import(import_name, file_path)
        if imported_provider is not None and imported_provider not in ALL_PROVIDERS:
            warnings.append(f"The provider {imported_provider} from {file_path} cannot be found.")
        elif imported_provider and file_provider != imported_provider:
            ALL_DEPENDENCIES[file_provider]["cross-providers-deps"].append(imported_provider)


if __name__ == "__main__":
    find_all_providers_and_provider_files()
    num_files = len(ALL_PROVIDER_FILES)
    num_providers = len(ALL_PROVIDERS)
    console.print(f"Found {len(ALL_PROVIDERS)} providers with {len(ALL_PROVIDER_FILES)} Python files.")
    for file in ALL_PROVIDER_FILES:
        check_if_different_provider_used(file)
    for provider, provider_yaml_content in ALL_PROVIDERS.items():
        if not provider_yaml_content.get("suspended"):
            ALL_DEPENDENCIES[provider]["deps"].extend(provider_yaml_content["dependencies"])
    if warnings:
        console.print("[yellow]Warnings!\n")
        for warning in warnings:
            console.print(f"[yellow] {warning}")
        console.print(f"[bright_blue]Total: {len(warnings)} warnings.")
    if errors:
        console.print("[red]Errors!\n")
        for error in errors:
            console.print(f"[red] {error}")
        console.print(f"[bright_blue]Total: {len(errors)} errors.")
    unique_sorted_dependencies: dict[str, dict[str, list[str]]] = defaultdict(dict)
    for key in sorted(ALL_DEPENDENCIES.keys()):
        unique_sorted_dependencies[key]["deps"] = sorted(ALL_DEPENDENCIES[key]["deps"])
        unique_sorted_dependencies[key]["cross-providers-deps"] = sorted(
            set(ALL_DEPENDENCIES[key]["cross-providers-deps"])
        )
        excluded_versions = ALL_PROVIDERS[key].get("excluded-python-versions")
        unique_sorted_dependencies[key]["excluded-python-versions"] = excluded_versions or []
    if errors:
        console.print()
        console.print("[red]Errors found during verification. Exiting!")
        console.print()
        sys.exit(1)
    old_dependencies = DEPENDENCIES_JSON_FILE_PATH.read_text()
    new_dependencies = json.dumps(unique_sorted_dependencies, indent=2) + "\n"
    if new_dependencies != old_dependencies:
        DEPENDENCIES_JSON_FILE_PATH.write_text(json.dumps(unique_sorted_dependencies, indent=2) + "\n")
        if os.environ.get("CI"):
            console.print()
            console.print(f"[info]Written {DEPENDENCIES_JSON_FILE_PATH}")
            console.print(
                f"[yellow]You will need to run breeze locally and commit "
                f"{DEPENDENCIES_JSON_FILE_PATH.relative_to(AIRFLOW_SOURCES_ROOT)}!\n"
            )
            console.print()
        else:
            console.print()
            console.print(
                f"[yellow]Regenerated new dependencies. Please commit "
                f"{DEPENDENCIES_JSON_FILE_PATH.relative_to(AIRFLOW_SOURCES_ROOT)}!\n"
            )
            console.print(f"[info]Written {DEPENDENCIES_JSON_FILE_PATH}")
            console.print()
    else:
        console.print(
            "[green]No need to regenerate dependencies!\n[/]"
            f"The {DEPENDENCIES_JSON_FILE_PATH.relative_to(AIRFLOW_SOURCES_ROOT)} is up to date!\n"
        )
