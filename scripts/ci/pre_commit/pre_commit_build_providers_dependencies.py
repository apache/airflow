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
import json
import os
import sys
from ast import Import, ImportFrom, NodeVisitor, parse
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

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

warnings: List[str] = []
errors: List[str] = []

CROSS_PROVIDERS_DEPS = "cross-providers-deps"
DEPS = "deps"

ALL_DEPENDENCIES: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: defaultdict(list))

ALL_PROVIDERS: Dict[str, Dict[str, Any]] = defaultdict(lambda: defaultdict())
ALL_PROVIDER_FILES: List[Path] = []

# Allow AST to parse the files.
sys.path.append(str(AIRFLOW_SOURCES_ROOT))


class ImportFinder(NodeVisitor):
    """
    AST visitor that collects all imported names in its imports
    """

    def __init__(self) -> None:
        self.imports: List[str] = []
        self.handled_import_exception = List[str]
        self.tried_imports: List[str] = []

    def process_import(self, import_name: str) -> None:
        self.imports.append(import_name)

    def get_import_name_from_import_from(self, node: ImportFrom) -> List[str]:
        import_names: List[str] = []
        for alias in node.names:
            name = alias.name
            fullname = f'{node.module}.{name}' if node.module else name
            import_names.append(fullname)
        return import_names

    def visit_Import(self, node: Import):
        for alias in node.names:
            self.process_import(alias.name)

    def visit_ImportFrom(self, node: ImportFrom):
        if node.module == '__future__':
            return
        for fullname in self.get_import_name_from_import_from(node):
            self.process_import(fullname)


def find_all_providers_and_provider_files():
    for (root, _, filenames) in os.walk(AIRFLOW_PROVIDERS_DIR):
        for filename in filenames:
            if filename == 'provider.yaml':
                provider_file = Path(root, filename)
                provider_name = str(provider_file.parent.relative_to(AIRFLOW_PROVIDERS_DIR)).replace(
                    os.sep, "."
                )
                ALL_PROVIDERS[provider_name] = yaml.safe_load(provider_file.read_text())
            path = Path(root, filename)
            if path.is_file() and path.name.endswith(".py"):
                ALL_PROVIDER_FILES.append(Path(root, filename))


def get_provider_id_from_relative_import_or_file(relative_path_or_file: str) -> Optional[str]:
    provider_candidate = relative_path_or_file.replace(os.sep, ".").split(".")
    while len(provider_candidate) > 0:
        candidate_provider_id = ".".join(provider_candidate)
        if candidate_provider_id in ALL_PROVIDERS:
            return candidate_provider_id
        provider_candidate = provider_candidate[:-1]
    return None


def get_provider_id_from_import(import_name: str, file_path: Path) -> Optional[str]:
    if not import_name.startswith(AIRFLOW_PROVIDERS_IMPORT_PREFIX):
        # skip silently - it's OK to get non-provider imports
        return None
    relative_provider_import = import_name[len(AIRFLOW_PROVIDERS_IMPORT_PREFIX) :]
    provider_id = get_provider_id_from_relative_import_or_file(relative_provider_import)
    if provider_id is None:
        warnings.append(f"We could not determine provider id from import {import_name} in {file_path}")
    return provider_id


def get_imports_from_file(file_path: Path) -> List[str]:
    root = parse(file_path.read_text(), file_path.name)
    visitor = ImportFinder()
    visitor.visit(root)
    return visitor.imports


def get_provider_id_from_file_name(file_path: Path) -> Optional[str]:
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
            ALL_DEPENDENCIES[file_provider][CROSS_PROVIDERS_DEPS].append(imported_provider)


if __name__ == '__main__':
    find_all_providers_and_provider_files()
    num_files = len(ALL_PROVIDER_FILES)
    num_providers = len(ALL_PROVIDERS)
    console.print(f"Found {len(ALL_PROVIDERS)} providers with {len(ALL_PROVIDER_FILES)} Python files.")

    for file in ALL_PROVIDER_FILES:
        check_if_different_provider_used(file)

    for provider, provider_yaml_content in ALL_PROVIDERS.items():
        ALL_DEPENDENCIES[provider][DEPS].extend(provider_yaml_content['dependencies'])

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
    unique_sorted_dependencies: Dict[str, Dict[str, List[str]]] = defaultdict(dict)
    for key in sorted(ALL_DEPENDENCIES.keys()):
        unique_sorted_dependencies[key][DEPS] = sorted(ALL_DEPENDENCIES[key][DEPS])
        unique_sorted_dependencies[key][CROSS_PROVIDERS_DEPS] = sorted(
            set(ALL_DEPENDENCIES[key][CROSS_PROVIDERS_DEPS])
        )
    if errors:
        console.print()
        console.print("[red]Errors found during verification. Exiting!")
        console.print()
        sys.exit(1)
    DEPENDENCIES_JSON_FILE_PATH.write_text(json.dumps(unique_sorted_dependencies, indent=2) + "\n")
    console.print()
    console.print("[green]Verification complete! Success!\n")
    console.print(f"Written {DEPENDENCIES_JSON_FILE_PATH}")
    console.print()
