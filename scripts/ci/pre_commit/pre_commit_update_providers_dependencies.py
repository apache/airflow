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

import hashlib
import json
import os
import sys
from ast import Import, ImportFrom, NodeVisitor, parse
from collections import defaultdict
from enum import Enum
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

PYPROJECT_TOML_FILE_PATH = AIRFLOW_SOURCES_ROOT / "pyproject.toml"

MY_FILE = Path(__file__).resolve()
MY_MD5SUM_FILE = MY_FILE.parent / MY_FILE.name.replace(".py", ".py.md5sum")


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
                if provider_info["state"] == "suspended":
                    suspended_paths.append(provider_file.parent.relative_to(AIRFLOW_PROVIDERS_DIR).as_posix())
                ALL_PROVIDERS[provider_name] = provider_info
            path = Path(root, filename)
            if path.is_file() and path.name.endswith(".py"):
                ALL_PROVIDER_FILES.append(Path(root, filename))


def get_provider_id_from_relative_import_or_file(relative_path_or_file: str) -> str | None:
    provider_candidate = relative_path_or_file.replace(os.sep, ".").split(".")
    while provider_candidate:
        candidate_provider_id = ".".join(provider_candidate)
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


STATES: dict[str, str] = {}

FOUND_EXTRAS: dict[str, list[str]] = defaultdict(list)


class ParsedDependencyTypes(Enum):
    CORE_EXTRAS = "core extras"
    APACHE_NO_PROVIDER_EXTRAS = "Apache no provider extras"
    DEVEL_EXTRAS = "devel extras"
    DOC_EXTRAS = "doc extras"
    BUNDLE_EXTRAS = "bundle extras"
    DEPRECATED_EXTRAS = "deprecated extras"
    MANUAL_EXTRAS = "manual extras"


GENERATED_DEPENDENCIES_START = "# START OF GENERATED DEPENDENCIES"
GENERATED_DEPENDENCIES_END = "# END OF GENERATED DEPENDENCIES"


def normalize_extra(dependency: str) -> str:
    return dependency.replace(".", "-").replace("_", "-")


def normalize_package_name(dependency: str) -> str:
    return f"apache-airflow-providers-{dependency.replace('.', '-').replace('_', '-')}"


def convert_to_extra_dependency(dependency: str) -> str:
    # if there is version in dependency - remove it as we do not need it in extra specification
    # for editable installation
    if ">=" in dependency:
        dependency = dependency.split(">=")[0]
    extra = dependency.replace("apache-airflow-providers-", "").replace("-", "_").replace(".", "_")
    return f"apache-airflow[{extra}]"


def generate_dependencies(
    result_content: list[str],
    dependencies: dict[str, dict[str, list[str] | str]],
):
    def generate_parsed_extras(type: ParsedDependencyTypes):
        result_content.append(f"    # {type.value}")
        for extra in FOUND_EXTRAS[type.value]:
            result_content.append(f'    "apache-airflow[{extra}]",')

    def get_python_exclusion(dependency_info: dict[str, list[str] | str]):
        excluded_python_versions = dependency_info.get("excluded-python-versions")
        exclusion = ""
        if excluded_python_versions:
            separator = ";"
            for version in excluded_python_versions:
                exclusion += f'{separator}python_version != \\"{version}\\"'
                separator = " and "
        return exclusion

    for dependency, dependency_info in dependencies.items():
        if dependency_info["state"] in ["suspended", "removed"]:
            continue
        result_content.append(f"{normalize_extra(dependency)} = [")
        deps = dependency_info["deps"]
        if not isinstance(deps, list):
            raise TypeError(f"Wrong type of 'deps' {deps} for {dependency} in {DEPENDENCIES_JSON_FILE_PATH}")
        for dep in deps:
            if dep.startswith("apache-airflow-providers-"):
                dep = convert_to_extra_dependency(dep)
            elif dep.startswith("apache-airflow>="):
                continue
            result_content.append(f'  "{dep}{get_python_exclusion(dependency_info)}",')
        devel_deps = dependency_info.get("devel-deps")
        if devel_deps:
            result_content.append(f"  # Devel dependencies for the {dependency} provider")
            for dep in devel_deps:
                result_content.append(f'  "{dep}{get_python_exclusion(dependency_info)}",')
        result_content.append("]")
    result_content.append("all = [")
    generate_parsed_extras(ParsedDependencyTypes.CORE_EXTRAS)
    generate_parsed_extras(ParsedDependencyTypes.APACHE_NO_PROVIDER_EXTRAS)
    result_content.append("    # Provider extras")
    for dependency, dependency_info in dependencies.items():
        result_content.append(f'    "apache-airflow[{normalize_extra(dependency)}]",')
    result_content.append("]")
    result_content.append("devel-all = [")
    result_content.append('    "apache-airflow[all]",')
    result_content.append('    "apache-airflow[devel]",')
    result_content.append('    "apache-airflow[doc]",')
    result_content.append('    "apache-airflow[doc-gen]",')
    result_content.append('    "apache-airflow[saml]",')
    generate_parsed_extras(ParsedDependencyTypes.APACHE_NO_PROVIDER_EXTRAS)
    result_content.append("    # Include all provider deps")
    for dependency, dependency_info in dependencies.items():
        result_content.append(f'    "apache-airflow[{normalize_extra(dependency)}]",')
    result_content.append("]")


def get_dependency_type(dependency_type: str) -> ParsedDependencyTypes | None:
    for dep_type in ParsedDependencyTypes:
        if dep_type.value == dependency_type:
            return dep_type
    return None


def update_pyproject_toml(dependencies: dict[str, dict[str, list[str] | str]]):
    file_content = PYPROJECT_TOML_FILE_PATH.read_text()
    result_content: list[str] = []
    copying = True
    current_type: str | None = None
    line_count: int = 0
    for line in file_content.splitlines():
        if copying:
            result_content.append(line)
        if line.strip().startswith(GENERATED_DEPENDENCIES_START):
            copying = False
            generate_dependencies(result_content, dependencies)
        elif line.strip().startswith(GENERATED_DEPENDENCIES_END):
            copying = True
            result_content.append(line)
        elif line.strip().startswith("# START OF "):
            current_type = line.strip().replace("# START OF ", "")
            type_enum = get_dependency_type(current_type)
            if type_enum is None:
                console.print(
                    f"[red]Wrong start of section '{current_type}' in {PYPROJECT_TOML_FILE_PATH} "
                    f"at line {line_count}: Unknown section type"
                )
                sys.exit(1)
        elif line.strip().startswith("# END OF "):
            end_type = line.strip().replace("# END OF ", "")
            if end_type != current_type:
                console.print(
                    f"[red]Wrong end of section {end_type} in {PYPROJECT_TOML_FILE_PATH} at line {line_count}"
                )
                sys.exit(1)
        if current_type:
            if line.strip().endswith(" = ["):
                FOUND_EXTRAS[current_type].append(line.split(" = [")[0].strip())
        line_count += 1
    PYPROJECT_TOML_FILE_PATH.write_text("\n".join(result_content) + "\n")


def calculate_my_hash():
    my_file = MY_FILE.resolve()
    hash_md5 = hashlib.md5()
    hash_md5.update(my_file.read_bytes())
    return hash_md5.hexdigest()


if __name__ == "__main__":
    find_all_providers_and_provider_files()
    num_files = len(ALL_PROVIDER_FILES)
    num_providers = len(ALL_PROVIDERS)
    console.print(f"Found {len(ALL_PROVIDERS)} providers with {len(ALL_PROVIDER_FILES)} Python files.")
    for file in ALL_PROVIDER_FILES:
        check_if_different_provider_used(file)
    for provider, provider_yaml_content in ALL_PROVIDERS.items():
        ALL_DEPENDENCIES[provider]["deps"].extend(provider_yaml_content["dependencies"])
        ALL_DEPENDENCIES[provider]["devel-deps"].extend(provider_yaml_content.get("devel-dependencies") or [])
        STATES[provider] = provider_yaml_content["state"]
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
    unique_sorted_dependencies: dict[str, dict[str, list[str] | str]] = defaultdict(dict)
    for key in sorted(ALL_DEPENDENCIES.keys()):
        unique_sorted_dependencies[key]["deps"] = sorted(ALL_DEPENDENCIES[key]["deps"])
        unique_sorted_dependencies[key]["devel-deps"] = ALL_DEPENDENCIES[key].get("devel-deps") or []
        unique_sorted_dependencies[key]["cross-providers-deps"] = sorted(
            set(ALL_DEPENDENCIES[key]["cross-providers-deps"])
        )
        excluded_versions = ALL_PROVIDERS[key].get("excluded-python-versions")
        unique_sorted_dependencies[key]["excluded-python-versions"] = excluded_versions or []
        unique_sorted_dependencies[key]["state"] = STATES[key]
    if errors:
        console.print()
        console.print("[red]Errors found during verification. Exiting!")
        console.print()
        sys.exit(1)
    old_dependencies = (
        DEPENDENCIES_JSON_FILE_PATH.read_text() if DEPENDENCIES_JSON_FILE_PATH.exists() else "{}"
    )
    new_dependencies = json.dumps(unique_sorted_dependencies, indent=2) + "\n"
    old_md5sum = MY_MD5SUM_FILE.read_text().strip() if MY_MD5SUM_FILE.exists() else ""
    new_md5sum = calculate_my_hash()
    if new_dependencies != old_dependencies or new_md5sum != old_md5sum:
        DEPENDENCIES_JSON_FILE_PATH.write_text(json.dumps(unique_sorted_dependencies, indent=2) + "\n")
        if os.environ.get("CI"):
            console.print()
            console.print(f"There is a need to regenerate {DEPENDENCIES_JSON_FILE_PATH}")
            console.print(
                f"[red]You need to run the following command locally and commit generated "
                f"{DEPENDENCIES_JSON_FILE_PATH.relative_to(AIRFLOW_SOURCES_ROOT)} file:\n"
            )
            console.print("breeze static-checks --type update-providers-dependencies --all-files")
            console.print()
        else:
            console.print()
            console.print(
                f"[yellow]Regenerated new dependencies. Please commit "
                f"{DEPENDENCIES_JSON_FILE_PATH.relative_to(AIRFLOW_SOURCES_ROOT)}!\n"
            )
            console.print(f"Written {DEPENDENCIES_JSON_FILE_PATH}")
            console.print()
            update_pyproject_toml(unique_sorted_dependencies)
            console.print(f"Written {PYPROJECT_TOML_FILE_PATH}")
            console.print()
            MY_MD5SUM_FILE.write_text(new_md5sum + "\n")
        sys.exit(1)
    else:
        console.print(
            "[green]No need to regenerate dependencies!\n[/]"
            f"The {DEPENDENCIES_JSON_FILE_PATH.relative_to(AIRFLOW_SOURCES_ROOT)} is up to date!\n"
        )
