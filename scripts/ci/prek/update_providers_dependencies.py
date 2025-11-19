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
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "pyyaml>=6.0.3",
#   "rich>=13.6.0",
#   "tomli>=2.0.1",
# ]
# ///
from __future__ import annotations

import json
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import yaml

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_prek_utils is imported
from common_prek_utils import (
    AIRFLOW_CORE_SOURCES_PATH,
    AIRFLOW_PROVIDERS_ROOT_PATH,
    AIRFLOW_ROOT_PATH,
    console,
    get_imports_from_file,
)

AIRFLOW_PROVIDERS_IMPORT_PREFIX = "airflow.providers."

DEPENDENCIES_JSON_FILE_PATH = AIRFLOW_ROOT_PATH / "generated" / "provider_dependencies.json"

PYPROJECT_TOML_FILE_PATH = AIRFLOW_ROOT_PATH / "pyproject.toml"

MY_FILE = Path(__file__).resolve()
PROVIDERS: set[str] = set()

PYPROJECT_TOML_CONTENT: dict[str, dict[str, Any]] = {}

sys.path.insert(0, str(AIRFLOW_CORE_SOURCES_PATH))  # make sure setup is imported from Airflow

warnings: list[str] = []
errors: list[str] = []

suspended_paths: list[str] = []

ALL_DEPENDENCIES: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))

ALL_PROVIDERS: dict[str, dict[str, Any]] = defaultdict(lambda: defaultdict())
ALL_PROVIDER_FILES: list[Path] = []


def load_pyproject_toml(pyproject_toml_file_path: Path) -> dict[str, Any]:
    try:
        import tomllib
    except ImportError:
        import tomli as tomllib
    return tomllib.loads(pyproject_toml_file_path.read_text())


def find_all_providers_and_provider_files():
    for root, dirs, filenames in os.walk(AIRFLOW_PROVIDERS_ROOT_PATH):
        for filename in filenames:
            if filename == "provider.yaml":
                provider_yaml_file = Path(root, filename)
                provider_name = str(
                    provider_yaml_file.parent.relative_to(AIRFLOW_PROVIDERS_ROOT_PATH)
                ).replace(os.sep, ".")
                PROVIDERS.add(provider_name)
                PYPROJECT_TOML_CONTENT[provider_name] = load_pyproject_toml(
                    provider_yaml_file.parent / "pyproject.toml"
                )
                # only descend to "src" directory in the new structure
                # this avoids descending into .venv or "build" directories in case
                # someone works on providers in a separate virtualenv
                if "src" in dirs:
                    dirs[:] = ["src"]
                else:
                    raise ValueError(
                        f"The provider {provider_name} does not have 'src' folder"
                        f" in {provider_yaml_file.parent}"
                    )
                provider_info = yaml.safe_load(provider_yaml_file.read_text())
                if provider_info["state"] == "suspended":
                    suspended_paths.append(
                        provider_yaml_file.parent.relative_to(AIRFLOW_PROVIDERS_ROOT_PATH).as_posix()
                    )
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


def get_provider_id_from_path(file_path: Path) -> str | None:
    """
    Get the provider id from the path of the file it belongs to.
    """
    for parent in file_path.parents:
        # This works fine for both new and old providers structure - because we moved provider.yaml to
        # the top-level of the provider and this code finding "providers"  will find the "providers" package
        # in old structure and "providers" directory in new structure - in both cases we can determine
        # the provider id from the relative folders
        if (parent / "provider.yaml").exists():
            for providers_root_candidate in parent.parents:
                if providers_root_candidate.name == "providers":
                    return parent.relative_to(providers_root_candidate).as_posix().replace("/", ".")
            return None
    return None


def check_if_different_provider_used(file_path: Path) -> None:
    file_provider = get_provider_id_from_path(file_path)
    if not file_provider:
        return
    imports = get_imports_from_file(file_path, only_top_level=False)
    for import_name in imports:
        imported_provider = get_provider_id_from_import(import_name, file_path)
        if imported_provider is not None and imported_provider not in ALL_PROVIDERS:
            warnings.append(f"The provider {imported_provider} from {file_path} cannot be found.")
            continue

        if "/example_dags/" in file_path.as_posix():
            # If provider is used in a example dags, we don't want to mark this
            # as a provider cross dependency
            continue
        if imported_provider == "standard" and file_path.name == "celery_executor_utils.py":
            # some common standard operators are pre-imported in celery when it starts in order to speed
            # up the task startup time - but it does not mean that standard provider is a cross-provider
            # dependency of the celery executor
            continue
        if imported_provider:
            if file_provider != imported_provider:
                ALL_DEPENDENCIES[file_provider]["cross-providers-deps"].append(imported_provider)


STATES: dict[str, str] = {}

FOUND_EXTRAS: dict[str, list[str]] = defaultdict(list)

if __name__ == "__main__":
    find_all_providers_and_provider_files()
    num_files = len(ALL_PROVIDER_FILES)
    num_providers = len(ALL_PROVIDERS)
    console.print(f"Refreshed {num_providers} providers with {num_files} Python files.")
    for file in ALL_PROVIDER_FILES:
        check_if_different_provider_used(file)
    for provider in sorted(ALL_PROVIDERS.keys()):
        provider_yaml_content = ALL_PROVIDERS[provider]
        if provider in PROVIDERS:
            ALL_DEPENDENCIES[provider]["deps"].extend(
                PYPROJECT_TOML_CONTENT[provider]["project"]["dependencies"]
            )
            dependency_groups = PYPROJECT_TOML_CONTENT[provider].get("dependency-groups")
            if dependency_groups and dependency_groups.get("dev"):
                ALL_DEPENDENCIES[provider]["devel-deps"].extend(
                    [dep for dep in dependency_groups["dev"] if not dep.startswith("apache-airflow")]
                )
        else:
            ALL_DEPENDENCIES[provider]["deps"].extend(provider_yaml_content["dependencies"])
        ALL_DEPENDENCIES[provider]["plugins"].extend(provider_yaml_content.get("plugins") or [])
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
        unique_sorted_dependencies[key]["devel-deps"] = sorted(ALL_DEPENDENCIES[key]["devel-deps"])
        unique_sorted_dependencies[key]["plugins"] = sorted(ALL_DEPENDENCIES[key]["plugins"])
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
    old_content = DEPENDENCIES_JSON_FILE_PATH.read_text() if DEPENDENCIES_JSON_FILE_PATH.exists() else ""
    new_content = json.dumps(unique_sorted_dependencies, indent=2) + "\n"
    DEPENDENCIES_JSON_FILE_PATH.write_text(new_content)
    if new_content != old_content:
        console.print()
        console.print(f"Written {DEPENDENCIES_JSON_FILE_PATH}")
        console.print()
