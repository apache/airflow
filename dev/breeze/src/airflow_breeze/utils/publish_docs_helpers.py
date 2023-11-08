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

import fnmatch
import json
import os
from glob import glob
from pathlib import Path
from typing import Any

import yaml

from airflow_breeze.utils.general_utils import get_docs_filter_name_from_short_hand
from airflow_breeze.utils.suspended_providers import get_removed_provider_ids

CONSOLE_WIDTH = 180

ROOT_DIR = Path(__file__).parents[5].resolve()
PROVIDER_DATA_SCHEMA_PATH = ROOT_DIR / "airflow" / "provider.yaml.schema.json"


def _load_schema() -> dict[str, Any]:
    with open(PROVIDER_DATA_SCHEMA_PATH) as schema_file:
        content = json.load(schema_file)
    return content


def _filepath_to_module(filepath: str):
    return str(Path(filepath).relative_to(ROOT_DIR)).replace("/", ".")


def _filepath_to_system_tests(filepath: str):
    return str(
        ROOT_DIR
        / "tests"
        / "system"
        / "providers"
        / Path(filepath).relative_to(ROOT_DIR / "airflow" / "providers")
    )


def get_provider_yaml_paths():
    """Returns list of provider.yaml files"""
    return sorted(glob(f"{ROOT_DIR}/airflow/providers/**/provider.yaml", recursive=True))


def load_package_data(include_suspended: bool = False) -> list[dict[str, Any]]:
    """
    Load all data from providers files

    :return: A list containing the contents of all provider.yaml files.
    """
    import jsonschema

    schema = _load_schema()
    result = []
    for provider_yaml_path in get_provider_yaml_paths():
        with open(provider_yaml_path) as yaml_file:
            provider = yaml.safe_load(yaml_file)
        try:
            jsonschema.validate(provider, schema=schema)
        except jsonschema.ValidationError:
            raise Exception(f"Unable to parse: {provider_yaml_path}.")
        if provider["suspended"] and not include_suspended:
            continue
        provider_yaml_dir = os.path.dirname(provider_yaml_path)
        provider["python-module"] = _filepath_to_module(provider_yaml_dir)
        provider["package-dir"] = provider_yaml_dir
        provider["system-tests-dir"] = _filepath_to_system_tests(provider_yaml_dir)
        result.append(provider)
    return result


def get_available_packages(include_suspended: bool = False):
    """Get list of all available packages to build."""
    all_providers_yaml = load_package_data(include_suspended=include_suspended)
    provider_package_names = [provider["package-name"] for provider in all_providers_yaml]
    return [
        "apache-airflow",
        "docker-stack",
        *provider_package_names,
        "apache-airflow-providers",
        "helm-chart",
    ]


def process_package_filters(
    available_packages: list[str], package_filters: list[str] | None, packages_short_form: tuple[str]
):
    """Filters the package list against a set of filters.

    A packet is returned if it matches at least one filter. The function keeps the order of the packages.
    """
    if not package_filters and not packages_short_form:
        return available_packages

    package_filters = list(package_filters + get_docs_filter_name_from_short_hand(packages_short_form))

    removed_packages = [
        f"apache-airflow-providers-{provider.replace('.','-')}" for provider in get_removed_provider_ids()
    ]
    all_packages_including_removed = available_packages + removed_packages
    invalid_filters = [
        f for f in package_filters if not any(fnmatch.fnmatch(p, f) for p in all_packages_including_removed)
    ]
    if invalid_filters:
        raise SystemExit(
            f"Some filters did not find any package: {invalid_filters}, Please check if they are correct."
        )

    return [p for p in all_packages_including_removed if any(fnmatch.fnmatch(p, f) for f in package_filters)]


def pretty_format_path(path: str, start: str) -> str:
    """Formats path nicely."""
    relpath = os.path.relpath(path, start)
    if relpath == path:
        return path
    return f"{start}/{relpath}"


def prepare_code_snippet(file_path: str, line_no: int, context_lines_count: int = 5) -> str:
    """
    Prepare code snippet with line numbers and  a specific line marked.

    :param file_path: File name
    :param line_no: Line number
    :param context_lines_count: The number of lines that will be cut before and after.
    :return: str
    """
    with open(file_path) as text_file:
        # Highlight code
        code = text_file.read()
        code_lines = code.splitlines()
        # Prepend line number
        code_lines = [
            f">{lno:3} | {line}" if line_no == lno else f"{lno:4} | {line}"
            for lno, line in enumerate(code_lines, 1)
        ]
        # # Cut out the snippet
        start_line_no = max(0, line_no - context_lines_count - 1)
        end_line_no = line_no + context_lines_count
        code_lines = code_lines[start_line_no:end_line_no]
        # Join lines
        code = "\n".join(code_lines)
    return code
