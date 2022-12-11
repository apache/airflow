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

from __future__ import annotations

import json
import os
import re
import shutil
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

import jsonschema
import yaml
from jinja2 import Template
from rich.console import Console

console = Console(width=400, color_system="standard")

ALL_PYTHON_VERSIONS = ["3.7", "3.8", "3.9", "3.10"]

AIRFLOW_SOURCES_ROOT_PATH = Path(__file__).parents[2].resolve()

TEMPLATES_DIR = Path(__file__).parent / "templates"

PYPROJECT_TOML_TEMPLATE_FILE = TEMPLATES_DIR / "PYPROJECT_TEMPLATE.toml.jinja2"
GET_PROVIDER_INFO_TEMPLATE_FILE = TEMPLATES_DIR / "get_provider_info_TEMPLATE.py.jinja2"
README_TEMPLATE_FILE = TEMPLATES_DIR / "PROVIDER_README_TEMPLATE.rst.jinja2"

ORIGIN_PROVIDERS_DIR = AIRFLOW_SOURCES_ROOT_PATH / "airflow" / "providers"
ORIGIN_UNIT_TESTS_PROVIDERS_DIR = AIRFLOW_SOURCES_ROOT_PATH / "tests" / "providers"
ORIGIN_INTEGRATION_TESTS_PROVIDERS_DIR = AIRFLOW_SOURCES_ROOT_PATH / "tests" / "integration" / "providers"
ORIGIN_SYSTEM_TESTS_PROVIDERS_DIR = AIRFLOW_SOURCES_ROOT_PATH / "tests" / "system" / "providers"
ORIGIN_DOCS_DIR = AIRFLOW_SOURCES_ROOT_PATH / "docs"
ORIGIN_SYSTEM_CONFTEST_PY_FILE_PATH = AIRFLOW_SOURCES_ROOT_PATH / "tests" / "system" / "conftest.py"

TARGET_PROVIDER_DIR = AIRFLOW_SOURCES_ROOT_PATH / "providers"
PROVIDER_DEPENDENCIES_JSON_FILE = AIRFLOW_SOURCES_ROOT_PATH / "generated" / "provider_dependencies.json"
PROVIDER_RUNTIME_DATA_SCHEMA_PATH = AIRFLOW_SOURCES_ROOT_PATH / "airflow" / "provider_info.schema.json"
PROVIDER_DEPENDENCIES = json.loads(PROVIDER_DEPENDENCIES_JSON_FILE.read_text())

LICENSE_RST = """
.. Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.
"""


@dataclass()
class ProviderMoveInfo:
    source_provider_path: Path
    provider_relative_path: Path = field(init=False)
    provider_id: str = field(init=False)
    origin_provider_unit_tests_path: Path = field(init=False)
    origin_provider_integration_tests_path: Path = field(init=False)
    origin_provider_system_tests_path: Path = field(init=False)
    origin_docs_dir: Path = field(init=False)
    target_provider_path: Path = field(init=False)
    target_provider_package_sources_path: Path = field(init=False)
    target_provider_unit_tests_path: Path = field(init=False)
    target_provider_integration_tests_path: Path = field(init=False)
    target_provider_system_tests_path: Path = field(init=False)
    target_pyproject_toml_file: Path = field(init=False)
    target_provider_yaml_file: Path = field(init=False)
    target_provider_readme_file: Path = field(init=False)
    target_get_provider_info_file: Path = field(init=False)
    target_docs_dir: Path = field(init=False)
    target_changelog_file: Path = field(init=False)

    def __post_init__(self):
        self.provider_relative_path = self.source_provider_path.relative_to(ORIGIN_PROVIDERS_DIR)
        self.provider_id = self.provider_relative_path.as_posix().replace("/", ".")
        self.origin_provider_unit_tests_path = ORIGIN_UNIT_TESTS_PROVIDERS_DIR / self.provider_relative_path
        self.origin_provider_integration_tests_path = (
            ORIGIN_INTEGRATION_TESTS_PROVIDERS_DIR / self.provider_relative_path
        )
        self.origin_provider_system_tests_path = (
            ORIGIN_SYSTEM_TESTS_PROVIDERS_DIR / self.provider_relative_path
        )
        self.origin_docs_dir = (
            ORIGIN_DOCS_DIR / f"apache-airflow-providers-{self.provider_id.replace('.', '-')}"
        )
        self.target_provider_path = TARGET_PROVIDER_DIR / self.source_provider_path.relative_to(
            ORIGIN_PROVIDERS_DIR
        )
        self.target_provider_package_sources_path = (
            self.target_provider_path / "src" / "airflow" / "providers" / self.provider_relative_path
        )
        self.target_provider_unit_tests_path = (
            self.target_provider_path / "tests" / "airflow" / "providers" / self.provider_relative_path
        )
        self.target_provider_integration_tests_path = (
            self.target_provider_path
            / "tests"
            / "integration"
            / "airflow"
            / "providers"
            / self.provider_relative_path
        )
        self.target_provider_system_tests_path = (
            self.target_provider_path
            / "tests"
            / "system"
            / "airflow"
            / "providers"
            / self.provider_relative_path
        )
        self.target_pyproject_toml_file = self.target_provider_path / "pyproject.toml"
        self.target_provider_yaml_file = self.target_provider_path / "provider.yaml"
        self.target_provider_readme_file = self.target_provider_path / "README.rst"
        self.target_get_provider_info_file = (
            self.target_provider_package_sources_path / "get_provider_info.py"
        )
        self.target_docs_dir = self.target_provider_path / "docs"
        self.target_changelog_file = self.target_docs_dir / "CHANGELOG.rst"

    def get_dict(self):
        _dict_repr = asdict(self)
        for key, value in _dict_repr.items():
            if isinstance(value, Path):
                _dict_repr[key] = os.fspath(value)
        return _dict_repr


def validate_provider_info_with_runtime_schema(provider_info: dict[str, Any]) -> None:
    """
    Validates provider info against the runtime schema. This way we check if the provider info in the
    packages is future-compatible. The Runtime Schema should only change when there is a major version
    change.

    :param provider_info: provider info to validate
    """

    with open(PROVIDER_RUNTIME_DATA_SCHEMA_PATH) as schema_file:
        schema = json.load(schema_file)
    try:
        jsonschema.validate(provider_info, schema=schema)
    except jsonschema.ValidationError as ex:
        raise Exception(
            "Error when validating schema. The schema must be compatible with "
            "airflow/provider_info.schema.json.",
            ex,
        )


def black_format(content) -> str:
    from black import format_str

    return format_str(content, mode=black_mode())


@lru_cache(maxsize=None)
def black_mode():
    from black import Mode, parse_pyproject_toml, target_version_option_callback

    config = parse_pyproject_toml(os.path.join(AIRFLOW_SOURCES_ROOT_PATH, "pyproject.toml"))

    target_versions = set(
        target_version_option_callback(None, None, tuple(config.get("target_version", ()))),
    )

    return Mode(
        target_versions=target_versions,
        line_length=config.get("line_length", Mode.line_length),
        is_pyi=bool(config.get("is_pyi", Mode.is_pyi)),
        string_normalization=not bool(config.get("skip_string_normalization", not Mode.string_normalization)),
        experimental_string_processing=bool(
            config.get("experimental_string_processing", Mode.experimental_string_processing)
        ),
    )


def find_providers_dirs() -> list[ProviderMoveInfo]:
    providers_dirs = []
    for file in ORIGIN_PROVIDERS_DIR.rglob("provider.yaml"):
        providers_dirs.append(ProviderMoveInfo(file.parent.resolve()))
    return providers_dirs


def moving(source_path: Path, target_path: Path, create_if_not_exists: bool = True) -> None:
    console.print(f"[bright_blue]Moving {source_path} -> {target_path}")
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if not source_path.exists():
        if create_if_not_exists:
            target_path.touch()
            return
        else:
            raise Exception(f"Source path {source_path} does not exist!")
    source_path.rename(target_path)


def move_provider_sources(move_info: ProviderMoveInfo):
    console.print(f"[yellow]Moving provider: {move_info.provider_id}")
    if move_info.target_provider_path.exists():
        raise Exception(f"Target directory {move_info.target_provider_path} already exists. Skipping.")
    moving(move_info.source_provider_path, move_info.target_provider_package_sources_path)
    if move_info.origin_provider_unit_tests_path.exists():
        moving(move_info.origin_provider_unit_tests_path, move_info.target_provider_unit_tests_path)
    if move_info.origin_provider_integration_tests_path.exists():
        moving(
            move_info.origin_provider_integration_tests_path,
            move_info.target_provider_integration_tests_path,
        )
    if move_info.origin_provider_system_tests_path.exists():
        moving(move_info.origin_provider_system_tests_path, move_info.target_provider_system_tests_path)
        shutil.copy(
            ORIGIN_SYSTEM_CONFTEST_PY_FILE_PATH, move_info.target_provider_system_tests_path / "conftest.py"
        )


def escape_list_of_dependencies(deps: list[str]) -> list[str]:
    return [dependency.replace('"', '\\"') for dependency in deps]


def generate_pyproject_toml(move_info: ProviderMoveInfo):
    console.print(f"[yellow]Generating {move_info.target_provider_yaml_file}")
    provider_yaml_info = yaml.safe_load(move_info.target_provider_yaml_file.read_text())
    provider_yaml_info["supported_python_versions"] = get_supported_python_versions(provider_yaml_info)
    optional_dependencies: dict[str, list[str]] = defaultdict(list)
    cross_deps = PROVIDER_DEPENDENCIES[move_info.provider_id].get("cross-provider-deps")
    if cross_deps:
        for extra in cross_deps:
            optional_dependencies[extra].append(f"apache-airflow-providers-{extra.replace('.', '-')}")
    if provider_yaml_info.get("additional-extras"):
        for extra in provider_yaml_info["additional-extras"]:
            name = extra["name"]
            escaped_deps = escape_list_of_dependencies(extra["dependencies"])
            for escape_dep in escaped_deps:
                base_package = escape_dep.strip("<>=~")
                if optional_dependencies.get(name):
                    # Remove automatically added non-versioned cross-dependency package if already present
                    optional_dependencies[name].remove(base_package)
                optional_dependencies[name].append(escape_dep)

    provider_yaml_info["optional_dependencies"] = optional_dependencies
    provider_yaml_info["package_id"] = move_info.provider_id
    provider_yaml_info["package_name"] = provider_yaml_info["package-name"]
    escaped_dependencies = escape_list_of_dependencies(provider_yaml_info["dependencies"])
    provider_yaml_info["escaped_dependencies"] = escaped_dependencies
    template = Template(PYPROJECT_TOML_TEMPLATE_FILE.read_text())
    rendered = template.render(provider_yaml_info)
    move_info.target_pyproject_toml_file.write_text(rendered)


def get_supported_python_versions(provider_info: dict[str, Any]) -> list[str]:
    excluded_versions = provider_info.get("excluded_python_versions")
    if not excluded_versions:
        return ALL_PYTHON_VERSIONS
    return [p for p in ALL_PYTHON_VERSIONS if p not in excluded_versions]


def generate_get_provider_info(move_info: ProviderMoveInfo):
    console.print(f"[yellow]Generating {move_info.target_get_provider_info_file}")
    provider_yaml_info = yaml.safe_load(move_info.target_provider_yaml_file.read_text())
    validate_provider_info_with_runtime_schema(provider_yaml_info)
    context = {"PROVIDER_INFO": provider_yaml_info}
    template = Template(GET_PROVIDER_INFO_TEMPLATE_FILE.read_text())
    rendered = template.render(context)
    move_info.target_get_provider_info_file.write_text(black_format(rendered))


def convert_pip_requirements_to_table(requirements: list[str]) -> str:
    """
    Converts PIP requirement list to an RST table.
    :param requirements: requirements list
    :return: formatted table
    """
    from tabulate import tabulate

    headers = ["PIP package", "Version required"]
    table_data: list[tuple[str, str]] = []
    for dependency in requirements:
        found = re.match(r"(^[^<=>~]*)([^<=>~]?.*)$", dependency)
        if found:
            package = found.group(1)
            version_required = found.group(2)
            if version_required != "":
                version_required = f"``{version_required}``"
            table_data.append((f"``{package}``", version_required))
        else:
            table_data.append((dependency, ""))
    return tabulate(table_data, headers=headers, tablefmt="rst")


def convert_cross_package_dependencies_to_table(
    cross_package_dependencies: list[str],
) -> str:
    """
    Converts cross-package dependencies to an RST table
    :param cross_package_dependencies: list of cross-package dependencies
    :return: formatted table
    """
    from tabulate import tabulate

    headers = ["Dependent package", "Extra"]
    table_data = []
    prefix = "apache-airflow-providers-"
    base_url = "https://airflow.apache.org/docsdocs/"
    for dependency in cross_package_dependencies:
        pip_package_name = f"{prefix}{dependency.replace('.','-')}"
        url_suffix = f"{dependency.replace('.','-')}"
        url = f"`{pip_package_name} <{base_url}{prefix}{url_suffix}>`_"
        table_data.append((url, f"``{dependency}``"))
    return tabulate(table_data, headers=headers, tablefmt="rst")


def generate_readme_file(move_info: ProviderMoveInfo):
    console.print(f"[yellow]Generating {move_info.target_provider_readme_file}")
    provider_yaml_info = yaml.safe_load(move_info.target_provider_yaml_file.read_text())
    context = {
        "PACKAGE_PIP_NAME": provider_yaml_info["package-name"],
        "RELEASE": provider_yaml_info["versions"][0],
        "FULL_PACKAGE_NAME": f"airflow.providers.{move_info.provider_id}",
        "VERSION_SUFFIX": "",
        "PROVIDER_DESCRIPTION": provider_yaml_info["description"],
        "PROVIDER_PACKAGE_ID": move_info.provider_id,
        "SUPPORTED_PYTHON_VERSIONS": get_supported_python_versions(provider_yaml_info),
        "PIP_REQUIREMENTS": provider_yaml_info["dependencies"],
        "PIP_REQUIREMENTS_TABLE_RST": convert_pip_requirements_to_table(provider_yaml_info["dependencies"]),
        "CROSS_PROVIDERS_DEPENDENCIES": PROVIDER_DEPENDENCIES[move_info.provider_id].get(
            "cross-provider-deps"
        ),
        "CROSS_PROVIDERS_DEPENDENCIES_TABLE_RST": convert_cross_package_dependencies_to_table(
            PROVIDER_DEPENDENCIES[move_info.provider_id]
        ),
        "CHANGELOG": move_info.target_changelog_file.read_text(),
    }
    template = Template(README_TEMPLATE_FILE.read_text())
    readme_content = LICENSE_RST + template.render(context)
    move_info.target_provider_readme_file.write_text(readme_content)


def generate_other_files(move_info: ProviderMoveInfo):
    context = yaml.safe_load(move_info.target_provider_yaml_file.read_text())
    context["package_name"] = context["package-name"]
    for file_name in ["INSTALL.txt", "LICENSE.txt", "NOTICE.txt"]:
        template = Template((TEMPLATES_DIR / file_name).with_suffix(".txt.jinja2").read_text())
        file_content = template.render(context)
        (move_info.target_provider_path / file_name).write_text(file_content)


def move_provider(provider_info: ProviderMoveInfo):
    console.print(provider_info.get_dict())
    move_provider_sources(provider_info)
    moving(provider_info.origin_docs_dir, provider_info.target_docs_dir)
    moving(
        provider_info.target_provider_package_sources_path / "CHANGELOG.rst",
        provider_info.target_changelog_file,
    )
    moving(
        provider_info.target_provider_package_sources_path / "provider.yaml",
        provider_info.target_provider_yaml_file,
    )
    moving(
        provider_info.target_provider_package_sources_path / ".latest-doc-only-change.txt",
        provider_info.target_provider_path / ".latest-doc-only-change.txt",
        create_if_not_exists=True,
    )
    generate_pyproject_toml(provider_info)
    generate_get_provider_info(provider_info)
    generate_readme_file(provider_info)
    generate_other_files(provider_info)


def move_all_providers():
    for api_dir in ORIGIN_DOCS_DIR.rglob("_api"):
        console.print(f"[yellow]Removing {api_dir}")
        shutil.rmtree(api_dir)
    shutil.rmtree(ORIGIN_DOCS_DIR / "_build", ignore_errors=True)
    shutil.rmtree(ORIGIN_DOCS_DIR / "_doctrees", ignore_errors=True)
    shutil.rmtree(ORIGIN_DOCS_DIR / "_inventory_cache", ignore_errors=True)
    for info in find_providers_dirs():
        move_provider(provider_info=info)

    shutil.rmtree(ORIGIN_PROVIDERS_DIR, ignore_errors=True)
    shutil.rmtree(ORIGIN_INTEGRATION_TESTS_PROVIDERS_DIR, ignore_errors=True)
    shutil.rmtree(ORIGIN_SYSTEM_TESTS_PROVIDERS_DIR, ignore_errors=True)
    ORIGIN_SYSTEM_CONFTEST_PY_FILE_PATH.unlink()


if __name__ == "__main__":
    move_all_providers()
