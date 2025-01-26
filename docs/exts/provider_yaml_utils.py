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
from functools import cache
from pathlib import Path
from typing import Any

import jsonschema
import yaml

ROOT_DIR = Path(__file__).parents[2].resolve()
AIRFLOW_PROVIDERS_DIR = ROOT_DIR / "providers"
AIRFLOW_PROVIDERS_SRC = AIRFLOW_PROVIDERS_DIR / "src"
AIRFLOW_PROVIDERS_NS_PACKAGE = AIRFLOW_PROVIDERS_SRC / "airflow" / "providers"
# TODO(potiuk) remove this when we move all providers from the old structure
OLD_PROVIDER_DATA_SCHEMA_PATH = ROOT_DIR / "airflow" / "provider.yaml.schema.json"
# TODO(potiuk) - rename when all providers are new-style
NEW_PROVIDER_DATA_SCHEMA_PATH = ROOT_DIR / "airflow" / "new_provider.yaml.schema.json"


# TODO(potiuk) - remove when all providers are new-style
@cache
def old_provider_yaml_schema() -> dict[str, Any]:
    with open(OLD_PROVIDER_DATA_SCHEMA_PATH) as schema_file:
        return json.load(schema_file)


# TODO(potiuk) - rename when all providers are new-style
@cache
def new_provider_yaml_schema() -> dict[str, Any]:
    with open(NEW_PROVIDER_DATA_SCHEMA_PATH) as schema_file:
        return json.load(schema_file)


def _provider_yaml_directory_to_module(provider_yaml_directory_path: str) -> str:
    return str(Path(provider_yaml_directory_path).relative_to(AIRFLOW_PROVIDERS_SRC)).replace("/", ".")


def _filepath_to_system_tests_str(provider_yaml_directory_path: str) -> str:
    return (
        ROOT_DIR
        / "providers"
        / "tests"
        / "system"
        / Path(provider_yaml_directory_path).relative_to(AIRFLOW_PROVIDERS_NS_PACKAGE)
    ).as_posix()


# TODO(potiuk) - rename when all providers are new-style
def _get_new_provider_root_path(provider_yaml_directory_path: Path) -> Path:
    for parent in Path(provider_yaml_directory_path).parents:
        if (parent / "src").exists():
            return parent
    raise ValueError(
        f"The path {provider_yaml_directory_path} should "
        f"be provider path under `providers/<PROVIDER>/src` folder.`"
    )


# TODO(potiuk) - rename when all providers are new-style
def _new_filepath_to_system_tests(provider_yaml_directory_path: Path) -> Path:
    test_root_path = _get_new_provider_root_path(provider_yaml_directory_path) / "tests"
    return (test_root_path / "system").relative_to(AIRFLOW_PROVIDERS_DIR)


@cache
def get_all_provider_yaml_paths() -> list[Path]:
    """Returns list of all provider.yaml files including new and old structure."""
    return sorted(
        list(AIRFLOW_PROVIDERS_DIR.glob("**/src/airflow/providers/**/provider.yaml"))
        +
        # TODO(potiuk) remove this when we move all providers from the old structure
        list(AIRFLOW_PROVIDERS_NS_PACKAGE.glob("**/provider.yaml"))
    )


# TODO(potiuk) remove this when we move all providers from the old structure
@cache
def get_old_provider_yaml_paths() -> list[Path]:
    """Returns list of provider.yaml files for old structure"""
    return sorted(AIRFLOW_PROVIDERS_NS_PACKAGE.rglob("**/provider.yaml"))


# TODO(potiuk) - rename when all providers are new-style
@cache
def get_new_provider_yaml_paths() -> list[Path]:
    """Returns list of provider.yaml files for new structure"""
    return sorted(
        list(set(AIRFLOW_PROVIDERS_DIR.rglob("**/provider.yaml")) - set(get_old_provider_yaml_paths()))
    )


@cache
def load_package_data(include_suspended: bool = False) -> list[dict[str, Any]]:
    """
    Load all data from providers files

    :return: A list containing the contents of all provider.yaml files - old and new structure.
    """
    # TODO(potiuk) - remove when all providers are new-style
    schema = old_provider_yaml_schema()
    new_schema = new_provider_yaml_schema()
    result = []
    # TODO(potiuk) - rename when all providers are new-style
    for new_provider_yaml_path in get_new_provider_yaml_paths():
        with open(new_provider_yaml_path) as yaml_file:
            provider = yaml.safe_load(yaml_file)
        try:
            jsonschema.validate(provider, schema=new_schema)
        except jsonschema.ValidationError as ex:
            msg = f"Unable to parse: {new_provider_yaml_path}. Original error {type(ex).__name__}: {ex}"
            raise RuntimeError(msg)
        if provider["state"] == "suspended" and not include_suspended:
            continue
        provider_yaml_dir_str = os.path.dirname(new_provider_yaml_path)
        module = provider["package-name"][len("apache-") :].replace("-", ".")
        module_folder = module[len("airflow-providers-") :].replace(".", "/")
        provider["python-module"] = module
        provider["package-dir"] = f"{provider_yaml_dir_str}/src/{module.replace('.', '/')}"
        provider["docs-dir"] = os.path.dirname(new_provider_yaml_path.parent / "docs")
        provider["system-tests-dir"] = f"{provider_yaml_dir_str}/tests/system/{module_folder}"
        # TODO(potiuk) - remove when all providers are new-style
        provider["is_new_provider"] = True
        result.append(provider)
    # TODO(potiuk): Remove me when all providers are moved ALL_PROVIDERS
    for provider_yaml_path in get_old_provider_yaml_paths():
        with open(provider_yaml_path) as yaml_file:
            provider = yaml.safe_load(yaml_file)
        try:
            jsonschema.validate(provider, schema=schema)
        except jsonschema.ValidationError as ex:
            msg = f"Unable to parse: {provider_yaml_path}. Original error {type(ex).__name__}: {ex}"
            raise RuntimeError(msg)
        if provider["state"] == "suspended" and not include_suspended:
            continue
        old_provider_yaml_dir_str = os.path.dirname(provider_yaml_path)
        provider["python-module"] = _provider_yaml_directory_to_module(old_provider_yaml_dir_str)
        provider["package-dir"] = old_provider_yaml_dir_str
        provider["system-tests-dir"] = _filepath_to_system_tests_str(old_provider_yaml_dir_str)
        provider["is_new_provider"] = False
        result.append(provider)

    return result
