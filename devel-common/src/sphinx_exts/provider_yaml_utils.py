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

AIRFLOW_ROOT_PATH = Path(__file__).parents[3].resolve()
AIRFLOW_PROVIDERS_PATH = AIRFLOW_ROOT_PATH / "providers"
AIRFLOW_PROVIDERS_SRC = AIRFLOW_PROVIDERS_PATH / "src"
PROVIDER_DATA_SCHEMA_PATH = (
    AIRFLOW_ROOT_PATH / "airflow-core" / "src" / "airflow" / "provider.yaml.schema.json"
)


@cache
def provider_yaml_schema() -> dict[str, Any]:
    with open(PROVIDER_DATA_SCHEMA_PATH) as schema_file:
        return json.load(schema_file)


def _provider_yaml_directory_to_module(provider_yaml_directory_path: str) -> str:
    return str(Path(provider_yaml_directory_path).relative_to(AIRFLOW_PROVIDERS_SRC)).replace("/", ".")


def _get_provider_root_path(provider_yaml_directory_path: Path) -> Path:
    for parent in Path(provider_yaml_directory_path).parents:
        if (parent / "src").exists():
            return parent
    raise ValueError(
        f"The path {provider_yaml_directory_path} should "
        f"be provider path under `providers/<PROVIDER>/src` folder.`"
    )


def _filepath_to_system_tests(provider_yaml_directory_path: Path) -> Path:
    test_root_path = _get_provider_root_path(provider_yaml_directory_path) / "tests"
    return (test_root_path / "system").relative_to(AIRFLOW_PROVIDERS_PATH)


@cache
def get_all_provider_yaml_paths() -> list[Path]:
    """Returns list of all provider.yaml files including new and old structure."""
    return sorted(list(AIRFLOW_PROVIDERS_PATH.glob("**/provider.yaml")))


@cache
def load_package_data(include_suspended: bool = False) -> list[dict[str, Any]]:
    """
    Load all data from providers files

    :return: A list containing the contents of all provider.yaml files - old and new structure.
    """
    schema = provider_yaml_schema()
    result = []
    for provider_yaml_path in get_all_provider_yaml_paths():
        with open(provider_yaml_path) as yaml_file:
            provider = yaml.safe_load(yaml_file)
        try:
            jsonschema.validate(provider, schema=schema)
        except jsonschema.ValidationError as ex:
            msg = f"Unable to parse: {provider_yaml_path}. Original error {type(ex).__name__}: {ex}"
            raise RuntimeError(msg)
        if provider["state"] == "suspended" and not include_suspended:
            continue
        provider_yaml_dir_str = os.path.dirname(provider_yaml_path)
        module = provider["package-name"][len("apache-") :].replace("-", ".")
        module_folder = module[len("airflow-providers-") :].replace(".", "/")
        provider["python-module"] = module
        provider["package-dir"] = f"{provider_yaml_dir_str}/src/{module.replace('.', '/')}"
        provider["docs-dir"] = os.path.dirname(provider_yaml_path.parent / "docs")
        provider["system-tests-dir"] = f"{provider_yaml_dir_str}/tests/system/{module_folder}"
        result.append(provider)
    return result
