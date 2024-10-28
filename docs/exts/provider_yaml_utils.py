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
from functools import lru_cache
from pathlib import Path
from typing import Any

import jsonschema
import yaml

ROOT_DIR = Path(__file__).parents[2].resolve()
AIRFLOW_PROVIDERS_SRC = ROOT_DIR / "providers" / "src"
AIRFLOW_PROVIDERS_NS_PACKAGE = AIRFLOW_PROVIDERS_SRC / "airflow" / "providers"
PROVIDER_DATA_SCHEMA_PATH = ROOT_DIR / "airflow" / "provider.yaml.schema.json"


def _load_schema() -> dict[str, Any]:
    with open(PROVIDER_DATA_SCHEMA_PATH) as schema_file:
        content = json.load(schema_file)
    return content


def _filepath_to_module(filepath: str):
    return str(Path(filepath).relative_to(AIRFLOW_PROVIDERS_SRC)).replace("/", ".")


def _filepath_to_system_tests(filepath: str):
    return str(
        ROOT_DIR
        / "providers"
        / "tests"
        / "system"
        / Path(filepath).relative_to(AIRFLOW_PROVIDERS_NS_PACKAGE)
    )


@lru_cache
def get_provider_yaml_paths():
    """Returns list of provider.yaml files"""
    return sorted(AIRFLOW_PROVIDERS_NS_PACKAGE.rglob("**/provider.yaml"))


@lru_cache
def load_package_data(include_suspended: bool = False) -> list[dict[str, Any]]:
    """
    Load all data from providers files

    :return: A list containing the contents of all provider.yaml files.
    """
    schema = _load_schema()
    result = []
    for provider_yaml_path in get_provider_yaml_paths():
        with open(provider_yaml_path) as yaml_file:
            provider = yaml.safe_load(yaml_file)
        try:
            jsonschema.validate(provider, schema=schema)
        except jsonschema.ValidationError as ex:
            msg = f"Unable to parse: {provider_yaml_path}. Original error {type(ex).__name__}: {ex}"
            raise RuntimeError(msg)
        if provider["state"] == "suspended" and not include_suspended:
            continue
        provider_yaml_dir = os.path.dirname(provider_yaml_path)
        provider["python-module"] = _filepath_to_module(provider_yaml_dir)
        provider["package-dir"] = provider_yaml_dir
        provider["system-tests-dir"] = _filepath_to_system_tests(provider_yaml_dir)
        result.append(provider)
    return result
