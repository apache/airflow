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
from pathlib import Path

import httpx
from datamodel_code_generator import (
    DataModelType,
    DatetimeClassType,
    InputFileType,
    LiteralType,
    PythonVersion,
    generate as generate_models,
)
from openapi_spec_validator import validate_spec

os.environ["_AIRFLOW__AS_LIBRARY"] = "1"

AIRFLOW_ROOT_PATH = Path(__file__).parents[2].resolve()
AIRFLOW_TASK_SDK_ROOT_PATH = AIRFLOW_ROOT_PATH / "task-sdk"
AIRFLOW_CORE_SOURCES_PATH = AIRFLOW_ROOT_PATH / "airflow-core" / "src"
sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_prek_utils is imported

sys.path.insert(0, AIRFLOW_CORE_SOURCES_PATH.as_posix())
sys.path.insert(0, str(AIRFLOW_ROOT_PATH))  # make sure setup is imported from Airflow


task_sdk_root = Path(__file__).parents[1]


def load_config():
    try:
        from tomllib import load as load_tomllib
    except ImportError:
        from tomli import load as load_tomllib

    pyproject = AIRFLOW_TASK_SDK_ROOT_PATH / "pyproject.toml"
    # Simulate what `datamodel-code-generator` does on the CLI
    with pyproject.open("rb") as fh:
        cfg = load_tomllib(fh)["tool"]["datamodel-codegen"]

    cfg = {k.replace("-", "_"): v for k, v in cfg.items()}

    # Translate config file option names to generate() kwarg names
    if (use_default := cfg.pop("use_default", None)) is not None:
        cfg["apply_default_values_for_required_fields"] = use_default

    if cfg.get("use_annotated"):
        cfg["field_constraints"] = True

    cfg["output"] = Path(cfg["output"])
    cfg["output_model_type"] = DataModelType(cfg["output_model_type"])
    cfg["output_datetime_class"] = DatetimeClassType(cfg["output_datetime_class"])
    cfg["input_file_type"] = InputFileType(cfg["input_file_type"])
    cfg["target_python_version"] = PythonVersion(cfg["target_python_version"])
    cfg["enum_field_as_literal"] = LiteralType(cfg["enum_field_as_literal"])
    return cfg


def generate_file():
    from airflow.api_fastapi.execution_api.app import InProcessExecutionAPI

    app = InProcessExecutionAPI()

    latest_version = app.app.versions.version_values[0]
    client = httpx.Client(transport=app.transport)
    openapi_schema = (
        client.get(f"http://localhost/openapi.json?version={latest_version}").raise_for_status().text
    )

    validate_spec(json.loads(openapi_schema))

    os.chdir(AIRFLOW_TASK_SDK_ROOT_PATH)

    args = load_config()
    args["input_filename"] = args.pop("url")
    args["custom_formatters_kwargs"] = {"api_version": latest_version}
    generate_models(openapi_schema, **args)


generate_file()
