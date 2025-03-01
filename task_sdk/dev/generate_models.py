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
from typing import TYPE_CHECKING

from datamodel_code_generator import (
    DataModelType,
    DatetimeClassType,
    InputFileType,
    LiteralType,
    PythonVersion,
    generate as generate_models,
)

os.environ["_AIRFLOW__AS_LIBRARY"] = "1"
sys.path.insert(
    0,
    str(Path(__file__).parents[2].joinpath("scripts", "ci", "pre_commit").resolve()),
)  # make sure common utils are importable

from common_precommit_utils import (
    AIRFLOW_SOURCES_ROOT_PATH,
)

sys.path.insert(0, str(AIRFLOW_SOURCES_ROOT_PATH))  # make sure setup is imported from Airflow

from airflow.api_fastapi.execution_api.app import create_task_execution_api_app

task_sdk_root = Path(__file__).parents[1]

if TYPE_CHECKING:
    from fastapi import FastAPI


def load_config():
    try:
        from tomllib import load as load_tomllib
    except ImportError:
        from tomli import load as load_tomllib

    pyproject = task_sdk_root / "pyproject.toml"
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


def generate_file(app: FastAPI):
    # The persisted openapi spec will list all endpoints (public and ui), this
    # is used for code generation.
    for route in app.routes:
        if getattr(route, "name") == "webapp":
            continue
        route.__setattr__("include_in_schema", True)

    os.chdir(task_sdk_root)

    openapi_schema = json.dumps(app.openapi())
    args = load_config()
    args["input_filename"] = args.pop("url")
    generate_models(openapi_schema, **args)


generate_file(create_task_execution_api_app())
