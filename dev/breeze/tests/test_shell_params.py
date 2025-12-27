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

from unittest.mock import patch

import pytest
from rich.console import Console

from airflow_breeze.branch_defaults import AIRFLOW_BRANCH
from airflow_breeze.params.shell_params import ShellParams

console = Console(width=400, color_system="standard")


@pytest.mark.parametrize(
    "env_vars, kwargs, expected_vars",
    [
        pytest.param(
            {},
            {"python": 3.12},
            {
                "DEFAULT_BRANCH": AIRFLOW_BRANCH,
                "AIRFLOW_CI_IMAGE": f"ghcr.io/apache/airflow/{AIRFLOW_BRANCH}/ci/python3.12",
                "PYTHON_MAJOR_MINOR_VERSION": "3.12",
            },
            id="python3.12",
        ),
        pytest.param(
            {},
            {"python": 3.9},
            {
                "AIRFLOW_CI_IMAGE": f"ghcr.io/apache/airflow/{AIRFLOW_BRANCH}/ci/python3.9",
                "PYTHON_MAJOR_MINOR_VERSION": "3.9",
            },
            id="python3.9",
        ),
        pytest.param(
            {},
            {"airflow_branch": "v2-7-test"},
            {
                "DEFAULT_BRANCH": "v2-7-test",
                "AIRFLOW_CI_IMAGE": "ghcr.io/apache/airflow/v2-7-test/ci/python3.10",
                "PYTHON_MAJOR_MINOR_VERSION": "3.10",
            },
            id="With release branch",
        ),
        pytest.param(
            {"DEFAULT_BRANCH": "v2-4-test"},
            {},
            {
                "DEFAULT_BRANCH": AIRFLOW_BRANCH,  # DEFAULT_BRANCH is overridden from sources
                "AIRFLOW_CI_IMAGE": f"ghcr.io/apache/airflow/{AIRFLOW_BRANCH}/ci/python3.10",
                "PYTHON_MAJOR_MINOR_VERSION": "3.10",
            },
            id="Branch variable from sources not from original env",
        ),
        pytest.param(
            {},
            {},
            {
                "FLOWER_HOST_PORT": "25555",
            },
            id="Default flower port",
        ),
        pytest.param(
            {"FLOWER_HOST_PORT": "1234"},
            {},
            {
                "FLOWER_HOST_PORT": "1234",
            },
            id="Overridden flower host",
        ),
        pytest.param(
            {},
            {"celery_broker": "redis"},
            {
                "AIRFLOW__CELERY__BROKER_URL": "redis://redis:6379/0",
            },
            id="Celery executor with redis broker",
        ),
        pytest.param(
            {},
            {"celery_broker": "unknown"},
            {
                "AIRFLOW__CELERY__BROKER_URL": "",
            },
            id="No URL for celery if bad broker specified",
        ),
        pytest.param(
            {},
            {},
            {
                "CI_EVENT_TYPE": "pull_request",
            },
            id="Default CI event type",
        ),
        pytest.param(
            {"CI_EVENT_TYPE": "push"},
            {},
            {
                "CI_EVENT_TYPE": "push",
            },
            id="Override CI event type by variable",
        ),
        pytest.param(
            {},
            {},
            {
                "INIT_SCRIPT_FILE": "init.sh",
            },
            id="Default init script file",
        ),
        pytest.param(
            {"INIT_SCRIPT_FILE": "my_init.sh"},
            {},
            {
                "INIT_SCRIPT_FILE": "my_init.sh",
            },
            id="Override init script file by variable",
        ),
        pytest.param(
            {},
            {},
            {
                "CI": "false",
            },
            id="CI false by default",
        ),
        pytest.param(
            {"CI": "true"},
            {},
            {
                "CI": "true",
            },
            id="Unless it's overridden by environment variable",
        ),
        pytest.param(
            {},
            {},
            {
                "PYTHONWARNINGS": None,
            },
            id="PYTHONWARNINGS should not be set by default",
        ),
        pytest.param(
            {"PYTHONWARNINGS": "default"},
            {},
            {
                "PYTHONWARNINGS": "default",
            },
            id="PYTHONWARNINGS should be set when specified in environment",
        ),
    ],
)
def test_shell_params_to_env_var_conversion(
    env_vars: dict[str, str], kwargs: dict[str, str | bool], expected_vars: dict[str, str]
):
    with patch("os.environ", env_vars):
        shell_params = ShellParams(**kwargs)
        env_vars = shell_params.env_variables_for_docker_commands
        error = False
        for expected_key, expected_value in expected_vars.items():
            if expected_key not in env_vars:
                if expected_value is not None:
                    console.print(f"[red] Expected variable {expected_key} missing.[/]\nVariables retrieved:")
                    console.print(env_vars)
                    error = True
            elif expected_key is None:
                console.print(f"[red] The variable {expected_key} is not expected.[/]\nVariables retrieved:")
                console.print(env_vars)
                error = True
            elif env_vars[expected_key] != expected_value:
                console.print(
                    f"[red] The expected variable {expected_key} value '{env_vars[expected_key]}' is different than expected {expected_value}[/]\n"
                    f"Variables retrieved:"
                )
                console.print(env_vars)
                error = True
        assert not error, "Some values are not as expected."
