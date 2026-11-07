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

import textwrap

import pytest
from check_core_shim_targets_exist import find_provider_targets, is_target_present, main

MODULE = "airflow.providers.acme.sensors.wait"


@pytest.fixture
def providers_root(tmp_path):
    package = tmp_path / "acme" / "src" / "airflow" / "providers" / "acme" / "sensors"
    package.mkdir(parents=True)
    (package / "wait.py").write_text(
        textwrap.dedent(
            """
            from typing import TYPE_CHECKING

            from airflow.sdk import BaseSensorOperator as Base

            if TYPE_CHECKING:
                from typing_only import TypeOnly

            annotated_only: int

            try:
                from fast import Engine
            except ImportError:
                Engine = None

            class WaitSensor(Base): ...

            def helper(): ...
            """
        )
    )
    return tmp_path


def test_find_provider_targets_skips_non_provider_targets(write_python_file):
    path = write_python_file(
        """
        __deprecated_classes = {
            "wait": {
                "WaitSensor": "airflow.providers.acme.sensors.wait.WaitSensor",
                "BaseSensorOperator": "airflow.sdk.bases.sensor.BaseSensorOperator",
            },
            "acme": {"*": "airflow.providers.acme.sensors"},
        }
        """
    )

    assert [(key, target) for _, key, target in find_provider_targets(path)] == [
        ("WaitSensor", "airflow.providers.acme.sensors.wait.WaitSensor"),
        ("*", "airflow.providers.acme.sensors"),
    ]


@pytest.mark.parametrize(
    ("key", "target", "expected"),
    [
        pytest.param("WaitSensor", f"{MODULE}.WaitSensor", True, id="class"),
        pytest.param("helper", f"{MODULE}.helper", True, id="function"),
        pytest.param("Base", f"{MODULE}.Base", True, id="import-alias"),
        pytest.param("Engine", f"{MODULE}.Engine", True, id="name-inside-try"),
        pytest.param("wait", MODULE, True, id="module"),
        pytest.param("*", MODULE, True, id="wildcard"),
        pytest.param("WaitSensorAsync", f"{MODULE}.WaitSensorAsync", False, id="removed-class"),
        pytest.param("*", "airflow.providers.acme.sensors.gone", False, id="removed-module"),
        pytest.param("TypeOnly", f"{MODULE}.TypeOnly", False, id="type-checking-only"),
        pytest.param("annotated_only", f"{MODULE}.annotated_only", False, id="annotation-without-value"),
        pytest.param("X", "airflow.providers.missing.hooks.X", False, id="missing-provider"),
    ],
)
def test_is_target_present(providers_root, key, target, expected):
    assert is_target_present(key, target, providers_root) is expected


def test_main_passes_on_repository():
    assert main() == 0
