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
from check_core_shim_targets_exist import (
    find_provider_targets,
    is_entry_present,
    is_target_present,
    main,
    read_snapshot,
)

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

            if not TYPE_CHECKING:
                class Runtime: ...
            else:
                class TypingOnly: ...
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
        pytest.param("Runtime", f"{MODULE}.Runtime", True, id="not-type-checking-body"),
        pytest.param("TypingOnly", f"{MODULE}.TypingOnly", False, id="not-type-checking-else"),
        pytest.param("annotated_only", f"{MODULE}.annotated_only", False, id="annotation-without-value"),
        pytest.param("X", "airflow.providers.missing.hooks.X", False, id="missing-provider"),
    ],
)
def test_is_target_present(providers_root, key, target, expected):
    assert is_target_present(key, target, providers_root) is expected


def test_read_snapshot_skips_comments_and_blank_lines(write_text_file):
    path = write_text_file(f"# header\n\n{MODULE}.WaitSensor\n{MODULE}.*\n")

    assert read_snapshot(path) == {f"{MODULE}.WaitSensor": 3, f"{MODULE}.*": 4}


@pytest.mark.parametrize(
    ("entry", "expected"),
    [
        pytest.param(f"{MODULE}.WaitSensor", True, id="class"),
        pytest.param(f"{MODULE}.*", True, id="wildcard"),
        pytest.param("airflow.providers.acme.sensors.gone.*", False, id="removed-wildcard-module"),
        pytest.param(f"{MODULE}.WaitSensorAsync", False, id="removed-class"),
    ],
)
def test_is_entry_present(providers_root, entry, expected):
    assert is_entry_present(entry, providers_root) is expected


@pytest.mark.parametrize(
    ("shim_targets", "snapshot_targets", "expected"),
    [
        pytest.param(["WaitSensor"], ["WaitSensor"], 0, id="recorded-and-present"),
        pytest.param(["WaitSensor"], [], 1, id="shim-target-not-recorded"),
        pytest.param(["WaitSensorAsync"], [], 1, id="unrecorded-shim-target-missing"),
        pytest.param([], ["WaitSensorAsync"], 1, id="shim-and-target-removed-together"),
    ],
)
def test_main(tmp_path, providers_root, shim_targets, snapshot_targets, expected):
    core_src = tmp_path / "core"
    core_src.mkdir()
    entries = ", ".join(f'"{name}": "{MODULE}.{name}"' for name in shim_targets)
    (core_src / "shims.py").write_text(f"add_deprecated_classes({{'wait': {{{entries}}}}}, __name__)\n")
    snapshot = tmp_path / "snapshot.txt"
    snapshot.write_text("".join(f"{MODULE}.{name}\n" for name in snapshot_targets))

    assert main(core_src, providers_root, snapshot) == expected


def test_main_passes_on_repository():
    assert main() == 0
