#
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
"""Pack the TypeScript SDK example bundle and probe it for its task handlers."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from typing import TYPE_CHECKING
from unittest import mock

import pytest
import structlog

from airflow.dag_processing.processor import TaskHandlerDeclaration, TaskHandlerParsingResult
from airflow.dag_processing.task_handler_processor import LangSDKTaskHandlerProcessorProcess
from airflow.sdk.coordinators._subprocess import supports_task_handler_parsing
from airflow.sdk.execution_time import supervisor
from airflow.sdk.execution_time.coordinator import get_coordinator_manager, reset_coordinator_manager

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.paths import AIRFLOW_ROOT_PATH
from unit.dag_processing.fake_task_handler_runtime import (
    LOCAL_BUNDLE,
    parse_dag_file,
    require_toolchain,
    task_handler_config,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

TS_SDK_PATH = AIRFLOW_ROOT_PATH / "ts-sdk"
# The SDK's `engines` requirement.
MIN_NODE_MAJOR = 22
COORDINATORS = {
    "ts": {
        "classpath": "airflow.sdk.coordinators.node.NodeCoordinator",
        "kwargs": {"node_executable": shutil.which("node")},
    }
}

pytestmark = pytest.mark.skipif(
    os.environ.get("AIRFLOW_LANG_SDK_REAL_PROBE_TESTS") != "1",
    reason="set AIRFLOW_LANG_SDK_REAL_PROBE_TESTS=1 to pack and probe a real TypeScript bundle",
)


def _get_toolchain_problem() -> str | None:
    """Return why this machine cannot pack the example bundle, or ``None`` when it can."""
    if not TS_SDK_PATH.is_dir():
        return "the TypeScript SDK sources are absent"
    node = shutil.which("node")
    if node is None or shutil.which("pnpm") is None:
        return "needs node and pnpm"
    try:
        version = subprocess.run(
            [node, "--version"], capture_output=True, text=True, check=True
        ).stdout.strip()
        major = int(version.lstrip("v").split(".")[0])
    except (OSError, subprocess.CalledProcessError, ValueError) as e:
        return f"cannot read the Node.js version: {e}"
    if major < MIN_NODE_MAJOR:
        return f"needs Node.js {MIN_NODE_MAJOR} or later, found {version}"
    return None


@pytest.fixture(scope="module")
def example_bundle(tmp_path_factory) -> Path:
    """Pack ``ts-sdk/example`` from a copy of the SDK sources, so the checkout gets no build output."""
    require_toolchain(_get_toolchain_problem())
    sdk = tmp_path_factory.mktemp("ts-sdk") / "ts-sdk"
    shutil.copytree(TS_SDK_PATH, sdk, ignore=shutil.ignore_patterns("node_modules", "dist", ".pnpm-store"))
    env = {**os.environ, "CI": "true", "COREPACK_ENABLE_DOWNLOAD_PROMPT": "0"}
    # The example links the SDK, so it is installed again once the SDK is built.
    for cwd, args in [
        (sdk, ["install", "--frozen-lockfile"]),
        (sdk, ["run", "build"]),
        (sdk / "example", ["install"]),
        (sdk / "example", ["run", "build"]),
    ]:
        subprocess.run(["pnpm", *args], cwd=cwd, env=env, check=True)
    return sdk / "example" / "dist" / "bundle.min.mjs"


@pytest.fixture
def fresh_coordinator_manager() -> Iterator[None]:
    reset_coordinator_manager()
    yield
    reset_coordinator_manager()


def _declare(task_id: str) -> TaskHandlerDeclaration:
    return TaskHandlerDeclaration(task_id=task_id, binding="named", params=None)


@pytest.mark.usefixtures("fresh_coordinator_manager")
@conf_vars({("sdk", "coordinators"): json.dumps(COORDINATORS)})
@mock.patch.object(supervisor, "_should_use_exec", autospec=True, return_value=False)
def test_probes_the_task_handlers_of_a_packed_typescript_bundle(mock_should_use_exec, example_bundle):
    coordinator = get_coordinator_manager().get_coordinator("ts")
    artifact = coordinator._find_task_handler_artifact(
        bundle_path=example_bundle.parent, dag_id="typescript_example"
    )
    assert supports_task_handler_parsing(artifact.schema_version)
    assert artifact.path == example_bundle.resolve()

    result = LangSDKTaskHandlerProcessorProcess.run(
        coordinator="ts",
        path=artifact.path,
        bundle_path=example_bundle.parent,
        bundle_name="ts-task-handlers",
        artifact_rel_path=os.fspath(artifact.path.relative_to(example_bundle.parent.resolve())),
        logger=structlog.get_logger(),
    )

    # Types are erased, so each handler lists no params and only its presence is checked.
    assert result == TaskHandlerParsingResult(
        fileloc=os.fspath(artifact.path),
        task_handlers={
            "typescript_example": [
                _declare("build_message"),
                _declare("read_connection"),
                _declare("write_and_delete_variable"),
            ],
            "typescript_taskflow_example": [
                _declare("summarize"),
                _declare("report"),
                _declare("build_message"),
            ],
        },
    )
    assert list(result.task_handlers) == ["typescript_example", "typescript_taskflow_example"]


@pytest.mark.usefixtures("fresh_coordinator_manager")
@pytest.mark.parametrize("dag_file_name", ["typescript_example.py", "typescript_taskflow_example.py"])
@mock.patch.object(supervisor, "_should_use_exec", autospec=True, return_value=False)
def test_the_example_dags_match_the_packed_bundle(
    mock_should_use_exec, example_bundle, dag_file_name, cap_structlog
):
    dag_file = TS_SDK_PATH / "example" / "dags" / dag_file_name
    coordinators = {
        "ts": {
            "classpath": "airflow.sdk.coordinators.node.NodeCoordinator",
            "kwargs": {
                "task_handler_bundle_name": "ts-task-handlers",
                "node_executable": shutil.which("node"),
            },
        }
    }
    bundles = [
        {"name": "dags", "classpath": LOCAL_BUNDLE, "kwargs": {"path": os.fspath(dag_file.parent)}},
        {
            "name": "ts-task-handlers",
            "classpath": LOCAL_BUNDLE,
            "kwargs": {"path": os.fspath(example_bundle.parent)},
        },
    ]

    with task_handler_config(
        dag_file.parent,
        example_bundle.parent,
        coordinators,
        queue_to_coordinator={"typescript": "ts"},
        bundles=bundles,
    ):
        result = parse_dag_file(dag_file)

    assert result.import_errors == {}
    assert {"event": "Probed a task handler artifact", "path": example_bundle.name} in cap_structlog
    assert not any(
        e["event"]
        in (
            "Dag's call passed argument(s) the task handler does not declare",
            "Task handler declares argument(s) the Dag's call did not pass",
            "Not checking a Dag's stub tasks against their task handlers",
        )
        for e in cap_structlog.entries
    )
