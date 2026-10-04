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
"""Pack the Go SDK example bundle and probe it for its task handlers."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from typing import TYPE_CHECKING
from unittest import mock

import pytest
import structlog

from airflow.dag_processing.processor import TaskHandlerDeclaration, TaskHandlerParam
from airflow.dag_processing.task_handler_processor import LangSDKTaskHandlerProcessorProcess
from airflow.sdk.coordinators._subprocess import supports_task_handler_parsing
from airflow.sdk.execution_time import supervisor
from airflow.sdk.execution_time.coordinator import get_coordinator_manager, reset_coordinator_manager

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.paths import AIRFLOW_ROOT_PATH

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.skipif(
    os.environ.get("AIRFLOW_LANG_SDK_REAL_PROBE_TESTS") != "1",
    reason="set AIRFLOW_LANG_SDK_REAL_PROBE_TESTS=1 to build and probe a real Go bundle",
)

STRING = {"type": "string"}
INT64 = {"type": "integer", "format": "int64"}
DOUBLE = {"type": "number", "format": "double"}


def _make_nullable(schema: dict) -> dict:
    return {"anyOf": [schema, {"type": "null"}]}


@pytest.fixture(scope="module")
def go_bundle(tmp_path_factory) -> Path:
    if shutil.which("go") is None:
        pytest.skip("needs a Go toolchain on PATH")
    bundle = tmp_path_factory.mktemp("go-task-handlers") / "example_dags"
    completed = subprocess.run(
        ["go", "tool", "airflow-go-pack", "--output", os.fspath(bundle), "./example/bundle"],
        cwd=AIRFLOW_ROOT_PATH / "go-sdk",
        env={**os.environ, "CGO_ENABLED": "0"},
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    return bundle


@pytest.fixture(autouse=True)
def _go_coordinator():
    spec = {"go": {"classpath": "airflow.sdk.coordinators.executable.ExecutableCoordinator"}}
    reset_coordinator_manager()
    try:
        # A bare fork, so the parse child sees this config.
        with (
            conf_vars({("sdk", "coordinators"): json.dumps(spec)}),
            mock.patch.object(supervisor, "_should_use_exec", autospec=True, return_value=False),
        ):
            yield
    finally:
        reset_coordinator_manager()


def test_probes_the_task_handlers_of_a_packed_go_bundle(go_bundle):
    coordinator = get_coordinator_manager().get_coordinator("go")
    artifact = coordinator._find_task_handler_artifact(bundle_path=go_bundle.parent, dag_id="simple_dag")
    assert supports_task_handler_parsing(artifact.schema_version)
    assert artifact.path == go_bundle.resolve()

    result = LangSDKTaskHandlerProcessorProcess.run(
        coordinator="go",
        path=artifact.path,
        bundle_path=go_bundle.parent,
        bundle_name="go-task-handlers",
        artifact_rel_path=os.fspath(artifact.path.relative_to(go_bundle.parent.resolve())),
        logger=structlog.get_logger(),
    )

    assert result.import_errors is None
    assert set(result.task_handlers) == {
        "simple_dag",
        "concurrent_xcom_dag",
        "taskflow_binding_dag",
        "variable_write_dag",
    }
    assert [d.task_id for d in result.task_handlers["simple_dag"]] == ["extract", "transform", "load"]

    declarations = {d.task_id: d for d in result.task_handlers["taskflow_binding_dag"]}
    assert declarations["via_flat_args"] == TaskHandlerDeclaration(
        task_id="via_flat_args",
        binding="positional",
        params=[
            TaskHandlerParam(name=None, value_schema=schema)
            for schema in (
                STRING,
                INT64,
                DOUBLE,
                {"type": "boolean"},
                _make_nullable({"type": "array", "items": STRING}),
                {"type": "object"},
                _make_nullable({"type": "array", "items": INT64}),
                _make_nullable(STRING),
            )
        ],
    )
    assert declarations["via_struct_arg_tag"] == TaskHandlerDeclaration(
        task_id="via_struct_arg_tag",
        binding="named",
        params=[
            TaskHandlerParam(name="region_code", exact_name=True, value_schema=STRING),
            TaskHandlerParam(name="threshold", exact_name=True, value_schema=DOUBLE),
        ],
    )
    # The Dag passes this handler an argument it does not take, which only warns under named binding.
    assert declarations["via_struct_more_args"] == TaskHandlerDeclaration(
        task_id="via_struct_more_args",
        binding="named",
        params=[TaskHandlerParam(name="region_code", exact_name=True, value_schema=STRING)],
    )
    assert declarations["via_flat_map"] == TaskHandlerDeclaration(
        task_id="via_flat_map",
        binding="named",
        params=[
            TaskHandlerParam(name="Region", value_schema=STRING),
            TaskHandlerParam(name="Count", value_schema=INT64),
        ],
    )
