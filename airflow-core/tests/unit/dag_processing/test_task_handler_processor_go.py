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
"""Pack the Go SDK example bundle, probe it for its task handlers, check the example Dags against it, and read its cache digest."""

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
from airflow.sdk.coordinators.executable.coordinator import read_cache_digest
from airflow.sdk.execution_time import supervisor
from airflow.sdk.execution_time.coordinator import reset_coordinator_manager

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.paths import AIRFLOW_ROOT_PATH
from unit.dag_processing.fake_task_handler_runtime import (
    LOCAL_BUNDLE,
    get_stub_task_ids,
    parse_dag_file,
    sort_bindings,
    task_handler_config,
)

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


GO_SDK_PATH = AIRFLOW_ROOT_PATH / "go-sdk"


def _pack(bundle: Path, *flags: str) -> Path:
    completed = subprocess.run(
        ["go", "tool", "airflow-go-pack", "--output", os.fspath(bundle), *flags, "./example/bundle"],
        cwd=GO_SDK_PATH,
        env={**os.environ, "CGO_ENABLED": "0"},
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    return bundle


@pytest.fixture(scope="module")
def go_bundle(tmp_path_factory) -> Path:
    if shutil.which("go") is None:
        pytest.skip("needs a Go toolchain on PATH")
    return _pack(tmp_path_factory.mktemp("go-task-handlers") / "example_dags")


@pytest.fixture(autouse=True)
def _go_coordinator(monkeypatch, tmp_path):
    # Without cgo the Go SDK reads the current user from USER and HOME, and fails without them.
    for name, value in (("USER", "airflow"), ("HOME", os.fspath(tmp_path))):
        if not os.environ.get(name):
            monkeypatch.setenv(name, value)
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
    result = LangSDKTaskHandlerProcessorProcess.run(
        coordinator="go",
        path=go_bundle,
        bundle_path=go_bundle.parent,
        bundle_name="go-task-handlers",
        artifact_rel_path=go_bundle.name,
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


def test_the_example_dags_match_the_packed_bundle(go_bundle, cap_structlog):
    dag_file = GO_SDK_PATH / "dags" / "go_examples.py"
    coordinators = {
        "go-sdk": {
            "classpath": "airflow.sdk.coordinators.executable.ExecutableCoordinator",
            "kwargs": {"task_handler_bundle_name": "go-task-handlers"},
        }
    }
    bundles = [
        {"name": "dags", "classpath": LOCAL_BUNDLE, "kwargs": {"path": os.fspath(dag_file.parent)}},
        {
            "name": "go-task-handlers",
            "classpath": LOCAL_BUNDLE,
            "kwargs": {"path": os.fspath(go_bundle.parent)},
        },
    ]

    with task_handler_config(
        dag_file.parent,
        go_bundle.parent,
        coordinators,
        queue_to_coordinator={"golang": "go-sdk"},
        bundles=bundles,
    ):
        first = parse_dag_file(dag_file)
        second = parse_dag_file(dag_file, known_artifacts=first.probed_artifacts)

    assert first.import_errors == {}
    assert {(b.dag_id, b.task_id) for b in first.task_handler_bindings} == get_stub_task_ids(first)
    assert [a.relative_fileloc for a in first.probed_artifacts] == [go_bundle.name]
    # The Dag passes via_struct_more_args an argument its struct does not declare, and
    # via_struct_fewer_args's struct declares one the Dag does not pass: warnings, not errors.
    assert {
        "event": "Dag's call passed argument(s) the task handler does not declare",
        "dag_id": "taskflow_binding_dag",
        "task_id": "via_struct_more_args",
        "passed_not_declared": ["unused_label"],
    } in cap_structlog
    assert {
        "event": "Task handler declares argument(s) the Dag's call did not pass",
        "dag_id": "taskflow_binding_dag",
        "task_id": "via_struct_fewer_args",
        "declared_not_passed": ["not_in_dag"],
    } in cap_structlog

    assert second.import_errors == {}
    assert second.probed_artifacts == []
    assert sort_bindings(second) == sort_bindings(first)


def test_a_repack_keeps_the_cache_digest_until_a_source_byte_changes(go_bundle, tmp_path):
    digest = read_cache_digest(go_bundle)
    assert digest is not None

    assert read_cache_digest(_pack(tmp_path / "unchanged")) == digest

    source = tmp_path / "main.go"
    source.write_bytes((GO_SDK_PATH / "example" / "bundle" / "main.go").read_bytes() + b"\n")
    assert read_cache_digest(_pack(tmp_path / "changed", "--source", os.fspath(source))) != digest
