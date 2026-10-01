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

import copy
import json
import subprocess
from pathlib import Path
from unittest import mock

import pytest
from ci.lang_sdk_serialization.compare import TEST_DAGS, compare, get_task_defaults, main

DEFAULTS = {"retry_delay": 300.0}


def build_serialized(fileloc: str, tasks: list[dict]) -> dict:
    return {
        "d": {
            "__version": 3,
            "dag": {
                "dag_id": "d",
                "fileloc": fileloc,
                "timezone": "UTC",
                "catchup": False,
                "tags": ["a"],
                "task_group": {"prefix_group_id": True, "children": {"extract": ["operator", "extract"]}},
                "tasks": [{"__type": "operator", "__var": task} for task in tasks],
            },
        }
    }


PYTHON = build_serialized(
    "/dags/d.py",
    [
        {
            "task_id": "extract",
            "retries": 2,
            "pool_slots": 1,
            "retry_delay": 300.0,
            "task_type": "NoopOperator",
        },
        {"task_id": "load", "retry_delay": 300.0, "task_type": "NoopOperator"},
    ],
)

# Differs from PYTHON only where an SDK may: where it was declared, what stands for its
# tasks, a number JavaScript writes without a fraction, and a field left at its default.
SDK = build_serialized(
    "/bundles/app/bundle.mjs",
    [
        {"task_id": "extract", "retries": 2, "pool_slots": 1.0, "task_type": "Task", "is_stub": True},
        {"task_id": "load", "task_type": "Task", "language": "typescript", "_arg_bindings": []},
    ],
)


def get_task(serialized: dict, index: int = 0) -> dict:
    return serialized["d"]["dag"]["tasks"][index]["__var"]


def test_accepts_the_differences_an_sdk_is_allowed():
    assert compare(PYTHON, SDK, DEFAULTS) == []


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        pytest.param(
            lambda sdk: get_task(sdk).update(retries=3),
            ["d: task extract: retries is 3, Python writes 2"],
            id="task-value",
        ),
        pytest.param(
            lambda sdk: get_task(sdk).update(pool_slots=True),
            ["d: task extract: pool_slots is True, Python writes 1"],
            id="bool-is-not-a-number",
        ),
        pytest.param(
            lambda sdk: get_task(sdk).pop("retries"),
            ["d: task extract: retries is missing, Python writes 2"],
            id="task-key-missing",
        ),
        pytest.param(
            lambda sdk: get_task(sdk).update(owner="me"),
            ["d: task extract: owner is 'me', which Python does not write"],
            id="task-key-extra",
        ),
        pytest.param(
            lambda sdk: sdk["d"]["dag"].pop("timezone"),
            ["d: timezone is missing, Python writes 'UTC'"],
            id="dag-key-missing",
        ),
        pytest.param(
            lambda sdk: sdk["d"]["dag"].update(tags=["b"]),
            ["d: tags[0] is 'b', Python writes 'a'"],
            id="nested-list",
        ),
        pytest.param(
            lambda sdk: sdk["d"]["dag"]["task_group"].update(prefix_group_id=False),
            ["d: task_group.prefix_group_id is False, Python writes True"],
            id="nested-dict",
        ),
        pytest.param(
            lambda sdk: sdk["d"]["dag"]["tasks"].reverse(),
            ["d: tasks are ['load', 'extract'], Python writes ['extract', 'load']"],
            id="task-order",
        ),
        pytest.param(
            lambda sdk: sdk["d"]["dag"]["tasks"][0].update(__type="taskgroup"),
            ["d: task extract is a 'taskgroup', not a 'operator'"],
            id="task-encoding",
        ),
        pytest.param(
            lambda sdk: sdk["d"].update(__version=4),
            ["d: __version is 4, Python writes 3"],
            id="version",
        ),
        pytest.param(
            lambda sdk: sdk.update(e=sdk["d"]),
            ["the Dags are ['d', 'e'], Python writes ['d']"],
            id="dag-ids",
        ),
    ],
)
def test_reports_each_difference(change, expected):
    sdk = copy.deepcopy(SDK)
    change(sdk)

    assert compare(PYTHON, sdk, DEFAULTS) == expected


def test_accepts_a_left_out_task_key_only_at_its_schema_default():
    python = copy.deepcopy(PYTHON)
    get_task(python)["retry_delay"] = 600.0

    assert compare(python, SDK, DEFAULTS) == ["d: task extract: retry_delay is missing, Python writes 600.0"]


def test_reads_the_task_defaults_from_the_dag_schema():
    defaults = get_task_defaults()

    assert defaults["retry_delay"] == 300.0
    # A null default and no default both mean the field has none.
    assert "render_template_as_native_obj" not in defaults
    assert "task_id" not in defaults


def write_outputs(received: dict):
    """
    Stand in for both serializers, each writing its output to the paths compare.py hands it.

    The SDK leaves catchup out and the received output has it, as when Airflow fills it in from its
    config, so only the received output agrees with Python.
    """
    sdk = copy.deepcopy(received)
    del sdk["d"]["dag"]["catchup"]

    def run(command, **kwargs):
        if command[0] == "uv":
            python_output, _, _, received_output = command[-4:]
            Path(python_output).write_text(json.dumps(PYTHON))
            Path(received_output).write_text(json.dumps(received))
        else:
            Path(command[-1]).write_text(json.dumps(sdk))
        return subprocess.CompletedProcess(command, 0)

    return run


@mock.patch("ci.lang_sdk_serialization.compare.get_task_defaults", autospec=True, return_value=DEFAULTS)
@mock.patch("ci.lang_sdk_serialization.compare.tempfile.mkdtemp", autospec=True)
@mock.patch("ci.lang_sdk_serialization.compare.subprocess.run", autospec=True)
def test_main_runs_both_serializers_and_removes_their_output_when_they_agree(
    mock_run, mock_mkdtemp, mock_get_task_defaults, tmp_path, capsys
):
    mock_mkdtemp.return_value = str(tmp_path)
    mock_run.side_effect = write_outputs(SDK)

    assert main(["--sdk", "typescript", "--", "pnpm", "exec", "tsx", "serialize.ts"]) == 0

    sdk_output = str(tmp_path / "serialized_typescript.json")
    assert [call.args[0] for call in mock_run.call_args_list] == [
        ["pnpm", "exec", "tsx", "serialize.ts", str(TEST_DAGS), sdk_output],
        [
            "uv",
            "run",
            "--project",
            "airflow-core",
            "--no-dev",
            "python",
            str(TEST_DAGS.parent / "serialize_python.py"),
            str(TEST_DAGS),
            str(tmp_path / "serialized_python.json"),
            "--receive",
            sdk_output,
            str(tmp_path / "received_typescript.json"),
        ],
    ]
    assert not tmp_path.exists()
    assert "serializes all 1 Dags" in capsys.readouterr().out


@mock.patch("ci.lang_sdk_serialization.compare.get_task_defaults", autospec=True, return_value=DEFAULTS)
@mock.patch("ci.lang_sdk_serialization.compare.tempfile.mkdtemp", autospec=True)
@mock.patch("ci.lang_sdk_serialization.compare.subprocess.run", autospec=True)
def test_main_reports_the_differences_and_keeps_the_output(
    mock_run, mock_mkdtemp, mock_get_task_defaults, tmp_path, capsys
):
    mock_mkdtemp.return_value = str(tmp_path)
    sdk = copy.deepcopy(SDK)
    get_task(sdk)["retries"] = 3
    mock_run.side_effect = write_outputs(sdk)

    assert main(["--sdk", "typescript", "--", "serialize"]) == 1

    assert (tmp_path / "serialized_typescript.json").exists()
    assert "d: task extract: retries is 3, Python writes 2" in capsys.readouterr().err


@mock.patch("ci.lang_sdk_serialization.compare.tempfile.mkdtemp", autospec=True)
@mock.patch("ci.lang_sdk_serialization.compare.subprocess.run", autospec=True)
def test_main_stops_when_a_serializer_fails(mock_run, mock_mkdtemp, tmp_path):
    mock_mkdtemp.return_value = str(tmp_path)
    mock_run.return_value = subprocess.CompletedProcess([], 1)

    with pytest.raises(SystemExit, match="`serialize .*` failed"):
        main(["--sdk", "typescript", "--", "serialize"])

    assert mock_run.call_count == 1
