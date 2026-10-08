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
import yaml
from ci.lang_sdk_serialization.compare import (
    FEATURES,
    TEST_DAGS,
    compare,
    filter_cases,
    get_task_defaults,
    main,
    parse_features,
)

DEFAULTS = {"retry_delay": 300.0}
REAL_TEST_DAGS = TEST_DAGS


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


CASES = """\
# The header.
dags:
  # A Dag with nothing special.
  - dag_id: d
    tasks:
      - task_id: extract

  # A Dag that needs a feature.
  - dag_id: branchy
    requires: [branch]
    tasks:
      - task_id: gate

  # A Dag that needs two features.
  - dag_id: labelled_switch
    requires: [switch, edge_labels]
    tasks:
      - task_id: pick
"""


@pytest.fixture
def cases(tmp_path):
    path = tmp_path / "cases" / "test_dags.yaml"
    path.parent.mkdir()
    path.write_text(CASES)
    with mock.patch("ci.lang_sdk_serialization.compare.TEST_DAGS", path):
        yield path


@mock.patch("ci.lang_sdk_serialization.compare.get_task_defaults", autospec=True, return_value=DEFAULTS)
@mock.patch("ci.lang_sdk_serialization.compare.tempfile.mkdtemp", autospec=True)
@mock.patch("ci.lang_sdk_serialization.compare.subprocess.run", autospec=True)
def test_main_runs_both_serializers_and_removes_their_output_when_they_agree(
    mock_run, mock_mkdtemp, mock_get_task_defaults, cases, tmp_path, capsys
):
    work_dir = tmp_path / "work"
    work_dir.mkdir()
    mock_mkdtemp.return_value = str(work_dir)
    mock_run.side_effect = write_outputs(SDK)

    assert main(["--sdk", "typescript", "--", "pnpm", "exec", "tsx", "serialize.ts"]) == 0

    sdk_output = str(work_dir / "serialized_typescript.json")
    filtered = str(work_dir / "test_dags.yaml")
    assert [call.args[0] for call in mock_run.call_args_list] == [
        ["pnpm", "exec", "tsx", "serialize.ts", filtered, sdk_output],
        [
            "uv",
            "run",
            "--project",
            "airflow-core",
            "--no-dev",
            "python",
            str(REAL_TEST_DAGS.parent / "serialize_python.py"),
            filtered,
            str(work_dir / "serialized_python.json"),
            "--receive",
            sdk_output,
            str(work_dir / "received_typescript.json"),
        ],
    ]
    assert not work_dir.exists()
    assert "serializes the 1 Dags of test_dags.yaml it supports" in capsys.readouterr().out


@mock.patch("ci.lang_sdk_serialization.compare.get_task_defaults", autospec=True, return_value=DEFAULTS)
@mock.patch("ci.lang_sdk_serialization.compare.tempfile.mkdtemp", autospec=True)
@mock.patch("ci.lang_sdk_serialization.compare.subprocess.run", autospec=True)
def test_main_reports_the_differences_and_keeps_the_output(
    mock_run, mock_mkdtemp, mock_get_task_defaults, cases, tmp_path, capsys
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
def test_main_stops_when_a_serializer_fails(mock_run, mock_mkdtemp, cases, tmp_path):
    mock_mkdtemp.return_value = str(tmp_path)
    mock_run.return_value = subprocess.CompletedProcess([], 1)

    with pytest.raises(SystemExit, match="`serialize .*` failed"):
        main(["--sdk", "typescript", "--", "serialize"])

    assert mock_run.call_count == 1


@mock.patch("ci.lang_sdk_serialization.compare.tempfile.mkdtemp", autospec=True)
@mock.patch("ci.lang_sdk_serialization.compare.subprocess.run", autospec=True)
def test_main_hands_the_sdk_only_the_dags_it_supports(mock_run, mock_mkdtemp, cases, tmp_path):
    mock_mkdtemp.return_value = str(tmp_path)
    mock_run.side_effect = write_outputs(SDK)

    main(["--sdk", "go", "--supports", "branch", "--", "serialize"])

    filtered = yaml.safe_load((tmp_path / "test_dags.yaml").read_text())
    assert [case["dag_id"] for case in filtered["dags"]] == ["d", "branchy"]


@mock.patch("ci.lang_sdk_serialization.compare.tempfile.mkdtemp", autospec=True)
@mock.patch("ci.lang_sdk_serialization.compare.subprocess.run", autospec=True)
def test_main_fails_when_the_sdk_writes_other_dags_than_it_supports(
    mock_run, mock_mkdtemp, cases, tmp_path, capsys
):
    mock_mkdtemp.return_value = str(tmp_path)
    mock_run.side_effect = write_outputs(SDK)

    assert main(["--sdk", "go", "--supports", "all", "--", "serialize"]) == 1

    assert mock_run.call_count == 1
    assert (
        "The go SDK wrote the Dags ['d'], but it supports "
        "['branch', 'edge_labels', 'group_options', 'literal_inputs', 'switch', 'trigger_dag_run'] "
        "and so should write ['branchy', 'd', 'labelled_switch']"
    ) in capsys.readouterr().err


def test_parse_features():
    assert parse_features("all") == FEATURES
    assert parse_features("") == frozenset()
    assert parse_features("branch,switch") == {"branch", "switch"}
    with pytest.raises(SystemExit, match=r"--supports names \['nope'\]"):
        parse_features("branch,nope")


@pytest.mark.parametrize(
    ("supported", "expected"),
    [
        pytest.param(frozenset(), ["d"], id="nothing"),
        pytest.param(frozenset({"branch"}), ["d", "branchy"], id="one-feature"),
        pytest.param(frozenset({"switch"}), ["d"], id="all-of-the-features"),
        pytest.param(frozenset({"switch", "edge_labels"}), ["d", "labelled_switch"], id="both-features"),
        pytest.param(FEATURES, ["d", "branchy", "labelled_switch"], id="all"),
    ],
)
def test_filter_cases_keeps_the_dags_whose_features_are_all_supported(supported, expected):
    filtered, dag_ids = filter_cases(CASES, supported)

    assert dag_ids == expected
    assert [case["dag_id"] for case in yaml.safe_load(filtered)["dags"]] == expected


def test_filter_cases_keeps_the_header_and_the_comment_of_each_dag():
    filtered, _ = filter_cases(CASES, frozenset({"branch"}))

    assert filtered.startswith("# The header.\ndags:\n  # A Dag with nothing special.\n  - dag_id: d\n")
    assert "  # A Dag that needs a feature.\n  - dag_id: branchy\n" in filtered
    assert "A Dag that needs two features" not in filtered


def test_filter_cases_rejects_a_feature_it_does_not_know():
    with pytest.raises(SystemExit, match=r"Dag d requires \['nope'\]"):
        filter_cases("dags:\n  - dag_id: d\n    requires: [nope]\n", FEATURES)


class DagsLoader(yaml.SafeLoader):
    """Reads test_dags.yaml, with the values of its tags as they are written."""


def read_tagged_scalar(loader: yaml.SafeLoader, node: yaml.ScalarNode) -> str:
    return loader.construct_scalar(node)


DagsLoader.add_constructor("!datetime", read_tagged_scalar)
DagsLoader.add_constructor("!timedelta", read_tagged_scalar)


def load_dag_ids(text: str) -> list[str]:
    return [case["dag_id"] for case in yaml.load(text, Loader=DagsLoader)["dags"]]


def load_test_dags() -> list[dict]:
    return yaml.load(TEST_DAGS.read_text(), Loader=DagsLoader)["dags"]


def test_test_dags_filters_to_the_dags_that_need_no_feature():
    cases = load_test_dags()
    filtered, dag_ids = filter_cases(TEST_DAGS.read_text(), frozenset())

    assert dag_ids == [case["dag_id"] for case in cases if "requires" not in case]
    assert load_dag_ids(filtered) == dag_ids
    assert filter_cases(TEST_DAGS.read_text(), FEATURES)[1] == [case["dag_id"] for case in cases]


# The keys that a language SDK's builder has to understand, with the features that allow them.
KEY_FEATURES = {
    "branch": {"branch", "switch"},
    "trigger_dag_run": {"trigger_dag_run"},
    "literals": {"literal_inputs"},
}


def test_test_dags_require_the_feature_of_each_key_they_use():
    for case in load_test_dags():
        required = set(case.get("requires", []))
        assert required <= FEATURES, case["dag_id"]
        for task in case["tasks"]:
            for key, allowed in KEY_FEATURES.items():
                if key in task:
                    assert required & allowed, f"{case['dag_id']}.{task['task_id']} uses {key}"
        if any(not isinstance(group, str) for group in case.get("groups", [])):
            assert "group_options" in required, case["dag_id"]
        if any(len(edge) == 3 for edge in case.get("order_edges", [])):
            assert "edge_labels" in required, case["dag_id"]
