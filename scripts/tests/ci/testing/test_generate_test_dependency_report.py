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

import importlib.util
import json
import sys
from pathlib import Path

import pytest

IN_CONTAINER_PATH = Path(__file__).resolve().parents[4] / "scripts" / "in_container"
DEP_STATUS_MODULE_PATH = IN_CONTAINER_PATH / "get_dependency_status.py"
REPORT_MODULE_PATH = IN_CONTAINER_PATH / "generate_test_dependency_report.py"


def _load_module(module_name: str, module_path: Path):
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def dependency_status_module():
    return _load_module("test_get_dependency_status_module", DEP_STATUS_MODULE_PATH)


@pytest.fixture
def dependency_report_module(monkeypatch):
    monkeypatch.syspath_prepend(str(IN_CONTAINER_PATH))
    return _load_module("test_generate_test_dependency_report_module", REPORT_MODULE_PATH)


def test_build_dependency_depth_map_parses_uv_tree_output(dependency_status_module):
    depth_map = dependency_status_module.build_dependency_depth_map(
        [
            "apache-airflow 3.2.0",
            "├── requests 2.32.5",
            "│   └── certifi 2025.1.31",
            "└── rich 14.1.0",
        ]
    )

    assert depth_map == {
        "apache-airflow": 0,
        "requests": 1,
        "certifi": 2,
        "rich": 1,
    }


def test_build_report_suffix_sanitizes_job_id(dependency_report_module):
    suffix = dependency_report_module.build_report_suffix("providers/google:3.12 All")

    assert suffix == "providers-google-3.12-All"


def test_run_command_returns_error_result_for_os_error(dependency_report_module):
    def failing_runner(*args, **kwargs):
        raise OSError("uv missing")

    result = dependency_report_module.run_command(("uv", "tree"), runner=failing_runner)

    assert result.returncode == 1
    assert result.stderr == "uv missing"
    assert result.stdout == ""


def test_generate_report_writes_expected_files(tmp_path, dependency_report_module, monkeypatch):
    env = {
        "JOB_ID": "providers-amazon-3.12",
        "TEST_GROUP": "providers",
        "TEST_TYPE": "Providers[amazon]",
        "PYTHON_MAJOR_MINOR_VERSION": "3.12",
        "BACKEND": "sqlite",
        "BACKEND_VERSION": "",
        "USE_AIRFLOW_VERSION": "current-sources",
    }

    freeze_result = dependency_report_module.CommandResult(
        command=("uv", "pip", "freeze"),
        returncode=0,
        stdout="apache-airflow==3.2.0\napache-airflow-providers-amazon==1.0.0\nrequests==2.32.5\n",
        stderr="",
    )
    tree_result = dependency_report_module.CommandResult(
        command=("uv", "tree", "--no-dedupe"),
        returncode=0,
        stdout="\n".join(
            [
                "apache-airflow 3.2.0",
                "├── apache-airflow-providers-amazon 1.0.0",
                "└── requests 2.32.5",
            ]
        ),
        stderr="",
    )

    def fake_run_command(command):
        if command == ("uv", "pip", "freeze"):
            return freeze_result
        if command == ("uv", "tree", "--no-dedupe"):
            return tree_result
        raise AssertionError(f"Unexpected command: {command}")

    monkeypatch.setattr(dependency_report_module, "run_command", fake_run_command)

    report_paths = dependency_report_module.generate_report(output_dir=tmp_path, env=env)

    assert report_paths["freeze"].read_text(encoding="utf-8") == freeze_result.stdout
    assert report_paths["tree"].read_text(encoding="utf-8") == tree_result.stdout
    assert json.loads(report_paths["depth"].read_text(encoding="utf-8")) == {
        "apache-airflow": 0,
        "apache-airflow-providers-amazon": 1,
        "requests": 1,
    }
    summary = report_paths["summary"].read_text(encoding="utf-8")
    assert "## Test Dependency Report" in summary
    assert "- Job ID: providers-amazon-3.12" in summary
    assert "- Airflow packages in freeze: 2" in summary
    assert "`apache-airflow==3.2.0`" in summary
    print(summary)


def test_generate_report_preserves_best_effort_behavior_on_command_failure(
    tmp_path, dependency_report_module, monkeypatch
):
    env = {"JOB_ID": "core-cli-3.12"}

    freeze_result = dependency_report_module.CommandResult(
        command=("uv", "pip", "freeze"),
        returncode=1,
        stdout="",
        stderr="freeze failed",
    )
    tree_result = dependency_report_module.CommandResult(
        command=("uv", "tree", "--no-dedupe"),
        returncode=1,
        stdout="",
        stderr="tree failed",
    )

    def fake_run_command(command):
        return freeze_result if command == ("uv", "pip", "freeze") else tree_result

    monkeypatch.setattr(dependency_report_module, "run_command", fake_run_command)

    report_paths = dependency_report_module.generate_report(output_dir=tmp_path, env=env)

    assert json.loads(report_paths["depth"].read_text(encoding="utf-8")) == {}
    assert report_paths["freeze"].with_suffix(".txt.stderr").read_text(encoding="utf-8") == "freeze failed"
    assert report_paths["tree"].with_suffix(".txt.stderr").read_text(encoding="utf-8") == "tree failed"
    summary = report_paths["summary"].read_text(encoding="utf-8")
    assert "failed (1)" in summary
    assert "freeze failed" in summary
    assert "tree failed" in summary
