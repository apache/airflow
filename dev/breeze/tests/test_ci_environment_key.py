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
import shutil
import subprocess
from unittest import mock

import pytest
from click.testing import CliRunner

from airflow_breeze.commands.ci_image_commands import environment_key
from airflow_breeze.global_constants import FILES_FOR_REBUILD_CHECK
from airflow_breeze.utils.ci_environment_key import calculate_ci_environment_fingerprint


@pytest.fixture
def checkout(tmp_path):
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    paths = [
        *FILES_FOR_REBUILD_CHECK,
        "uv.lock",
        "pyproject.toml",
        "providers/example/pyproject.toml",
        "providers/example/provider.yaml",
        "shared/example/pyproject.toml",
        "providers/example/src/operator.py",
        "providers/example/tests/test_operator.py",
    ]
    for path in paths:
        file = tmp_path / path
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text(path)
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True)
    return tmp_path


def fingerprint(root, **kwargs):
    return calculate_ci_environment_fingerprint(
        root, **{"python": "3.12", "platform": "linux/amd64", **kwargs}
    )


@pytest.mark.parametrize(
    "path",
    [
        *FILES_FOR_REBUILD_CHECK,
        "uv.lock",
        "pyproject.toml",
        "providers/example/pyproject.toml",
        "providers/example/provider.yaml",
        "shared/example/pyproject.toml",
    ],
)
def test_environment_inputs_invalidate_key(checkout, path):
    before = fingerprint(checkout)
    (checkout / path).write_text("changed")
    assert fingerprint(checkout)["key"] != before["key"]


@pytest.mark.parametrize(
    "path", ["providers/example/src/operator.py", "providers/example/tests/test_operator.py"]
)
def test_implementation_and_tests_do_not_invalidate_key(checkout, path):
    before = fingerprint(checkout)
    (checkout / path).write_text("changed")
    assert fingerprint(checkout) == before


@pytest.mark.parametrize("operation", ["add", "delete", "rename"])
def test_metadata_membership_invalidates_key(checkout, operation):
    before = fingerprint(checkout)
    path = "providers/example/provider.yaml"
    if operation == "add":
        (checkout / "providers/example/new").mkdir()
        (checkout / "providers/example/new/provider.yaml").write_text("new")
        subprocess.run(["git", "add", "."], cwd=checkout, check=True)
    elif operation == "delete":
        subprocess.run(["git", "rm", "-f", path], cwd=checkout, check=True)
    else:
        subprocess.run(["git", "mv", path, "providers/example/new-provider.yaml"], cwd=checkout, check=True)
    assert fingerprint(checkout)["key"] != before["key"]


@pytest.mark.parametrize("coordinates", [{"python": "3.13"}, {"platform": "linux/arm64"}])
def test_execution_coordinates_invalidate_key(checkout, coordinates):
    assert fingerprint(checkout)["key"] != fingerprint(checkout, **coordinates)["key"]


def test_key_is_versioned_and_observational(checkout, monkeypatch):
    before = fingerprint(checkout)
    assert before["reuse_eligible"] is False
    assert before["unresolved_inputs"] == ["base_image_digest", "effective_build_parameters"]
    monkeypatch.setattr("airflow_breeze.utils.ci_environment_key.ENVIRONMENT_KEY_SCHEMA_VERSION", 2)
    assert fingerprint(checkout)["key"] != before["key"]


def test_order_and_checkout_location_do_not_affect_key(checkout, tmp_path_factory):
    before = fingerprint(checkout)
    other = tmp_path_factory.mktemp("other")

    shutil.copytree(checkout, other, dirs_exist_ok=True)
    assert fingerprint(other) == before
    tracked = subprocess.check_output(["git", "ls-files", "-z"], cwd=checkout, text=True)
    with mock.patch(
        "airflow_breeze.utils.ci_environment_key.subprocess.check_output",
        autospec=True,
        return_value="\0".join(reversed(tracked.split("\0"))),
    ):
        assert fingerprint(checkout) == before


@mock.patch("airflow_breeze.commands.ci_image_commands.calculate_ci_environment_fingerprint", autospec=True)
@pytest.mark.parametrize("github_actions", ["false", "true"])
def test_command_prints_key_without_building(mock_calculate, monkeypatch, github_actions):
    monkeypatch.setenv("CI", "true")
    monkeypatch.setenv("GITHUB_ACTIONS", github_actions)
    mock_calculate.return_value = {"key": "observed", "reuse_eligible": False}
    result = CliRunner().invoke(environment_key, ["--python", "3.12", "--platform", "linux/amd64"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output.splitlines()[0]) == mock_calculate.return_value
    assert mock_calculate.call_args.args[1:] == ("3.12", "linux/amd64")
