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
import subprocess
import sys
from pathlib import Path
from unittest import mock

import pytest

MODULE_PATH = Path(__file__).resolve().parents[3] / "scripts" / "ci" / "delete_retired_image_packages.py"
KEEP = ["--keep-branches", "main v3-4-test", "--keep-python", "3.11 3.12"]


@pytest.fixture
def script():
    module_name = "test_delete_retired_image_packages_module"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, MODULE_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _completed(returncode: int = 0, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess([], returncode, stdout=stdout, stderr=stderr)


@pytest.mark.parametrize(
    ("name", "retired"),
    [
        pytest.param("airflow/main/ci/python3.11", False, id="kept-branch-and-python"),
        pytest.param("airflow/v3-4-test/prod/python3.12", False, id="kept-prod"),
        pytest.param("airflow/main/ci/python3.10", True, id="retired-python-on-kept-branch"),
        pytest.param("airflow/v3-3-test/ci/python3.11", True, id="retired-branch"),
        pytest.param("airflow/v2-2-test/prod/python3.6", True, id="retired-both"),
        pytest.param("airflow/main/ci/python3.1", True, id="python-prefix-is-not-kept"),
        pytest.param("airflow-site/main/ci/python3.10", False, id="other-repo"),
        pytest.param("airflow/main/docs/python3.10", False, id="not-an-image-package"),
        pytest.param("airflow/main/ci/python3.10/extra", False, id="deeper-name"),
    ],
)
def test_find_retired_packages(script, name, retired):
    found = script.find_retired_packages([name], "airflow", {"main", "v3-4-test"}, {"3.11", "3.12"})

    assert found == ([name] if retired else [])


@pytest.mark.parametrize(
    ("only", "expected"),
    [
        pytest.param(None, ["airflow/main/ci/python3.10", "airflow/v2-2-test/ci/python3.7"], id="no-filter"),
        pytest.param(["airflow/v2-*"], ["airflow/v2-2-test/ci/python3.7"], id="branch-glob"),
        pytest.param(["airflow/main/ci/python3.10"], ["airflow/main/ci/python3.10"], id="exact-name"),
        pytest.param(["airflow/main/ci/python3.11"], [], id="never-a-kept-package"),
    ],
)
def test_only_narrows_the_retired_packages(script, only, expected):
    names = ["airflow/main/ci/python3.11", "airflow/main/ci/python3.10", "airflow/v2-2-test/ci/python3.7"]

    assert script.find_retired_packages(names, "airflow", {"main"}, {"3.11"}, only) == expected


def test_dry_run_deletes_nothing(script, monkeypatch, capsys):
    monkeypatch.setenv("GITHUB_REPOSITORY", "apache/airflow")
    monkeypatch.setattr(sys, "argv", ["script", *KEEP, "--dry-run"])
    listing = "airflow/main/ci/python3.11\nairflow/main/ci/python3.10\n"
    with mock.patch.object(script, "_gh_api", autospec=True, return_value=_completed(stdout=listing)) as gh:
        assert script.main() == 0

    assert gh.call_count == 1
    assert "Would delete airflow/main/ci/python3.10" in capsys.readouterr().out


def test_deletes_retired_packages_and_reports_failures(script, monkeypatch, capsys):
    monkeypatch.setenv("GITHUB_REPOSITORY", "apache/airflow")
    monkeypatch.setattr(sys, "argv", ["script", *KEEP])
    listing = "airflow/main/ci/python3.11\nairflow/main/ci/python3.10\nairflow/v2-2-test/ci/python3.7\n"

    def gh_api(args):
        if args[0] == "--paginate":
            return _completed(stdout=listing)
        if args[-1].endswith("v2-2-test%2Fci%2Fpython3.7"):
            return _completed(returncode=1, stderr="Package not found. (HTTP 404)")
        return _completed()

    with mock.patch.object(script, "_gh_api", autospec=True, side_effect=gh_api) as gh:
        assert script.main() == 1

    deleted = [call.args[0][-1] for call in gh.call_args_list if call.args[0][0] == "-X"]
    assert sorted(deleted) == [
        "/orgs/apache/packages/container/airflow%2Fmain%2Fci%2Fpython3.10",
        "/orgs/apache/packages/container/airflow%2Fv2-2-test%2Fci%2Fpython3.7",
    ]
    out = capsys.readouterr().out
    assert "Deleted airflow/main/ci/python3.10" in out
    assert "Could not delete airflow/v2-2-test/ci/python3.7: Package not found. (HTTP 404)" in out


@pytest.mark.parametrize(
    "keep",
    [["--keep-branches", " ", "--keep-python", "3.11"], ["--keep-branches", "main", "--keep-python", ""]],
)
def test_refuses_to_run_without_anything_to_keep(script, monkeypatch, keep):
    monkeypatch.setenv("GITHUB_REPOSITORY", "apache/airflow")
    monkeypatch.setattr(sys, "argv", ["script", *keep])
    with mock.patch.object(script, "_gh_api", autospec=True) as gh:
        assert script.main() == 1

    gh.assert_not_called()
