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

import pathlib
import subprocess
from unittest import mock

import check_go_example_mod_tidy as checker
import pytest

# Trimmed to the shape that matters: the drift #70226 introduced and #70561 cleaned up.
GRPC_DRIFT_DIFF = """\
diff current/go.mod tidy/go.mod
--- current/go.mod
+++ tidy/go.mod
@@ -37,9 +37,9 @@
-    google.golang.org/grpc v1.79.3 // indirect
+    google.golang.org/grpc v1.82.1 // indirect
"""


MODULES = pytest.mark.parametrize(
    ("module", "job"),
    [
        pytest.param(
            pathlib.Path("kubernetes-tests/lang_sdk/go_example"),
            "Kubernetes tests / K8S Lang-SDK",
            id="kubernetes-example",
        ),
        pytest.param(
            pathlib.Path("airflow-e2e-tests/go-test-bundle"), "Go SDK e2e test", id="e2e-test-bundle"
        ),
    ],
)


def test_each_go_module_is_checked_with_the_job_it_breaks():
    assert {str(module): job for module, job in checker.GO_MODULES.items()} == {
        "kubernetes-tests/lang_sdk/go_example": "Kubernetes tests / K8S Lang-SDK",
        "airflow-e2e-tests/go-test-bundle": "Go SDK e2e test",
    }


@MODULES
def test_tidy_module_passes(module, job):
    exit_code, report = checker.format_report(module, job, 0, "")

    assert exit_code == 0
    assert f"{module} is tidy" in report


@MODULES
def test_untidy_module_fails_with_the_fix_command_and_the_diff(module, job):
    exit_code, report = checker.format_report(module, job, 1, GRPC_DRIFT_DIFF)

    assert exit_code == 1
    assert f"{module} is not tidy" in report
    assert f"(cd {module} && go mod tidy)" in report
    # The reason the contributor cares: this is what turns the job red for everyone.
    assert job in report
    assert "google.golang.org/grpc v1.82.1" in report


@MODULES
def test_untidy_module_without_diff_output_still_reports(module, job):
    exit_code, report = checker.format_report(module, job, 1, "")

    assert exit_code == 1
    assert "(no output)" in report


@mock.patch("check_go_example_mod_tidy.subprocess.run", autospec=True)
def test_run_tidy_diff_never_writes_to_the_working_tree(mock_run, tmp_path):
    mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")

    checker.run_tidy_diff(tmp_path)

    args = mock_run.call_args.args[0]
    assert args == ["go", "mod", "tidy", "-diff"]
    assert mock_run.call_args.kwargs["cwd"] == tmp_path


@mock.patch("check_go_example_mod_tidy.subprocess.run", autospec=True)
def test_run_tidy_diff_combines_stdout_and_stderr(mock_run, tmp_path):
    mock_run.return_value = subprocess.CompletedProcess(
        args=[], returncode=1, stdout="diff current/go.mod tidy/go.mod\n", stderr="go: downloading\n"
    )

    returncode, output = checker.run_tidy_diff(tmp_path)

    assert returncode == 1
    assert "diff current/go.mod tidy/go.mod" in output
    assert "go: downloading" in output


@pytest.mark.parametrize(
    ("ci_env", "expected_exit", "expected_text"),
    [
        pytest.param({"CI": "true"}, 1, "this is a CI run", id="ci-fails-loudly"),
        pytest.param({}, 0, "SKIPPED", id="local-skips"),
    ],
)
@mock.patch("check_go_example_mod_tidy.shutil.which", return_value=None)
def test_missing_go_toolchain(mock_which, ci_env, expected_exit, expected_text, monkeypatch, capsys):
    monkeypatch.delenv("CI", raising=False)
    for key, value in ci_env.items():
        monkeypatch.setenv(key, value)

    assert checker.main() == expected_exit
    assert expected_text in capsys.readouterr().out


@pytest.mark.parametrize(
    ("untidy_module", "expected_exit"),
    [
        pytest.param(None, 0, id="all-tidy"),
        pytest.param("kubernetes-tests/lang_sdk/go_example", 1, id="only-the-kubernetes-example-is-untidy"),
        pytest.param("airflow-e2e-tests/go-test-bundle", 1, id="only-the-e2e-test-bundle-is-untidy"),
    ],
)
@mock.patch("check_go_example_mod_tidy.run_tidy_diff", autospec=True)
@mock.patch("check_go_example_mod_tidy.shutil.which", return_value="/usr/bin/go")
def test_main_fails_when_any_module_is_untidy(
    mock_which, mock_run_tidy_diff, untidy_module, expected_exit, capsys
):
    def run_tidy_diff(module_dir):
        untidy = untidy_module is not None and module_dir == checker.REPO_ROOT / untidy_module
        return (1, GRPC_DRIFT_DIFF) if untidy else (0, "")

    mock_run_tidy_diff.side_effect = run_tidy_diff

    assert checker.main() == expected_exit
    assert mock_run_tidy_diff.call_count == len(checker.GO_MODULES)
    output = capsys.readouterr().out
    assert ("is not tidy" in output) == (untidy_module is not None)
