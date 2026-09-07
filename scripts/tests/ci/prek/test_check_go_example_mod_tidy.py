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


def test_tidy_module_passes():
    exit_code, report = checker.format_report(0, "")

    assert exit_code == 0
    assert "is tidy" in report


def test_untidy_module_fails_with_the_fix_command_and_the_diff():
    exit_code, report = checker.format_report(1, GRPC_DRIFT_DIFF)

    assert exit_code == 1
    assert "is not tidy" in report
    assert "(cd kubernetes-tests/lang_sdk/go_example && go mod tidy)" in report
    # The reason the contributor cares: this is what turns K8S Lang-SDK red for everyone.
    assert "K8S Lang-SDK" in report
    assert "google.golang.org/grpc v1.82.1" in report


def test_untidy_module_without_diff_output_still_reports():
    exit_code, report = checker.format_report(1, "")

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
