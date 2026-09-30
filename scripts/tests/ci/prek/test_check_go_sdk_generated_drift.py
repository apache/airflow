#!/usr/bin/env python3
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

import check_go_sdk_generated_drift as checker
import pytest

SPECS, MODELS = checker.TARGETS

SPEC_DRIFT_DIFF = """\
diff --git a/go-sdk/airflow/spec.gen.go b/go-sdk/airflow/spec.gen.go
--- a/go-sdk/airflow/spec.gen.go
+++ b/go-sdk/airflow/spec.gen.go
@@ -40,6 +40,9 @@ type DagSpec struct {
+    // Deadline corresponds to the JSON schema field "deadline".
+    Deadline string
"""

MODELS_DRIFT_DIFF = """\
diff --git a/go-sdk/pkg/execution/genmodels/models.gen.go b/go-sdk/pkg/execution/genmodels/models.gen.go
--- a/go-sdk/pkg/execution/genmodels/models.gen.go
+++ b/go-sdk/pkg/execution/genmodels/models.gen.go
@@ -1624,6 +1624,9 @@ type TIRunContext struct {
+    MultiTeam bool `msgpack:"multi_team,omitempty"`
"""


def test_both_targets_are_checked_and_name_generated_files_not_a_package_directory():
    assert [target.package for target in checker.TARGETS] == [
        "./airflow/...",
        "./pkg/execution/genmodels/...",
    ]
    # A package directory would widen `git diff` onto the hand-written gen.go beside the
    # generated files, and hide a deleted one from the missing-file guard in main().
    for target in checker.TARGETS:
        assert target.committed
        assert all(path.name.endswith(".gen.go") for path in target.committed), target.committed


def test_current_files_pass():
    exit_code, report = checker.format_report(SPECS, 0, "", 0, "")

    assert exit_code == 0
    assert "up to date with airflow-core/src/airflow/serialization/schema.json" in report


def test_drifted_specs_fail_with_the_diff_and_where_to_decide_about_a_property():
    exit_code, report = checker.format_report(SPECS, 0, "", 0, SPEC_DRIFT_DIFF)

    assert exit_code == 1
    assert "out of date" in report
    assert "go-sdk/internal/genspec/authoring.go" in report
    assert "git add go-sdk/airflow/spec.gen.go" in report
    assert "Deadline string" in report


def test_drifted_models_name_the_supervisor_snapshot_and_have_nothing_to_decide():
    exit_code, report = checker.format_report(MODELS, 0, "", 0, MODELS_DRIFT_DIFF)

    assert exit_code == 1
    assert "task-sdk/src/airflow/sdk/execution_time/schema/schema.json" in report
    assert (
        "git add go-sdk/pkg/execution/genmodels/models.gen.go "
        "go-sdk/pkg/execution/genmodels/discriminators.gen.go "
        "go-sdk/pkg/execution/genmodels/defaults.gen.go" in report
    )
    # Nothing is excluded from the models, so there is no list to weigh a field against.
    assert "authoring.go" not in report
    assert "MultiTeam" in report


def test_failed_generation_reports_the_generator_output_instead_of_a_diff():
    exit_code, report = checker.format_report(
        SPECS,
        1,
        "genspec: shaping schema.json for authoring: definitions/dag/properties/fileloc is excluded",
        0,
        "",
    )

    assert exit_code == 1
    assert "failed" in report
    assert "definitions/dag/properties/fileloc is excluded" in report


def test_failed_generation_without_output_still_reports():
    exit_code, report = checker.format_report(SPECS, 1, "", 0, "")

    assert exit_code == 1
    assert "(no output)" in report


def test_unreadable_diff_fails_instead_of_passing_as_no_drift():
    exit_code, report = checker.format_report(SPECS, 0, "", 128, "")

    assert exit_code == 1
    assert "is unknown" in report


@mock.patch("check_go_sdk_generated_drift.subprocess.run", autospec=True)
def test_regeneration_runs_one_targets_generators_in_the_go_sdk_module(mock_run, tmp_path):
    mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")

    checker.regenerate(tmp_path, MODELS.package)

    assert mock_run.call_args.args[0] == ["go", "generate", "./pkg/execution/genmodels/..."]
    assert mock_run.call_args.kwargs["cwd"] == tmp_path


@mock.patch("check_go_sdk_generated_drift.subprocess.run", autospec=True)
def test_regeneration_combines_stdout_and_stderr(mock_run, tmp_path):
    mock_run.return_value = subprocess.CompletedProcess(
        args=[], returncode=1, stdout="genspec: shaping failed\n", stderr="exit status 1\n"
    )

    returncode, output = checker.regenerate(tmp_path, SPECS.package)

    assert returncode == 1
    assert "genspec: shaping failed" in output
    assert "exit status 1" in output


@mock.patch("check_go_sdk_generated_drift.subprocess.run", autospec=True)
def test_read_drift_asks_git_only_about_one_targets_files(mock_run, tmp_path):
    mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")

    checker.read_drift(tmp_path, (pathlib.Path("go-sdk/airflow/spec.gen.go"),))

    assert mock_run.call_args.args[0] == ["git", "diff", "--", "go-sdk/airflow/spec.gen.go"]
    assert mock_run.call_args.kwargs["cwd"] == tmp_path


@pytest.mark.parametrize(
    ("ci_env", "expected_exit", "expected_text"),
    [
        pytest.param({"CI": "true"}, 1, "this is a CI run", id="ci-fails-loudly"),
        pytest.param({}, 0, "SKIPPED", id="local-skips"),
    ],
)
@mock.patch("check_go_sdk_generated_drift.shutil.which", autospec=True, return_value=None)
def test_missing_go_toolchain(mock_which, ci_env, expected_exit, expected_text, monkeypatch, capsys):
    monkeypatch.delenv("CI", raising=False)
    for key, value in ci_env.items():
        monkeypatch.setenv(key, value)

    assert checker.main() == expected_exit
    assert expected_text in capsys.readouterr().out


@mock.patch("check_go_sdk_generated_drift.pathlib.Path.exists", autospec=True, return_value=False)
def test_missing_generated_file_fails(mock_exists, capsys):
    assert checker.main() == 1
    assert "not found" in capsys.readouterr().out
