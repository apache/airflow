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

import contextlib
import json
from pathlib import Path
from unittest import mock

import pytest

from airflow_breeze.utils.constraints_version_check import (
    explain_package_upgrade,
    get_table_format,
    process_packages,
)

MODULE = "airflow_breeze.utils.constraints_version_check"

# Old enough that the default 4-day cooldown never filters these releases out.
OLD_UPLOAD_TIME = "2020-01-01T00:00:00.000000Z"


def _pypi_payload(latest: str, *versions: str) -> bytes:
    releases = {v: [{"upload_time_iso_8601": OLD_UPLOAD_TIME, "yanked": False}] for v in versions}
    return json.dumps({"info": {"version": latest}, "releases": releases}).encode()


@pytest.fixture
def pypi(monkeypatch):
    """Serve every package the same two-release history: pinned 1.0.0, latest 2.0.0."""

    def fake_urlopen(_url):
        response = mock.MagicMock()
        response.read.return_value = _pypi_payload("2.0.0", "1.0.0", "2.0.0")
        return contextlib.nullcontext(response)

    monkeypatch.setattr(f"{MODULE}.urllib.request.urlopen", fake_urlopen)


def _run_process_packages(packages, explain_why=True):
    col_widths, format_str, _, _ = get_table_format(packages)
    return process_packages(
        packages=packages,
        constraints_date=None,
        mode="full",
        explain_why=explain_why,
        col_widths=col_widths,
        format_str=format_str,
        python_version="3.11",
        airflow_constraints_mode="constraints",
        github_repository="apache/airflow",
    )


@mock.patch(f"{MODULE}.explain_package_upgrade", return_value="explanation")
@mock.patch(f"{MODULE}.resolve_baseline_versions", return_value=("baseline log", {"pkg-a": "1.0.0"}))
def test_baseline_is_resolved_once_for_all_outdated_packages(mock_baseline, mock_explain, pypi):
    packages = [("pkg-a", "1.0.0"), ("pkg-b", "1.0.0"), ("pkg-c", "1.0.0")]

    _, _, explanations, _ = _run_process_packages(packages)

    assert mock_explain.call_count == 3
    assert len(explanations) == 3
    mock_baseline.assert_called_once_with(
        python_version="3.11",
        airflow_constraints_mode="constraints",
        github_repository="apache/airflow",
    )
    for call in mock_explain.call_args_list:
        assert call.kwargs["baseline_text"] == "baseline log"
        assert call.kwargs["baseline_versions"] == {"pkg-a": "1.0.0"}


@mock.patch(f"{MODULE}.explain_package_upgrade", return_value="explanation")
@mock.patch(f"{MODULE}.resolve_baseline_versions")
def test_baseline_is_not_resolved_when_nothing_needs_explaining(mock_baseline, mock_explain, pypi):
    # Already at the latest version, so no package triggers an explanation.
    _run_process_packages([("pkg-a", "2.0.0"), ("pkg-b", "2.0.0")])

    mock_explain.assert_not_called()
    mock_baseline.assert_not_called()


@mock.patch(f"{MODULE}.explain_package_upgrade")
@mock.patch(f"{MODULE}.resolve_baseline_versions")
def test_baseline_is_not_resolved_without_explain_why(mock_baseline, mock_explain, pypi):
    _run_process_packages([("pkg-a", "1.0.0")], explain_why=False)

    mock_explain.assert_not_called()
    mock_baseline.assert_not_called()


@mock.patch(f"{MODULE}.update_pyproject_dependency")
@mock.patch(f"{MODULE}.preserve_files")
@mock.patch(f"{MODULE}.sync_and_freeze")
def test_explain_package_upgrade_syncs_only_the_pinned_resolution(
    mock_sync, mock_preserve, mock_update_pyproject
):
    mock_preserve.return_value = contextlib.nullcontext()
    mock_sync.return_value = (mock.MagicMock(returncode=0), "after log", {"pkg-a": "2.0.0"})

    explanation = explain_package_upgrade(
        pkg="pkg-a",
        pinned_version="1.0.0",
        latest_version="2.0.0",
        python_version="3.11",
        airflow_constraints_mode="constraints",
        github_repository="apache/airflow",
        baseline_text="baseline log",
        baseline_versions={"pkg-a": "1.0.0"},
    )

    mock_sync.assert_called_once()
    assert mock_sync.call_args.kwargs["title"] == "output_after"
    assert "can be upgraded from 1.0.0 to 2.0.0" in explanation


@mock.patch(f"{MODULE}.update_pyproject_dependency")
@mock.patch(f"{MODULE}.preserve_files")
@mock.patch(f"{MODULE}.execute_command_in_shell")
@mock.patch(f"{MODULE}.sync_and_freeze")
def test_explain_package_upgrade_reads_baseline_for_downgrade_detection(
    mock_sync, mock_conflict_probe, mock_preserve, mock_update_pyproject
):
    mock_preserve.return_value = contextlib.nullcontext()

    def write_conflict_narrative(*_args, **kwargs):
        # The downgrade branch reruns uv from scratch to capture the resolver narrative.
        Path(kwargs["output"].file_name).write_text(
            "No solution found\nBecause pkg-a==2.0.0 depends on other-pkg<5"
        )
        return mock.MagicMock(returncode=1)

    mock_conflict_probe.side_effect = write_conflict_narrative
    # Reaching pkg-a 2.0.0 pushed other-pkg back from 5.0.0 to 4.0.0.
    mock_sync.return_value = (
        mock.MagicMock(returncode=0),
        "after log",
        {"pkg-a": "2.0.0", "other-pkg": "4.0.0"},
    )

    explanation = explain_package_upgrade(
        pkg="pkg-a",
        pinned_version="1.0.0",
        latest_version="2.0.0",
        python_version="3.11",
        airflow_constraints_mode="constraints",
        github_repository="apache/airflow",
        baseline_text="baseline log",
        baseline_versions={"pkg-a": "1.0.0", "other-pkg": "5.0.0"},
    )

    assert "only by DOWNGRADING" in explanation
    assert "other-pkg: 5.0.0 -> 4.0.0" in explanation
