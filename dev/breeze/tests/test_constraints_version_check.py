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
import re
import threading
import time
from pathlib import Path
from unittest import mock
from urllib.error import HTTPError, URLError

import pytest

from airflow_breeze.utils.constraints_version_check import (
    PYPI_FETCH_MAX_ATTEMPTS,
    PYPI_FETCH_PARALLELISM,
    PYPI_FETCH_TIMEOUT_SECONDS,
    explain_package_upgrade,
    fetch_pypi_data,
    get_table_format,
    iter_pypi_data,
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

    def fake_urlopen(_url, timeout=None):
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


def test_pypi_metadata_is_fetched_concurrently(monkeypatch):
    """A constraints file pins hundreds of packages; fetching them serially is the job's runtime."""
    in_flight = 0
    peak = 0
    lock = threading.Lock()

    def slow_fetch(pkg: str) -> dict:
        nonlocal in_flight, peak
        with lock:
            in_flight += 1
            peak = max(peak, in_flight)
        time.sleep(0.05)
        with lock:
            in_flight -= 1
        return {"pkg": pkg}

    monkeypatch.setattr(f"{MODULE}.fetch_pypi_data", slow_fetch)

    results = list(iter_pypi_data([f"pkg-{i}" for i in range(20)]))

    assert results == [{"pkg": f"pkg-{i}"} for i in range(20)]
    assert 1 < peak <= PYPI_FETCH_PARALLELISM


def test_only_one_batch_is_held_in_memory_at_a_time(monkeypatch):
    """A single project's release history can be tens of MB - all ~770 would not fit."""
    fetched: list[str] = []

    def fetch(pkg: str) -> dict:
        fetched.append(pkg)
        return {"pkg": pkg}

    monkeypatch.setattr(f"{MODULE}.fetch_pypi_data", fetch)

    packages = [f"pkg-{i}" for i in range(PYPI_FETCH_PARALLELISM * 3)]
    stream = iter_pypi_data(packages)
    next(stream)

    # Consuming the first result must not have fetched beyond the first batch.
    assert len(fetched) <= PYPI_FETCH_PARALLELISM


def test_a_failed_fetch_is_yielded_against_its_own_package(monkeypatch):
    def fetch(pkg: str) -> dict:
        if pkg == "pkg-b":
            raise URLError("boom")
        return {"pkg": pkg}

    monkeypatch.setattr(f"{MODULE}.fetch_pypi_data", fetch)

    results = list(iter_pypi_data(["pkg-a", "pkg-b", "pkg-c"]))

    assert results[0] == {"pkg": "pkg-a"}
    assert isinstance(results[1], URLError)
    assert results[2] == {"pkg": "pkg-c"}


def test_fetch_uses_a_timeout(monkeypatch):
    """Without one, a stalled PyPI connection hangs the job until the CI job timeout."""
    calls = {}

    def fake_urlopen(url, timeout=None):
        calls["url"] = url
        calls["timeout"] = timeout
        response = mock.MagicMock()
        response.read.return_value = _pypi_payload("2.0.0", "2.0.0")
        return contextlib.nullcontext(response)

    monkeypatch.setattr(f"{MODULE}.urllib.request.urlopen", fake_urlopen)

    fetch_pypi_data("pkg-a")

    assert calls["url"] == "https://pypi.org/pypi/pkg-a/json"
    assert calls["timeout"] == PYPI_FETCH_TIMEOUT_SECONDS


def test_every_row_keeps_its_own_packages_data_however_the_fetches_finish(monkeypatch, capsys):
    """Results are matched to packages by position, so returning them out of order would put
    another package's versions on the row - a silent mismatch rather than a visible reshuffle."""
    count = PYPI_FETCH_PARALLELISM * 2
    packages = [(f"pkg-{i:02d}", "1.0.0") for i in range(count)]

    def fetch(pkg: str) -> dict:
        index = int(pkg.removeprefix("pkg-"))
        # Finish in reverse, so completion order is the opposite of constraints-file order.
        time.sleep((count - index) * 0.01)
        return json.loads(_pypi_payload(f"{index + 2}.0.0", "1.0.0", f"{index + 2}.0.0").decode())

    monkeypatch.setattr(f"{MODULE}.fetch_pypi_data", fetch)

    _run_process_packages(packages, explain_why=False)

    plain = re.sub(r"\x1b\[[0-9;]*m", "", capsys.readouterr().out)
    printed = re.findall(r"pypi\.org/project/(pkg-\d+)/(\d+\.\d+\.\d+)", plain)

    assert printed == [(f"pkg-{i:02d}", f"{i + 2}.0.0") for i in range(count)]


def test_a_throttled_fetch_is_retried(monkeypatch):
    """PyPI can throttle a burst of parallel requests; one 429 should not fail the package."""
    attempts = []

    def urlopen(url, timeout=None):
        attempts.append(url)
        if len(attempts) == 1:
            raise HTTPError(url, 429, "Too Many Requests", {}, None)
        response = mock.MagicMock()
        response.read.return_value = _pypi_payload("2.0.0", "2.0.0")
        return contextlib.nullcontext(response)

    monkeypatch.setattr(f"{MODULE}.urllib.request.urlopen", urlopen)
    monkeypatch.setattr(f"{MODULE}.time.sleep", lambda _: None)

    assert fetch_pypi_data("pkg-a")["info"]["version"] == "2.0.0"
    assert len(attempts) == 2


def test_a_throttled_fetch_waits_for_retry_after(monkeypatch):
    """PyPI says how long to wait when it throttles; guessing shorter just gets throttled again."""
    slept = []

    def urlopen(url, timeout=None):
        if not slept:
            raise HTTPError(url, 429, "Too Many Requests", {"Retry-After": "7"}, None)
        response = mock.MagicMock()
        response.read.return_value = _pypi_payload("2.0.0", "2.0.0")
        return contextlib.nullcontext(response)

    monkeypatch.setattr(f"{MODULE}.urllib.request.urlopen", urlopen)
    monkeypatch.setattr(f"{MODULE}.time.sleep", slept.append)

    fetch_pypi_data("pkg-a")

    assert slept == [7.0]


def test_a_missing_package_is_not_retried(monkeypatch):
    """A 404 is an answer, not congestion - retrying it just multiplies the wait."""
    attempts = []

    def urlopen(url, timeout=None):
        attempts.append(url)
        raise HTTPError(url, 404, "Not Found", {}, None)

    monkeypatch.setattr(f"{MODULE}.urllib.request.urlopen", urlopen)
    monkeypatch.setattr(f"{MODULE}.time.sleep", lambda _: None)

    with pytest.raises(HTTPError):
        fetch_pypi_data("pkg-a")
    assert len(attempts) == 1


def test_a_fetch_that_stays_throttled_fails_against_its_package(monkeypatch):
    """Retries are bounded; the last failure has to surface rather than loop forever."""
    attempts = []

    def urlopen(url, timeout=None):
        attempts.append(url)
        raise HTTPError(url, 503, "Service Unavailable", {}, None)

    monkeypatch.setattr(f"{MODULE}.urllib.request.urlopen", urlopen)
    monkeypatch.setattr(f"{MODULE}.time.sleep", lambda _: None)

    with pytest.raises(HTTPError):
        fetch_pypi_data("pkg-a")
    assert len(attempts) == PYPI_FETCH_MAX_ATTEMPTS


def test_a_failed_package_does_not_stop_the_others(pypi, monkeypatch, capsys):
    real_fetch = fetch_pypi_data

    def fetch(pkg: str):
        if pkg == "pkg-b":
            raise HTTPError("https://pypi.org/pypi/pkg-b/json", 503, "boom", {}, None)  # type: ignore[arg-type]
        return real_fetch(pkg)

    monkeypatch.setattr(f"{MODULE}.fetch_pypi_data", fetch)

    _, _, _, status_counts = _run_process_packages(
        [("pkg-a", "2.0.0"), ("pkg-b", "2.0.0"), ("pkg-c", "2.0.0")], explain_why=False
    )

    assert status_counts["ok"] == 2
    printed = re.sub(r"\x1b\[[0-9;]*m", "", capsys.readouterr().out)
    assert "Error fetching pkg-b from PyPI: HTTP 503" in printed


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
