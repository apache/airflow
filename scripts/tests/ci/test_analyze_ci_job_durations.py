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
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

MODULE_PATH = Path(__file__).resolve().parents[3] / "scripts" / "ci" / "analyze_ci_job_durations.py"


@pytest.fixture
def durations_module():
    module_name = "test_analyze_ci_job_durations_module"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, MODULE_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestGhApi:
    def test_forces_get_method(self, durations_module):
        """`gh api` defaults to POST when -f is passed; we must force GET to avoid 404."""
        completed = subprocess.CompletedProcess(args=[], returncode=0, stdout="{}", stderr="")
        with patch.object(subprocess, "run", return_value=completed) as mock_run:
            durations_module.gh_api("repos/apache/airflow/actions/workflows/x/runs", branch="main")
        args = mock_run.call_args[0][0]
        assert "--method" in args
        assert args[args.index("--method") + 1] == "GET"


class TestParseIso:
    def test_parses_z_suffix(self, durations_module):
        dt = durations_module.parse_iso("2026-06-10T13:01:53Z")
        assert dt is not None
        assert dt.year == 2026 and dt.hour == 13

    def test_parses_offset(self, durations_module):
        dt = durations_module.parse_iso("2026-06-10T13:01:53+00:00")
        assert dt is not None

    def test_none_for_empty(self, durations_module):
        assert durations_module.parse_iso("") is None
        assert durations_module.parse_iso(None) is None

    def test_none_for_garbage(self, durations_module):
        assert durations_module.parse_iso("not-a-date") is None


class TestDurationSeconds:
    def test_computes_positive_duration(self, durations_module):
        seconds = durations_module.duration_seconds("2026-06-10T13:00:00Z", "2026-06-10T13:29:00Z")
        assert seconds == 29 * 60

    def test_none_when_unparsable(self, durations_module):
        assert durations_module.duration_seconds("bad", "2026-06-10T13:29:00Z") is None

    def test_none_when_negative(self, durations_module):
        """A clock skew / out-of-order pair must not produce a negative duration."""
        assert durations_module.duration_seconds("2026-06-10T13:29:00Z", "2026-06-10T13:00:00Z") is None


class TestMedian:
    def test_odd(self, durations_module):
        assert durations_module.median([3, 1, 2]) == 2

    def test_even(self, durations_module):
        assert durations_module.median([1, 2, 3, 4]) == 2.5


class TestFormatDuration:
    def test_minutes_and_seconds(self, durations_module):
        assert durations_module.format_duration(29 * 60 + 41) == "29m 41s"

    def test_seconds_only(self, durations_module):
        assert durations_module.format_duration(45) == "45s"

    def test_zero_pads_seconds(self, durations_module):
        assert durations_module.format_duration(60 + 5) == "1m 05s"


class TestFormatDurationDelta:
    def test_positive(self, durations_module):
        assert durations_module.format_duration_delta(60 + 5) == "+1m 05s"

    def test_negative(self, durations_module):
        assert durations_module.format_duration_delta(-(60 + 5)) == "-1m 05s"


class TestDetectRegression:
    def test_flags_regression_above_both_thresholds(self, durations_module):
        # baseline median ~1800s (30m), latest 2700s (45m) -> +50%, +15m
        regression = durations_module.detect_regression(
            latest_values=[2700],
            baseline_values=[1800, 1810, 1790, 1805, 1795],
            rel_threshold=0.25,
            min_abs_increase_seconds=300,
        )
        assert regression is not None
        assert regression["latest"] == 2700
        assert round(regression["rel_increase"], 2) == 0.5

    def test_no_regression_below_relative_threshold(self, durations_module):
        # +5% only — under the 25% relative threshold even though absolute is large
        regression = durations_module.detect_regression(
            latest_values=[6300],
            baseline_values=[6000, 6000, 6000, 6000, 6000],
            rel_threshold=0.25,
            min_abs_increase_seconds=300,
        )
        assert regression is None

    def test_no_regression_below_absolute_floor(self, durations_module):
        # +50% relative but only +60s absolute — under the 300s floor (noisy short job)
        regression = durations_module.detect_regression(
            latest_values=[180],
            baseline_values=[120, 120, 120, 120, 120],
            rel_threshold=0.25,
            min_abs_increase_seconds=300,
        )
        assert regression is None

    def test_robust_to_single_baseline_outlier(self, durations_module):
        # One slow baseline run should not move the median enough to mask a real regression.
        regression = durations_module.detect_regression(
            latest_values=[2700],
            baseline_values=[1800, 1800, 1800, 1800, 5000],
            rel_threshold=0.25,
            min_abs_increase_seconds=300,
        )
        assert regression is not None

    def test_empty_inputs(self, durations_module):
        assert durations_module.detect_regression([], [1, 2], 0.25, 300) is None
        assert durations_module.detect_regression([1], [], 0.25, 300) is None


class TestGetRecentRuns:
    def _runs_payload(self):
        return json.dumps(
            {
                "workflow_runs": [
                    {
                        "id": 2,
                        "run_number": 102,
                        "conclusion": "success",
                        "event": "schedule",
                        "html_url": "https://example/2",
                        "run_started_at": "2026-06-10T13:00:00Z",
                        "updated_at": "2026-06-10T13:45:00Z",
                    },
                    {
                        "id": 1,
                        "run_number": 101,
                        "conclusion": "failure",
                        "event": "schedule",
                        "html_url": "https://example/1",
                        "run_started_at": "2026-06-09T13:00:00Z",
                        "updated_at": "2026-06-09T13:30:00Z",
                    },
                    {
                        # A cancelled/skipped run with a near-zero wall-clock must be dropped
                        # so it cannot drag the baseline down.
                        "id": 3,
                        "run_number": 103,
                        "conclusion": "success",
                        "event": "schedule",
                        "html_url": "https://example/3",
                        "run_started_at": "2026-06-10T14:00:00Z",
                        "updated_at": "2026-06-10T14:00:30Z",
                    },
                ]
            }
        )

    def test_filters_to_successful_and_computes_duration(self, durations_module):
        with patch.object(durations_module, "gh_api", return_value=self._runs_payload()):
            runs = durations_module.get_recent_runs(
                "apache/airflow", "ci-amd.yml", "main", max_runs=25, only_successful=True, event="schedule"
            )
        # Only the 45-minute successful run survives: the failure and the 30s run are dropped.
        assert len(runs) == 1
        assert runs[0]["id"] == 2
        assert runs[0]["duration"] == 45 * 60

    def test_includes_failures_when_not_filtering(self, durations_module):
        with patch.object(durations_module, "gh_api", return_value=self._runs_payload()):
            runs = durations_module.get_recent_runs(
                "apache/airflow", "ci-amd.yml", "main", max_runs=25, only_successful=False, event="schedule"
            )
        # The failure is kept, but the 30s run is still dropped as non-representative.
        assert len(runs) == 2
        assert {r["id"] for r in runs} == {1, 2}

    def test_passes_event_to_api(self, durations_module):
        with patch.object(durations_module, "gh_api", return_value=self._runs_payload()) as mock_api:
            durations_module.get_recent_runs(
                "apache/airflow", "ci-amd.yml", "main", max_runs=25, only_successful=True, event="schedule"
            )
        assert mock_api.call_args.kwargs.get("event") == "schedule"

    def test_omits_event_when_empty(self, durations_module):
        with patch.object(durations_module, "gh_api", return_value=self._runs_payload()) as mock_api:
            durations_module.get_recent_runs(
                "apache/airflow", "ci-amd.yml", "main", max_runs=25, only_successful=True, event=""
            )
        assert "event" not in mock_api.call_args.kwargs

    def test_empty_on_api_failure(self, durations_module):
        with patch.object(durations_module, "gh_api", return_value=None):
            runs = durations_module.get_recent_runs(
                "apache/airflow", "ci-amd.yml", "main", max_runs=25, only_successful=True, event="schedule"
            )
        assert runs == []


class TestGetRunJobs:
    def test_parses_successful_jobs(self, durations_module):
        payload = json.dumps(
            {
                "jobs": [
                    {
                        "name": "Tests",
                        "conclusion": "success",
                        "startedAt": "2026-06-10T13:00:00Z",
                        "completedAt": "2026-06-10T13:20:00Z",
                        "steps": [
                            {
                                "name": "Prepare breeze & CI image: 3.10",
                                "startedAt": "2026-06-10T13:00:00Z",
                                "completedAt": "2026-06-10T13:05:00Z",
                            }
                        ],
                    },
                    {
                        "name": "Skipped job",
                        "conclusion": "skipped",
                        "startedAt": "2026-06-10T13:00:00Z",
                        "completedAt": "2026-06-10T13:00:00Z",
                    },
                ]
            }
        )
        completed = subprocess.CompletedProcess(args=[], returncode=0, stdout=payload, stderr="")
        with patch.object(subprocess, "run", return_value=completed):
            jobs = durations_module.get_run_jobs("apache/airflow", 2)
        assert jobs == {"Tests": {"duration": 20 * 60, "prepare_breeze_duration": 5 * 60}}

    def test_omits_prepare_breeze_duration_when_step_missing(self, durations_module):
        payload = json.dumps(
            {
                "jobs": [
                    {
                        "name": "Tests",
                        "conclusion": "success",
                        "startedAt": "2026-06-10T13:00:00Z",
                        "completedAt": "2026-06-10T13:20:00Z",
                        "steps": [],
                    }
                ]
            }
        )
        completed = subprocess.CompletedProcess(args=[], returncode=0, stdout=payload, stderr="")
        with patch.object(subprocess, "run", return_value=completed):
            jobs = durations_module.get_run_jobs("apache/airflow", 2)
        assert jobs == {"Tests": {"duration": 20 * 60, "prepare_breeze_duration": None}}

    def test_keeps_longest_duplicate_job_name(self, durations_module):
        payload = json.dumps(
            {
                "jobs": [
                    {
                        "name": "Tests",
                        "conclusion": "success",
                        "startedAt": "2026-06-10T13:00:00Z",
                        "completedAt": "2026-06-10T13:10:00Z",
                        "steps": [],
                    },
                    {
                        "name": "Tests",
                        "conclusion": "success",
                        "startedAt": "2026-06-10T13:00:00Z",
                        "completedAt": "2026-06-10T13:20:00Z",
                        "steps": [
                            {
                                "name": "Prepare breeze & CI image: 3.10",
                                "startedAt": "2026-06-10T13:00:00Z",
                                "completedAt": "2026-06-10T13:05:00Z",
                            }
                        ],
                    },
                ]
            }
        )
        completed = subprocess.CompletedProcess(args=[], returncode=0, stdout=payload, stderr="")
        with patch.object(subprocess, "run", return_value=completed):
            jobs = durations_module.get_run_jobs("apache/airflow", 2)
        assert jobs == {"Tests": {"duration": 20 * 60, "prepare_breeze_duration": 5 * 60}}

    def test_empty_on_command_failure(self, durations_module):
        completed = subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr="boom")
        with patch.object(subprocess, "run", return_value=completed):
            assert durations_module.get_run_jobs("apache/airflow", 2) == {}


class TestWorkDuration:
    def test_subtracts_image_build(self, durations_module):
        assert (
            durations_module.calculate_work_duration({"duration": 1200, "prepare_breeze_duration": 300})
            == 900
        )

    def test_full_duration_when_no_image_build_step(self, durations_module):
        assert (
            durations_module.calculate_work_duration({"duration": 1200, "prepare_breeze_duration": None})
            == 1200
        )

    def test_never_negative(self, durations_module):
        assert (
            durations_module.calculate_work_duration({"duration": 100, "prepare_breeze_duration": 300}) == 0
        )


class TestRunImageBuildSeconds:
    def test_median_across_jobs(self, durations_module):
        jobs = {
            "a": {"duration": 0, "prepare_breeze_duration": 300},
            "b": {"duration": 0, "prepare_breeze_duration": 500},
            "c": {"duration": 0, "prepare_breeze_duration": 400},
        }
        assert durations_module.calculate_image_build_seconds(jobs) == 400

    def test_ignores_jobs_without_the_step(self, durations_module):
        jobs = {
            "a": {"duration": 0, "prepare_breeze_duration": None},
            "b": {"duration": 0, "prepare_breeze_duration": 500},
        }
        assert durations_module.calculate_image_build_seconds(jobs) == 500

    def test_none_when_no_job_recorded_the_step(self, durations_module):
        jobs = {"a": {"duration": 0, "prepare_breeze_duration": None}}
        assert durations_module.calculate_image_build_seconds(jobs) is None


class TestAnalyzeJobs:
    def test_reports_only_regressed_jobs_with_enough_baseline(self, durations_module):
        latest_runs = [{"id": 100}]
        baseline_runs = [{"id": i} for i in range(5)]
        # slow-job work time (image build excluded): latest 2400 vs baseline 1500 -> +60%.
        jobs_by_run_id = {
            100: {
                "slow-job": {"duration": 2700, "prepare_breeze_duration": 300},
                "stable-job": {"duration": 600, "prepare_breeze_duration": None},
                "new-job": {"duration": 999, "prepare_breeze_duration": 300},
            }
        }
        for i in range(5):
            jobs_by_run_id[i] = {
                "slow-job": {"duration": 1800, "prepare_breeze_duration": 300},
                "stable-job": {"duration": 590, "prepare_breeze_duration": None},
            }

        regressions = durations_module.analyze_jobs(
            jobs_by_run_id,
            latest_runs,
            baseline_runs,
            min_baseline_runs=5,
            rel_threshold=0.25,
            min_abs_increase_seconds=180,
        )
        names = [r["job"] for r in regressions]
        # slow-job regressed; stable-job did not; new-job lacks baseline samples
        assert names == ["slow-job"]
        # Job regressions no longer carry image-build detail; that is reported separately.
        assert "prepare_breeze" not in regressions[0]

    def test_image_build_spike_alone_does_not_flag_a_job(self, durations_module):
        """A job whose total ballooned only because the image build spiked is not flagged."""
        latest_runs = [{"id": 100}]
        baseline_runs = [{"id": i} for i in range(5)]
        # latest total 1500 = +150% vs baseline 600, but work time (300) is unchanged.
        jobs_by_run_id = {100: {"job": {"duration": 1500, "prepare_breeze_duration": 1200}}}
        for i in range(5):
            jobs_by_run_id[i] = {"job": {"duration": 600, "prepare_breeze_duration": 300}}

        regressions = durations_module.analyze_jobs(
            jobs_by_run_id,
            latest_runs,
            baseline_runs,
            min_baseline_runs=5,
            rel_threshold=0.25,
            min_abs_increase_seconds=60,
        )
        assert regressions == []


class TestDetectImageBuildRegression:
    @staticmethod
    def _runs_and_jobs(specs):
        """Build (runs, jobs_by_run_id) from newest-first (run_id, created_at, image_seconds)."""
        runs = []
        jobs_by_run_id = {}
        for run_id, created_at, image_seconds in specs:
            runs.append({"id": run_id, "created_at": created_at})
            jobs_by_run_id[run_id] = {
                "job": {"duration": image_seconds, "prepare_breeze_duration": image_seconds}
            }
        return runs, jobs_by_run_id

    def _call(self, durations_module, specs, persistence_days=2.0, min_abs=180.0):
        runs, jobs_by_run_id = self._runs_and_jobs(specs)
        return durations_module.detect_image_build_regression(
            runs,
            jobs_by_run_id,
            latest_runs_count=1,
            min_baseline_runs=3,
            rel_threshold=0.25,
            min_abs_increase_seconds=min_abs,
            persistence_days=persistence_days,
        )

    def test_flags_when_slow_for_more_than_persistence_window(self, durations_module):
        # Newest 4 runs (06-18..06-15, a 3-day span) elevated; older 4 at baseline.
        specs = [
            (18, "2026-06-18T03:00:00Z", 1200),
            (17, "2026-06-17T03:00:00Z", 1200),
            (16, "2026-06-16T03:00:00Z", 1200),
            (15, "2026-06-15T03:00:00Z", 1200),
            (14, "2026-06-14T03:00:00Z", 300),
            (13, "2026-06-13T03:00:00Z", 300),
            (12, "2026-06-12T03:00:00Z", 300),
            (11, "2026-06-11T03:00:00Z", 300),
        ]
        regression = self._call(durations_module, specs)
        assert regression is not None
        assert regression["baseline"] == 300
        assert regression["latest"] == 1200
        assert regression["elevated_runs"] == 4
        assert round(regression["span_days"]) == 3

    def test_ignores_one_off_spike(self, durations_module):
        # Only the newest run is slow -> span 0 days -> below the 2-day window.
        specs = [
            (18, "2026-06-18T03:00:00Z", 1200),
            (17, "2026-06-17T03:00:00Z", 300),
            (16, "2026-06-16T03:00:00Z", 300),
            (15, "2026-06-15T03:00:00Z", 300),
            (14, "2026-06-14T03:00:00Z", 300),
        ]
        assert self._call(durations_module, specs) is None

    def test_non_consecutive_elevation_does_not_count(self, durations_module):
        # A gap right after the newest run breaks the streak, so the span is 0.
        specs = [
            (18, "2026-06-18T03:00:00Z", 1200),
            (17, "2026-06-17T03:00:00Z", 300),
            (16, "2026-06-16T03:00:00Z", 1200),
            (15, "2026-06-15T03:00:00Z", 1200),
            (14, "2026-06-14T03:00:00Z", 300),
            (13, "2026-06-13T03:00:00Z", 300),
        ]
        assert self._call(durations_module, specs) is None

    def test_no_alert_when_current_run_not_elevated(self, durations_module):
        # Older runs were slow but the latest recovered -> no ongoing problem.
        specs = [
            (18, "2026-06-18T03:00:00Z", 300),
            (17, "2026-06-17T03:00:00Z", 1200),
            (16, "2026-06-16T03:00:00Z", 1200),
            (15, "2026-06-15T03:00:00Z", 1200),
            (14, "2026-06-14T03:00:00Z", 300),
            (13, "2026-06-13T03:00:00Z", 300),
        ]
        assert self._call(durations_module, specs) is None

    def test_respects_absolute_floor(self, durations_module):
        # Sustained but tiny elevation (300 -> 380) stays under the absolute floor.
        specs = [
            (18, "2026-06-18T03:00:00Z", 380),
            (17, "2026-06-17T03:00:00Z", 380),
            (16, "2026-06-16T03:00:00Z", 380),
            (15, "2026-06-15T03:00:00Z", 300),
            (14, "2026-06-14T03:00:00Z", 300),
            (13, "2026-06-13T03:00:00Z", 300),
        ]
        assert self._call(durations_module, specs, min_abs=180.0) is None

    def test_none_when_not_enough_runs_with_image_data(self, durations_module):
        specs = [
            (18, "2026-06-18T03:00:00Z", 1200),
            (17, "2026-06-17T03:00:00Z", 1200),
        ]
        assert self._call(durations_module, specs) is None


class TestFormatSlackMessage:
    def test_includes_channel_and_blocks(self, durations_module):
        msg = durations_module.format_slack_message(
            repo="apache/airflow",
            workflow="ci-amd.yml",
            branch="main",
            overall_regression={
                "latest": 2700,
                "baseline": 1800,
                "increase": 900,
                "rel_increase": 0.5,
            },
            job_regressions=[
                {"job": "Tests", "latest": 1500, "baseline": 1000, "increase": 500, "rel_increase": 0.5}
            ],
            image_build_regression=None,
            recent_runs=[{"run_number": 102, "html_url": "https://example/2", "duration": 2700}],
            rel_threshold=0.25,
            channel="internal-airflow-ci-cd",
        )
        assert msg["channel"] == "internal-airflow-ci-cd"
        assert any(b["type"] == "header" for b in msg["blocks"])
        text_blob = json.dumps(msg)
        assert "Tests" in text_blob
        assert "main" in msg["text"]

    def test_includes_image_build_section_when_present(self, durations_module):
        msg = durations_module.format_slack_message(
            repo="apache/airflow",
            workflow="ci-amd.yml",
            branch="main",
            overall_regression=None,
            job_regressions=[],
            image_build_regression={
                "latest": 1080,
                "baseline": 300,
                "increase": 780,
                "rel_increase": 2.6,
                "elevated_runs": 5,
                "span_days": 3.0,
            },
            recent_runs=[{"run_number": 102, "html_url": "https://example/2", "duration": 2700}],
            rel_threshold=0.25,
            channel="internal-airflow-ci-cd",
        )
        text_blob = json.dumps(msg).lower()
        # Present even though no job/overall regression: the image build alone triggers it.
        assert "image build slow for 3.0 days" in text_blob
        assert "18m 00s" in json.dumps(msg)  # 1080s latest
        assert "image build slow" in msg["text"].lower()

    def test_omits_image_build_section_when_none(self, durations_module):
        msg = durations_module.format_slack_message(
            repo="apache/airflow",
            workflow="ci-amd.yml",
            branch="main",
            overall_regression=None,
            job_regressions=[
                {"job": "Tests", "latest": 1500, "baseline": 1000, "increase": 500, "rel_increase": 0.5}
            ],
            image_build_regression=None,
            recent_runs=[{"run_number": 102, "html_url": "https://example/2", "duration": 2700}],
            rel_threshold=0.25,
            channel="internal-airflow-ci-cd",
        )
        assert "image build slow" not in json.dumps(msg).lower()
