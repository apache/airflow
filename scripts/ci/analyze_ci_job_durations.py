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
# /// script
# requires-python = ">=3.10"
# ///
"""
Watch CI job durations on ``main`` and warn when they creep above the recent trend.

Motivation: CI run times can drift upwards slowly (new tests, slower runners,
queue pressure) and nobody notices until a job starts timing out. This script
fetches the recent completed runs of the main CI workflow, computes the wall-clock
duration of each run and of each individual job, and compares the latest run(s)
against a robust baseline built from the preceding runs. When the latest duration
is meaningfully above that baseline it emits a warning so regressions are caught
early rather than at the timeout cliff.

The baseline is the *median* of the preceding window (robust to the odd slow run),
and a regression is only flagged when the latest median exceeds the baseline by
both a relative margin (``REL_THRESHOLD``) and an absolute floor
(``MIN_ABS_INCREASE_MINUTES`` / ``JOB_MIN_ABS_INCREASE_MINUTES``) so short jobs
with noisy timings do not trigger spurious alerts.

Environment variables (required):
  GITHUB_REPOSITORY  - Owner/repo (e.g. apache/airflow)
  GITHUB_TOKEN       - GitHub token for API access (used by ``gh``)

Environment variables (optional):
  WORKFLOW_NAME             - Workflow file to query (default: ci-amd.yml)
  BRANCH                    - Branch to filter runs (default: main)
  EVENT                     - Trigger event to restrict runs to, e.g. "schedule"; the main
                              post-merge canaries. Empty string = all events (default: schedule)
  MAX_RUNS                  - Window of completed runs to analyze (default: 25)
  LATEST_RUNS               - Most-recent runs compared against the baseline (default: 1)
  MIN_BASELINE_RUNS         - Minimum baseline runs needed to compute a trend (default: 5)
  REL_THRESHOLD             - Relative increase over baseline to flag, e.g. 0.25 = 25% (default: 0.25)
  MIN_ABS_INCREASE_MINUTES  - Absolute floor for the overall-run alert (default: 5)
  JOB_MIN_ABS_INCREASE_MINUTES - Absolute floor for per-job alerts (default: 3)
  ANALYZE_JOBS              - Whether to fetch per-job durations ("true"/"false", default: true)
  ONLY_SUCCESSFUL           - Only consider runs that concluded "success" (default: true)
  SLACK_CHANNEL             - Slack channel for the message payload (default: internal-airflow-ci-cd)
  OUTPUT_FILE               - Path for the Slack message output (default: slack-message.json)
  GITHUB_OUTPUT             - Path to GitHub Actions output file
  GITHUB_STEP_SUMMARY       - Path to GitHub Actions step summary file
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import TypedDict

ISO_SUFFIX_Z = "Z"
PREPARE_BREEZE_STEP_PREFIX = "Prepare breeze & CI image"


class JobDuration(TypedDict):
    duration: float
    prepare_breeze_duration: float | None


def env_float(name: str, default: float) -> float:
    """Read a float environment variable, falling back to ``default`` when unset/invalid."""
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError:
        print(f"Invalid float for {name}={raw!r}; using default {default}", file=sys.stderr)
        return default


def env_int(name: str, default: int) -> int:
    """Read an int environment variable, falling back to ``default`` when unset/invalid."""
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        print(f"Invalid int for {name}={raw!r}; using default {default}", file=sys.stderr)
        return default


def env_bool(name: str, default: bool) -> bool:
    """Read a boolean environment variable ("true"/"false")."""
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def escape_slack_mrkdwn(text: str) -> str:
    """Escape special characters for Slack mrkdwn format."""
    text = text.replace("&", "&amp;")
    text = text.replace("<", "&lt;")
    text = text.replace(">", "&gt;")
    return text


def gh_api(endpoint: str, **kwargs: str) -> str | None:
    """Call GitHub API via gh CLI.

    Forces ``--method GET``: ``gh api`` defaults to POST whenever ``-f``
    parameters are present, which makes read-only endpoints (such as the
    workflow runs list) return 404.
    """
    cmd = ["gh", "api", "--method", "GET", endpoint]
    for key, value in kwargs.items():
        cmd.extend(["-f", f"{key}={value}"])
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        print(f"gh api error for {endpoint}: {result.stderr}", file=sys.stderr)
        return None
    return result.stdout.strip()


def parse_iso(timestamp: str | None) -> datetime | None:
    """Parse an ISO-8601 timestamp (with trailing ``Z``) to an aware datetime."""
    if not timestamp:
        return None
    try:
        # ``datetime.fromisoformat`` only learned to parse the ``Z`` suffix in 3.11.
        if timestamp.endswith(ISO_SUFFIX_Z):
            timestamp = timestamp[:-1] + "+00:00"
        return datetime.fromisoformat(timestamp)
    except ValueError:
        return None


def duration_seconds(start: str | None, end: str | None) -> float | None:
    """Return the number of seconds between two ISO timestamps, or None if unparsable."""
    start_dt = parse_iso(start)
    end_dt = parse_iso(end)
    if start_dt is None or end_dt is None:
        return None
    seconds = (end_dt - start_dt).total_seconds()
    if seconds < 0:
        return None
    return seconds


def median(values: list[float]) -> float:
    """Return the median of a non-empty list of values."""
    ordered = sorted(values)
    n = len(ordered)
    mid = n // 2
    if n % 2 == 1:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2


def format_duration(seconds: float) -> str:
    """Format a duration in seconds as e.g. ``29m 41s``."""
    total = int(round(seconds))
    minutes, secs = divmod(total, 60)
    if minutes == 0:
        return f"{secs}s"
    return f"{minutes}m {secs:02d}s"


def format_duration_delta(seconds: float) -> str:
    """Format a duration delta with an explicit sign."""
    if seconds < 0:
        return f"-{format_duration(abs(seconds))}"
    return f"+{format_duration(seconds)}"


def format_prepare_breeze_timing(regression: dict) -> str | None:
    """Format prepare breeze timing details for a job regression."""
    if prepare_breeze := regression.get("prepare_breeze"):
        return (
            f"{PREPARE_BREEZE_STEP_PREFIX}: "
            f"{format_duration(prepare_breeze['baseline'])} → "
            f"{format_duration(prepare_breeze['latest'])} "
            f"({format_duration_delta(prepare_breeze['increase'])})"
        )
    return None


# Wall-clock shorter than this almost always means a run that was cancelled,
# skipped by selective checks, or never really executed the test matrix — not a
# representative "main build". Such runs would corrupt the duration baseline.
MIN_VALID_RUN_SECONDS = 120


def get_recent_runs(
    repo: str, workflow: str, branch: str, max_runs: int, only_successful: bool, event: str
) -> list[dict]:
    """Get recent completed workflow runs (newest first) with timing metadata.

    ``event`` (e.g. ``schedule``) restricts the result to a single trigger type so
    that PR runs targeting ``main`` are not mixed in with the post-merge canary
    runs. Pass an empty string to include every event.
    """
    # Over-fetch when filtering to success so we still end up with ~max_runs usable runs.
    per_page = max_runs * 2 if only_successful else max_runs
    per_page = min(per_page, 100)
    params = {
        "branch": branch,
        "per_page": str(per_page),
        "status": "completed",
    }
    if event:
        params["event"] = event
    output = gh_api(f"repos/{repo}/actions/workflows/{workflow}/runs", **params)
    if not output:
        return []
    try:
        data = json.loads(output)
    except json.JSONDecodeError:
        return []

    runs: list[dict] = []
    for run in data.get("workflow_runs", []):
        if only_successful and run.get("conclusion") != "success":
            continue
        seconds = duration_seconds(run.get("run_started_at"), run.get("updated_at"))
        if seconds is None or seconds < MIN_VALID_RUN_SECONDS:
            continue
        runs.append(
            {
                "id": run["id"],
                "run_number": run.get("run_number", "?"),
                "created_at": run.get("created_at", ""),
                "conclusion": run.get("conclusion", "unknown"),
                "event": run.get("event", "unknown"),
                "html_url": run.get("html_url", ""),
                "duration": seconds,
            }
        )
        if len(runs) >= max_runs:
            break
    return runs


def get_prepare_breeze_step_duration(job: dict) -> float | None:
    """Return the prepare breeze step duration for a job, when the step exists."""
    for step in job.get("steps", []):
        name = step.get("name", "")
        if not name.startswith(PREPARE_BREEZE_STEP_PREFIX):
            continue
        return duration_seconds(step.get("startedAt"), step.get("completedAt"))
    return None


def get_run_jobs(repo: str, run_id: int) -> dict[str, JobDuration]:
    """Return a mapping of job name -> duration details for a single run.

    Only jobs that completed successfully are included, so that a job which was
    cancelled or skipped on a particular run does not pollute its duration trend.
    """
    result = subprocess.run(
        ["gh", "run", "view", str(run_id), "--repo", repo, "--json", "jobs"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        print(f"Could not fetch jobs for run {run_id}: {result.stderr}", file=sys.stderr)
        return {}
    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        return {}

    durations: dict[str, JobDuration] = {}
    for job in data.get("jobs", []):
        if job.get("conclusion") != "success":
            continue
        seconds = duration_seconds(job.get("startedAt"), job.get("completedAt"))
        if seconds is None:
            continue
        name = job.get("name", "unknown")
        # A matrix can surface the same job name more than once per run; keep the longest.
        existing = durations.get(name)
        if existing is None or seconds > existing["duration"]:
            durations[name] = {
                "duration": seconds,
                "prepare_breeze_duration": get_prepare_breeze_step_duration(job),
            }
    return durations


def detect_regression(
    latest_values: list[float],
    baseline_values: list[float],
    rel_threshold: float,
    min_abs_increase_seconds: float,
) -> dict | None:
    """Compare latest durations against a baseline window.

    Returns a dict describing the regression when the latest median is above the
    baseline median by both the relative threshold and the absolute floor, else None.
    """
    if not latest_values or not baseline_values:
        return None
    latest = median(latest_values)
    baseline = median(baseline_values)
    increase = latest - baseline
    rel_increase = increase / baseline if baseline > 0 else 0.0
    if increase >= min_abs_increase_seconds and rel_increase >= rel_threshold:
        return {
            "latest": latest,
            "baseline": baseline,
            "increase": increase,
            "rel_increase": rel_increase,
        }
    return None


def analyze_jobs(
    repo: str,
    latest_runs: list[dict],
    baseline_runs: list[dict],
    min_baseline_runs: int,
    rel_threshold: float,
    min_abs_increase_seconds: float,
) -> list[dict]:
    """Fetch per-job durations and return the jobs whose latest duration regressed."""
    latest_job_durations: dict[str, list[float]] = {}
    latest_prepare_breeze_durations: dict[str, list[float]] = {}
    for run in latest_runs:
        for name, job_data in get_run_jobs(repo, run["id"]).items():
            latest_job_durations.setdefault(name, []).append(job_data["duration"])
            prepare_breeze_duration = job_data["prepare_breeze_duration"]
            if prepare_breeze_duration is not None:
                latest_prepare_breeze_durations.setdefault(name, []).append(prepare_breeze_duration)

    baseline_job_durations: dict[str, list[float]] = {}
    baseline_prepare_breeze_durations: dict[str, list[float]] = {}
    for run in baseline_runs:
        for name, job_data in get_run_jobs(repo, run["id"]).items():
            baseline_job_durations.setdefault(name, []).append(job_data["duration"])
            prepare_breeze_duration = job_data["prepare_breeze_duration"]
            if prepare_breeze_duration is not None:
                baseline_prepare_breeze_durations.setdefault(name, []).append(prepare_breeze_duration)

    regressions: list[dict] = []
    for name, latest_values in latest_job_durations.items():
        baseline_values = baseline_job_durations.get(name, [])
        if len(baseline_values) < min_baseline_runs:
            continue
        regression = detect_regression(
            latest_values, baseline_values, rel_threshold, min_abs_increase_seconds
        )
        if regression:
            latest_prepare_breeze_values = latest_prepare_breeze_durations.get(name, [])
            baseline_prepare_breeze_values = baseline_prepare_breeze_durations.get(name, [])
            if latest_prepare_breeze_values and len(baseline_prepare_breeze_values) >= min_baseline_runs:
                latest_prepare_breeze = median(latest_prepare_breeze_values)
                baseline_prepare_breeze = median(baseline_prepare_breeze_values)
                regression["prepare_breeze"] = {
                    "latest": latest_prepare_breeze,
                    "baseline": baseline_prepare_breeze,
                    "increase": latest_prepare_breeze - baseline_prepare_breeze,
                }
            regression["job"] = name
            regressions.append(regression)

    regressions.sort(key=lambda r: r["rel_increase"], reverse=True)
    return regressions


def format_slack_message(
    repo: str,
    workflow: str,
    branch: str,
    overall_regression: dict | None,
    job_regressions: list[dict],
    recent_runs: list[dict],
    rel_threshold: float,
    channel: str,
) -> dict:
    """Format the regression report as a Slack Block Kit message."""
    blocks: list[dict] = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "⏱️ CI Duration Trend Alert"},
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"CI run times on *{escape_slack_mrkdwn(branch)}* "
                    f"(`{escape_slack_mrkdwn(workflow)}`) have risen above the recent trend "
                    f"(baseline = median of the preceding runs; threshold = "
                    f"+{int(rel_threshold * 100)}%)."
                ),
            },
        },
        {"type": "divider"},
    ]

    if overall_regression:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        "*Overall run wall-clock regressed:*\n"
                        f"• Latest: *{format_duration(overall_regression['latest'])}*\n"
                        f"• Baseline: {format_duration(overall_regression['baseline'])}\n"
                        f"• Increase: *+{format_duration(overall_regression['increase'])}* "
                        f"(+{round(overall_regression['rel_increase'] * 100, 1)}%)"
                    ),
                },
            }
        )

    if job_regressions:
        lines = ["*Jobs that got slower:*"]
        for reg in job_regressions[:15]:
            line = (
                f"• *{escape_slack_mrkdwn(reg['job'])}* — "
                f"{format_duration(reg['baseline'])} → *{format_duration(reg['latest'])}* "
                f"(+{round(reg['rel_increase'] * 100, 1)}%)"
            )
            if prepare_breeze_timing := format_prepare_breeze_timing(reg):
                line += f"\n  {escape_slack_mrkdwn(prepare_breeze_timing)}"
            lines.append(line)
        text = "\n".join(lines)
        if len(text) > 2900:
            text = text[:2900] + "\n_...truncated_"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})
        if len(job_regressions) > 15:
            blocks.append(
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f"_...and {len(job_regressions) - 15} more slower jobs_",
                        }
                    ],
                }
            )

    if recent_runs:
        latest_run = recent_runs[0]
        blocks.append({"type": "divider"})
        blocks.append(
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": (
                            f"<https://github.com/{repo}/actions/workflows/{workflow}"
                            f"?query=branch%3A{branch}|View {escape_slack_mrkdwn(workflow)} runs> | "
                            f"Latest: <{latest_run['html_url']}|Run #{latest_run['run_number']}> "
                            f"({format_duration(latest_run['duration'])})"
                        ),
                    }
                ],
            }
        )

    fallback_parts = []
    if overall_regression:
        fallback_parts.append(f"overall +{round(overall_regression['rel_increase'] * 100, 1)}%")
    if job_regressions:
        fallback_parts.append(f"{len(job_regressions)} slower job(s)")
    fallback = f"CI Duration Trend Alert on {branch}: " + ", ".join(fallback_parts)

    return {
        "channel": channel,
        "text": fallback,
        "blocks": blocks,
    }


def write_step_summary(
    workflow: str,
    branch: str,
    overall_regression: dict | None,
    job_regressions: list[dict],
    recent_runs: list[dict],
    baseline_count: int,
) -> None:
    """Write a GitHub Actions step summary in markdown."""
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return

    lines = [
        "## ⏱️ CI Duration Trend",
        "",
        f"Workflow `{workflow}` on `{branch}` — baseline from {baseline_count} preceding runs.",
        "",
    ]

    if overall_regression:
        lines += [
            "### ⚠️ Overall run regressed",
            "",
            f"- Latest: **{format_duration(overall_regression['latest'])}**",
            f"- Baseline: {format_duration(overall_regression['baseline'])}",
            f"- Increase: **+{format_duration(overall_regression['increase'])}** "
            f"(+{round(overall_regression['rel_increase'] * 100, 1)}%)",
            "",
        ]
    else:
        lines += ["### ✅ Overall run within trend", ""]

    if job_regressions:
        lines += [
            "### ⚠️ Slower jobs",
            "",
            "| Job | Baseline | Latest | Increase |",
            "|-----|----------|--------|----------|",
        ]
        for reg in job_regressions[:25]:
            job = reg["job"]
            if prepare_breeze_timing := format_prepare_breeze_timing(reg):
                job += f"<br>{prepare_breeze_timing}"
            lines.append(
                f"| {job} | {format_duration(reg['baseline'])} | "
                f"{format_duration(reg['latest'])} | +{round(reg['rel_increase'] * 100, 1)}% |"
            )
        lines.append("")
    else:
        lines += ["### ✅ No individual job regressed", ""]

    if recent_runs:
        lines += [
            "### Recent run durations",
            "",
            "| Run | Event | Duration |",
            "|-----|-------|----------|",
        ]
        for run in recent_runs[:15]:
            lines.append(
                f"| [#{run['run_number']}]({run['html_url']}) | {run['event']} | "
                f"{format_duration(run['duration'])} |"
            )
        lines.append("")

    with open(summary_path, "a") as f:
        f.write("\n".join(lines))


def main() -> None:
    repo = os.environ.get("GITHUB_REPOSITORY", "apache/airflow")
    workflow = os.environ.get("WORKFLOW_NAME", "ci-amd.yml")
    branch = os.environ.get("BRANCH", "main")
    event = os.environ.get("EVENT", "schedule")
    max_runs = env_int("MAX_RUNS", 25)
    latest_runs_count = env_int("LATEST_RUNS", 1)
    min_baseline_runs = env_int("MIN_BASELINE_RUNS", 5)
    rel_threshold = env_float("REL_THRESHOLD", 0.25)
    min_abs_increase_seconds = env_float("MIN_ABS_INCREASE_MINUTES", 5.0) * 60
    job_min_abs_increase_seconds = env_float("JOB_MIN_ABS_INCREASE_MINUTES", 3.0) * 60
    do_analyze_jobs = env_bool("ANALYZE_JOBS", True)
    only_successful = env_bool("ONLY_SUCCESSFUL", True)
    channel = os.environ.get("SLACK_CHANNEL", "internal-airflow-ci-cd")
    output_file = Path(os.environ.get("OUTPUT_FILE", "slack-message.json"))

    event_label = event or "all events"
    print(f"Analyzing CI durations for {repo} ({workflow} on {branch}, event={event_label})")
    print(f"Window: up to {max_runs} completed runs; latest {latest_runs_count} vs baseline.")

    runs = get_recent_runs(repo, workflow, branch, max_runs, only_successful, event)
    if len(runs) < latest_runs_count + min_baseline_runs:
        print(
            f"Not enough runs to establish a trend "
            f"(found {len(runs)}, need {latest_runs_count + min_baseline_runs}). Skipping."
        )
        _write_outputs(False, False, 0)
        sys.exit(0)

    latest_runs = runs[:latest_runs_count]
    baseline_runs = runs[latest_runs_count:]
    print(f"Latest runs: {len(latest_runs)}; baseline runs: {len(baseline_runs)}.")

    overall_regression = detect_regression(
        [r["duration"] for r in latest_runs],
        [r["duration"] for r in baseline_runs],
        rel_threshold,
        min_abs_increase_seconds,
    )
    if overall_regression:
        print(
            f"Overall regression: {format_duration(overall_regression['baseline'])} -> "
            f"{format_duration(overall_regression['latest'])} "
            f"(+{round(overall_regression['rel_increase'] * 100, 1)}%)"
        )
    else:
        print("Overall run duration is within the recent trend.")

    job_regressions: list[dict] = []
    if do_analyze_jobs:
        job_regressions = analyze_jobs(
            repo,
            latest_runs,
            baseline_runs,
            min_baseline_runs,
            rel_threshold,
            job_min_abs_increase_seconds,
        )
        print(f"Jobs that regressed: {len(job_regressions)}")

    has_regression = bool(overall_regression) or bool(job_regressions)

    if has_regression:
        slack_message = format_slack_message(
            repo, workflow, branch, overall_regression, job_regressions, runs, rel_threshold, channel
        )
        output_file.write_text(json.dumps(slack_message, indent=2))
        print(f"Slack message written to: {output_file}")
    else:
        print("No regression detected; no Slack message written.")

    write_step_summary(workflow, branch, overall_regression, job_regressions, runs, len(baseline_runs))
    _write_outputs(has_regression, bool(overall_regression), len(job_regressions))


def _write_outputs(has_regression: bool, overall_regression: bool, regressed_jobs: int) -> None:
    """Write GitHub Actions outputs used to gate the Slack-notify step."""
    github_output = os.environ.get("GITHUB_OUTPUT")
    if not github_output:
        return
    with open(github_output, "a") as f:
        f.write(f"has-regression={str(has_regression).lower()}\n")
        f.write(f"overall-regression={str(overall_regression).lower()}\n")
        f.write(f"regressed-jobs={regressed_jobs}\n")


if __name__ == "__main__":
    main()
