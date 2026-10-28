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
# requires-python = ">=3.11"
# ///
"""
Measure the numbers behind ``dev/breeze/src/airflow_breeze/utils/ci_minutes_estimate.py``.

Downloads recent successful ``Tests (AMD)`` PR runs, their job timings and the selective-check outputs
their ``Build info`` job printed, then prints the medians and fits to copy into the estimate module.
Only runs on public runners with the default versions are counted, as the estimate models them.
Downloads are cached in ``--cache-dir``, so re-runs only fetch new runs.

Needs an authenticated ``gh`` CLI. Run from the repository root:

    uv run dev/calibrate_ci_minutes_estimates.py --since 2026-09-10
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
import subprocess
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

REPOSITORY = "apache/airflow"
WORKFLOW = "ci-amd.yml"
ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*m")
# What ci_minutes_estimate.py models: jobs on GitHub's public AMD runners, default versions only.
# Self-hosted (RunsOn) runs and all-versions runs take different times and would skew the medians.
PUBLIC_AMD_RUNNERS = '["ubuntu-22.04"]'

# Job-name patterns for each job group of ci_minutes_estimate.py; the first match wins.
JOB_GROUPS = [
    ("docs", r"^CI image checks / (Build documentation|Publish documentation)"),
    ("core", r"^(MySQL|Postgres|Sqlite|Non-DB) tests: core|^Low dep tests:core"),
    ("special", r"^Special tests"),
    ("mypy-providers", r"^MyPy providers checks"),
    ("providers", r"^Provider distributions tests|tests: providers|^Low dep tests: providers"),
    ("prod", r"^Build PROD images|^Additional PROD image tests"),
    ("kubernetes", r"^Kubernetes tests"),
    ("helm", r"^Helm tests"),
    ("kustomize", r"^Kustomize"),
    ("integration", r"^Integration and System Tests"),
    ("task-sdk", r"^Task SDK tests"),
    ("agent-framework", r"^Agent framework tests"),
    ("java-sdk", r"^Java SDK"),
    ("go-sdk", r"^Go SDK"),
    ("ts-sdk-docs", r"^TypeScript SDK docs"),
    ("ts-sdk", r"^TypeScript SDK"),
    ("airflow-ctl", r"^Airflow CTL"),
    ("base", r".*"),
]


def gh_api(path: str, *extra: str) -> str:
    for _ in range(3):
        result = subprocess.run(["gh", "api", *extra, path], capture_output=True, text=True, check=False)
        if result.returncode == 0:
            return result.stdout
    raise RuntimeError(f"gh api {path} failed: {result.stderr.strip()}")


def list_runs(since: str) -> list[int]:
    run_ids: list[int] = []
    for page in range(1, 11):
        payload = json.loads(
            gh_api(
                f"repos/{REPOSITORY}/actions/workflows/{WORKFLOW}/runs?event=pull_request"
                f"&status=success&per_page=100&page={page}&created=>={since}"
            )
        )
        run_ids.extend(run["id"] for run in payload["workflow_runs"])
        if len(payload["workflow_runs"]) < 100:
            break
    return run_ids


def fetch_run(run_id: int, cache_dir: Path) -> None:
    jobs_file = cache_dir / f"{run_id}.jobs.json"
    log_file = cache_dir / f"{run_id}.build-info.log"
    try:
        if not jobs_file.exists():
            jobs = json.loads(
                gh_api(f"repos/{REPOSITORY}/actions/runs/{run_id}/jobs?per_page=100", "--paginate", "--slurp")
            )
            jobs_file.write_text(json.dumps([job for page in jobs for job in page["jobs"]]))
        if not log_file.exists():
            build_info = next(job for job in json.loads(jobs_file.read_text()) if job["name"] == "Build info")
            log_file.write_text(
                gh_api(f"repos/{REPOSITORY}/actions/jobs/{build_info['id']}/logs", "--allow-escape-sequences")
            )
    except (RuntimeError, StopIteration) as error:
        print(f"skipping run {run_id}: {error}")


def is_incomplete_json(value: str) -> bool:
    if not value.startswith(("[", "{")):
        return False
    try:
        json.loads(value)
    except json.JSONDecodeError:
        return True
    return False


def read_outputs(log_file: Path) -> dict[str, str]:
    # Rich wraps long JSON values over several log lines; glue continuation lines onto the value only
    # until it parses, so diagnostics printed after it are not appended.
    outputs: dict[str, str] = {}
    key = None
    for line in log_file.read_text(errors="replace").splitlines():
        text = ANSI_ESCAPE.sub("", line).split(" ", 1)[-1]
        match = re.match(r"^([a-z0-9-]+) = (.*)$", text)
        if match:
            key = match.group(1)
            outputs[key] = match.group(2)
        elif key and text and is_incomplete_json(outputs[key]):
            outputs[key] += text
        else:
            key = None
    return outputs


def get_test_types(outputs: dict[str, str], name: str) -> list[str]:
    value = outputs.get(name)
    if not value or value in ("null", "[]"):
        return []
    return sorted({test_type for group in json.loads(value) for test_type in group["test_types"].split()})


def count_provider_test_files(provider_id: str) -> int:
    tests_path = Path("providers", *provider_id.split("."), "tests")
    return sum(1 for _ in tests_path.rglob("test_*.py"))


def get_unmodelled_reason(outputs: dict[str, str]) -> str | None:
    """Why a run cannot calibrate the estimate, or None when it matches what the estimate models."""
    if outputs.get("runner-type") != PUBLIC_AMD_RUNNERS:
        return f"it ran on {outputs.get('runner-type', 'unknown runners')}"
    if outputs.get("all-versions") != "false":
        return "it ran all Python and database versions"
    return None


def build_rows(run_ids: list[int], cache_dir: Path) -> list[dict]:
    rows = []
    for run_id in run_ids:
        jobs_file = cache_dir / f"{run_id}.jobs.json"
        log_file = cache_dir / f"{run_id}.build-info.log"
        if not jobs_file.exists() or not log_file.exists():
            continue
        outputs = read_outputs(log_file)
        if "full-tests-needed" not in outputs:
            continue
        reason = get_unmodelled_reason(outputs)
        if reason is not None:
            print(f"skipping run {run_id}: {reason}")
            continue
        minutes: dict[str, float] = defaultdict(float)
        longest: dict[str, float] = defaultdict(float)
        for job in json.loads(jobs_file.read_text()):
            if job["conclusion"] == "skipped" or not job["started_at"] or not job["completed_at"]:
                continue
            duration = (
                datetime.fromisoformat(job["completed_at"]) - datetime.fromisoformat(job["started_at"])
            ).total_seconds() / 60
            group = next(group for group, pattern in JOB_GROUPS if re.search(pattern, job["name"]))
            minutes[group] += duration
            longest[group] = max(longest[group], duration)
        try:
            providers = get_test_types(outputs, "individual-providers-test-types-list-as-strings-in-json")
            core_types = len(get_test_types(outputs, "core-test-types-list-as-strings-in-json"))
        except json.JSONDecodeError:
            print(f"skipping run {run_id}: its selective-check outputs could not be parsed")
            continue
        rows.append(
            {
                "outputs": outputs,
                "minutes": minutes,
                "longest": longest,
                "core_types": core_types,
                "providers": [p[len("Providers[") : -1] for p in providers if p.startswith("Providers[")],
            }
        )
    return rows


def print_report(rows: list[dict]) -> None:
    print(f"\nruns analysed: {len(rows)}")
    print("\njob group          runs  median job-minutes  median longest job")
    for group, _ in JOB_GROUPS:
        present = [row for row in rows if row["minutes"].get(group)]
        if present:
            print(
                f"{group:18s}{len(present):5d}  {statistics.median(r['minutes'][group] for r in present):18.1f}"
                f"  {statistics.median(r['longest'][group] for r in present):18.1f}"
            )
    print("\ncore test types   runs  median job-minutes  median longest job")
    core = [row for row in rows if row["core_types"] and row["minutes"].get("core")]
    for count in sorted({row["core_types"] for row in core}):
        selected = [row for row in core if row["core_types"] == count]
        print(
            f"{count:15d}{len(selected):7d}  {statistics.median(r['minutes']['core'] for r in selected):18.1f}"
            f"  {statistics.median(r['longest']['core'] for r in selected):18.1f}"
        )
    tested = [row for row in rows if row["providers"] and row["minutes"].get("providers")]
    partial = [row for row in tested if len(row["providers"]) < 50]
    full = [row for row in tested if len(row["providers"]) >= 50]
    if full:
        print(
            f"\nall providers: {len(full)} runs, median job-minutes "
            f"{statistics.median(r['minutes']['providers'] for r in full):.1f}, median longest job "
            f"{statistics.median(r['longest']['providers'] for r in full):.1f}"
        )
    test_files = [sum(count_provider_test_files(p) for p in row["providers"]) for row in partial]
    if len(set(test_files)) < 2:
        print("partial provider runs: not enough different provider test-suite sizes to fit")
    else:
        slope, intercept = statistics.linear_regression(
            test_files, [r["minutes"]["providers"] for r in partial]
        )
        longest_slope, longest_intercept = statistics.linear_regression(
            test_files, [r["longest"]["providers"] for r in partial]
        )
        print(
            f"partial provider runs: {len(partial)} runs, "
            f"{len({tuple(row['providers']) for row in partial})} distinct provider sets"
        )
        print(f"  job-minutes  ~= {intercept:.1f} + {slope:.3f} * provider test files")
        print(f"  longest job  ~= {longest_intercept:.1f} + {longest_slope:.3f} * provider test files")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--since", required=True, help="Earliest run creation date (YYYY-MM-DD).")
    parser.add_argument("--cache-dir", type=Path, default=Path("files/ci-minutes-cache"))
    args = parser.parse_args()
    args.cache_dir.mkdir(parents=True, exist_ok=True)
    run_ids = list_runs(args.since)
    print(f"runs listed: {len(run_ids)}")
    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(lambda run_id: fetch_run(run_id, args.cache_dir), run_ids))
    print_report(build_rows(run_ids, args.cache_dir))


if __name__ == "__main__":
    main()
