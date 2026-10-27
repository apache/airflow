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
Tell failures caused by EC2 Spot interruptions of RunsOn runners apart from real ones.

RunsOn fails a job whose Spot instance AWS reclaimed, marks it with an annotation titled
"EC2 Spot interruption", and re-runs the failed jobs of the run once it finishes, for attempts
1 and 2. Those failures are not worth anyone's attention until RunsOn gives up.

Modes:

  slack       For the Slack notification of a scheduled run. Writes to GITHUB_OUTPUT:
                failed-jobs   newline-separated failed jobs, spot ones marked as such
                failed-jobs-text  the same, with escaped newlines, for a Slack payload
                has-failures  "true" when any job failed
                skip          "true" when nothing is posted and no state is recorded: for a
                              re-run not started by RunsOn, or when every failure is a spot
                              interruption that RunsOn will re-run
              Environment: GITHUB_REPOSITORY, RUN_ID, RUN_ATTEMPT, TRIGGERING_ACTOR

  pr-comments For the newest CI run of each pull request, in progress or recently completed,
              keeps one comment on the pull request: which jobs were stopped by a spot
              interruption and whether RunsOn re-runs them, and, once a re-run finished, how it
              ended. Run on a schedule, so a pull request run never gets write access, and a
              notice appears as soon as a job is interrupted.
              Environment: GITHUB_REPOSITORY
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from urllib.parse import quote

SPOT_ANNOTATION_TITLE = "EC2 Spot interruption"
# The annotation is trusted only on a RunsOn runner. A pull request's run executes the pull
# request's workflow file, which could add an annotation with that title, but runs of external
# contributors use GitHub-hosted runners.
RUNS_ON_RUNNER_PREFIX = "runs-on--"
# RunsOn re-runs the failed jobs of attempts 1 and 2; attempt 3 is its last.
LAST_RETRIED_ATTEMPT = 2
# Fails whenever any other job failed, so it is no failure of its own when the others were spot.
CI_PASSED_JOB = "CI passed"
# Starts the notice; it records the run it is about, so another run never reports into it.
COMMENT_MARKER = "<!-- spot-interruption-notice"
NOTICE_RUN = re.compile(r"^<!-- spot-interruption-notice run=(\d+) -->")
# The GitHub App RunsOn re-runs the failed jobs of an interrupted run as.
RUNS_ON_BOT = "runsonairflow[bot]"
CI_WORKFLOW = "ci-amd.yml"
# A completed run is looked at while it changed recently: RunsOn's re-run may finish long after the
# run was created, so the creation window is wide and the update window is what bounds the work.
CREATED_WINDOW = timedelta(days=7)
UPDATED_WINDOW = timedelta(hours=2)
# The author of the notice; a contributor's comment starting with the marker is not the notice.
COMMENT_AUTHOR = "github-actions[bot]"
# A job cancelled while its run was not, e.g. by its timeout, is a failure too.
FAILED_CONCLUSIONS = ("failure", "cancelled", "timed_out")
# GitHub answers with a server error while overloaded, and the same request often succeeds later.
TRANSIENT_ERROR = re.compile(r"\(HTTP 5\d\d\)")
GH_API_ATTEMPTS = 3
GH_API_RETRY_DELAY_SECONDS = 5


@dataclass(frozen=True)
class FailedJob:
    id: int
    name: str
    url: str
    spot: bool
    conclusion: str = "failure"


def is_transient(error: subprocess.CalledProcessError) -> bool:
    return bool(TRANSIENT_ERROR.search(error.stderr or ""))


def gh_api(*args: str) -> str:
    command = ["gh", "api", *args]
    # A POST that failed with a server error may still have created the comment.
    attempts = 1 if "POST" in args else GH_API_ATTEMPTS
    for attempt in range(1, attempts):
        try:
            return subprocess.run(command, check=True, capture_output=True, text=True).stdout
        except subprocess.CalledProcessError as e:
            if not is_transient(e):
                raise
            time.sleep(GH_API_RETRY_DELAY_SECONDS * attempt)
    return subprocess.run(command, check=True, capture_output=True, text=True).stdout


def is_spot_interrupted(repo: str, job_id: int) -> bool:
    titles = gh_api(
        "--paginate", f"repos/{repo}/check-runs/{job_id}/annotations?per_page=100", "--jq", ".[].title"
    )
    return SPOT_ANNOTATION_TITLE in titles.splitlines()


def get_failed_jobs(repo: str, run_id: str, attempt: int | None = None) -> list[FailedJob]:
    """Return the failed jobs of an attempt of the run, by default each job's latest attempt."""
    jobs_path = f"attempts/{attempt}/jobs?" if attempt else "jobs?filter=latest&"
    # Pages of 100 jobs of a full CI run time out on GitHub's side (HTTP 502).
    output = gh_api(
        "--paginate",
        f"repos/{repo}/actions/runs/{run_id}/{jobs_path}per_page=50",
        "--jq",
        ".jobs[] | {id, name, html_url, conclusion, runner_name}",
    )
    jobs = [json.loads(line) for line in output.splitlines() if line.strip()]
    jobs = [job for job in jobs if job["conclusion"] in FAILED_CONCLUSIONS]
    return [
        FailedJob(
            job["id"],
            job["name"],
            job["html_url"],
            # RunsOn fails an interrupted job; one that was cancelled was not interrupted.
            job["conclusion"] == "failure"
            and (job.get("runner_name") or "").startswith(RUNS_ON_RUNNER_PREFIX)
            and is_spot_interrupted(repo, job["id"]),
            job["conclusion"],
        )
        for job in jobs
    ]


def find_real_failures(jobs: list[FailedJob]) -> list[FailedJob]:
    real = [job for job in jobs if not job.spot]
    if any(job.spot for job in jobs):
        real = [job for job in real if not job.name.startswith(CI_PASSED_JOB)]
    return real


def should_skip_slack(jobs: list[FailedJob], attempt: int, triggering_actor: str) -> bool:
    # A maintainer's re-run is not reported; RunsOn's re-run is, as the attempt before it was not.
    if attempt > 1 and triggering_actor != RUNS_ON_BOT:
        return True
    spot_only = any(job.spot for job in jobs) and not find_real_failures(jobs)
    return spot_only and attempt <= LAST_RETRIED_ATTEMPT


def format_failed_jobs(jobs: list[FailedJob]) -> str:
    def describe(job: FailedJob) -> str:
        if job.spot:
            return f"{job.name} (spot instance reclaimed)"
        if job.conclusion != "failure":
            return f"{job.name} ({job.conclusion.replace('_', ' ')})"
        return job.name

    return "\n".join(describe(job) for job in sorted(jobs, key=lambda job: job.name))


def format_job_name(name: str) -> str:
    """Make a job name safe for a code span: no backtick or line break to end it early."""
    return re.sub(r"\s+", " ", name.replace("`", "")).strip()


def build_comment(
    jobs: list[FailedJob], *, run_id: int, attempt: int, conclusion: str, run_url: str
) -> str | None:
    """Return the comment for this attempt, or None when it has nothing to say."""
    marker = f"{COMMENT_MARKER} run={run_id} -->"
    spot_jobs = sorted((job for job in jobs if job.spot), key=lambda job: job.name)
    if not spot_jobs:
        if attempt == 1:
            return None
        outcome = "passed" if conclusion == "success" else "failed"
        return (
            f"{marker}\n"
            f"The re-run of the jobs stopped by EC2 Spot interruptions finished: [attempt {attempt}]"
            f"({run_url}/attempts/{attempt}) {outcome}."
        )
    # Job names come from the pull request's workflow file: in a code span they cannot render
    # markdown or mention anyone.
    job_list = "\n".join(f"- [`{format_job_name(job.name)}`]({job.url})" for job in spot_jobs)
    if attempt <= LAST_RETRIED_ATTEMPT:
        what_next = "RunsOn re-runs them automatically when the run finishes; no action is needed."
    else:
        what_next = (
            "RunsOn does not re-run them again after this attempt. A maintainer can re-run the failed jobs."
        )
    return (
        f"{marker}\n"
        f"⚠️ {len(spot_jobs)} job(s) of [attempt {attempt}]({run_url}/attempts/{attempt}) were stopped "
        "because AWS reclaimed their EC2 Spot instances. These are not test failures.\n\n"
        f"{job_list}\n\n{what_next}"
    )


def find_pull_requests(repo: str, run: dict) -> list[int]:
    """Return the open pull requests the run tested."""
    if run["pr_numbers"]:
        return run["pr_numbers"]
    # GitHub does not attach pull requests from forks to their runs. Several pull requests of one
    # branch, against different base branches, are told apart by the commit the run tested; when
    # they share it, the run's pull request is unknown and nothing is posted.
    output = gh_api(
        "-X",
        "GET",
        f"repos/{repo}/pulls",
        "-f",
        f"head={run['head_owner']}:{run['head_branch']}",
        "-f",
        "state=open",
        "--jq",
        ".[] | {number, sha: .head.sha}",
    )
    pulls = [json.loads(line) for line in output.splitlines() if line.strip()]
    matching = [pull["number"] for pull in pulls if pull["sha"] == run["head_sha"]]
    return matching if len(matching) == 1 else []


@dataclass(frozen=True)
class Comment:
    id: int
    body: str

    @property
    def run_id(self) -> int | None:
        match = NOTICE_RUN.match(self.body)
        return int(match.group(1)) if match else None


def find_comment(repo: str, pr_number: int) -> Comment | None:
    output = gh_api(
        "--paginate",
        f"repos/{repo}/issues/{pr_number}/comments?per_page=100",
        "--jq",
        ".[] | {id, body, author: .user.login}",
    )
    comments = [json.loads(line) for line in output.splitlines() if line.strip()]
    notices = [
        Comment(comment["id"], comment["body"])
        for comment in comments
        if comment["author"] == COMMENT_AUTHOR and comment["body"].startswith(COMMENT_MARKER)
    ]
    return notices[-1] if notices else None


def write_comment(repo: str, pr_number: int, existing: Comment | None, body: str) -> None:
    if existing is not None:
        gh_api("-X", "PATCH", f"repos/{repo}/issues/comments/{existing.id}", "-f", f"body={body}")
    else:
        gh_api("-X", "POST", f"repos/{repo}/issues/{pr_number}/comments", "-f", f"body={body}")


def write_output(name: str, value: str) -> None:
    if output_file := os.environ.get("GITHUB_OUTPUT"):
        with open(output_file, "a") as f:
            f.write(f"{name}<<EOF\n{value}\nEOF\n")
    print(f"{name}={value}")


def run_slack(repo: str, run_id: str, attempt: int) -> None:
    jobs = get_failed_jobs(repo, run_id)
    failed_jobs = format_failed_jobs(jobs)
    write_output("failed-jobs", failed_jobs)
    # A literal newline would end the double-quoted YAML scalar the Slack payload interpolates
    # job names into, and the action would reject the whole payload.
    write_output("failed-jobs-text", failed_jobs.replace("\n", "\\n"))
    write_output("has-failures", "true" if jobs else "false")
    skip = should_skip_slack(jobs, attempt, os.environ.get("TRIGGERING_ACTOR", ""))
    write_output("skip", "true" if skip else "false")


# GitHub returns at most this many runs for a filtered listing, however many pages are read.
RUN_LISTING_LIMIT = 1000


def list_runs(repo: str, query: str) -> list[dict]:
    output = gh_api(
        "--paginate",
        f"repos/{repo}/actions/workflows/{CI_WORKFLOW}/runs?event=pull_request&per_page=100&{query}",
        "--jq",
        ".workflow_runs[] | {id, run_attempt, status, conclusion, html_url, head_branch, head_sha, "
        "created_at, updated_at, head_owner: .head_repository.owner.login, "
        "triggering_actor: .triggering_actor.login, pr_numbers: [.pull_requests[].number]}",
    )
    runs = [json.loads(line) for line in output.splitlines() if line.strip()]
    if len(runs) >= RUN_LISTING_LIMIT:
        print(f"Run listing {query} reached GitHub's limit; older runs are missed.", file=sys.stderr)
    return runs


def list_pull_request_runs(repo: str, now: datetime) -> list[dict]:
    """Return the newest CI run of each pull request, when it is in progress or changed recently."""
    updated_since = (now - UPDATED_WINDOW).strftime("%Y-%m-%dT%H:%M:%SZ")
    runs = list_runs(repo, "status=in_progress")
    # A day at a time, so that no listing reaches the limit.
    for day in range(CREATED_WINDOW.days):
        end = now - timedelta(days=day)
        start = end - timedelta(days=1)
        created = f"{start:%Y-%m-%dT%H:%M:%SZ}..{end:%Y-%m-%dT%H:%M:%SZ}"
        runs.extend(list_runs(repo, f"status=completed&created={quote(created)}"))
    # A run is identified by the pull requests GitHub attaches to it; runs of pull requests from
    # forks have none attached, and are told apart by their branch.
    newest: dict[tuple, dict] = {}
    for run in runs:
        keys: list[tuple[str, ...]] = [("pr", str(number)) for number in run["pr_numbers"]] or [
            ("branch", run["head_owner"], run["head_branch"])
        ]
        for key in keys:
            if key not in newest or run["created_at"] > newest[key]["created_at"]:
                newest[key] = run
    recent = {
        run["id"]: run
        for run in newest.values()
        if run["status"] != "completed" or run["updated_at"] >= updated_since
    }
    return list(recent.values())


def update_notice(repo: str, run: dict) -> None:
    """Post or update the notice for the pull request of a run, when there is anything to say."""
    run_id, attempt, completed = run["id"], run["run_attempt"], run["status"] == "completed"
    retried_by_runs_on = attempt > 1 and run.get("triggering_actor") == RUNS_ON_BOT
    jobs = get_failed_jobs(repo, str(run_id))
    has_spot = any(job.spot for job in jobs)
    # Without interruptions in this attempt, there is only the outcome of a finished re-run, or the
    # interruption RunsOn is re-running, to report.
    if not has_spot and attempt == 1:
        return
    if not has_spot and not completed and not retried_by_runs_on:
        return
    for pr_number in find_pull_requests(repo, run):
        update_notice_on(repo, pr_number, run, jobs)


def update_notice_on(repo: str, pr_number: int, run: dict, jobs: list[FailedJob]) -> None:
    run_id, attempt, completed = run["id"], run["run_attempt"], run["status"] == "completed"
    retried_by_runs_on = attempt > 1 and run.get("triggering_actor") == RUNS_ON_BOT
    has_spot = any(job.spot for job in jobs)
    existing = find_comment(repo, pr_number)
    notice_of_run = existing is not None and existing.run_id == run_id

    def build(failed: list[FailedJob], for_attempt: int) -> str | None:
        return build_comment(
            failed,
            run_id=run_id,
            attempt=for_attempt,
            conclusion=run["conclusion"] or "",
            run_url=run["html_url"],
        )

    if has_spot:
        body = build(jobs, attempt)
    elif notice_of_run:
        # The outcome of a re-run is reported only into the notice about that run.
        body = build(jobs, attempt) if completed else None
    elif retried_by_runs_on:
        # RunsOn started its re-run between two polls, so the interruption is in the attempt before.
        previous = get_failed_jobs(repo, str(run_id), attempt - 1)
        body = build(previous, attempt - 1) if any(job.spot for job in previous) else None
    else:
        body = None
    if body is None or (existing is not None and existing.body == body):
        return
    print(f"#{pr_number}: notice for run {run['html_url']} attempt {attempt}")
    write_comment(repo, pr_number, existing, body)


def describe_error(error: subprocess.CalledProcessError) -> str:
    return " ".join((error.stderr or str(error)).split())


def run_pr_comments(repo: str, now: datetime) -> bool:
    """
    Update the notices; return False when a run could not be handled for a lasting reason.

    Server errors that outlast the retries only skip the work they hit: the next scheduled run does
    it again.
    """
    try:
        runs = list_pull_request_runs(repo, now)
    except subprocess.CalledProcessError as e:
        if not is_transient(e):
            raise
        print(f"::warning::Could not list the CI runs, GitHub keeps failing: {describe_error(e)}")
        return True
    ok = True
    for run in runs:
        try:
            update_notice(repo, run)
        except subprocess.CalledProcessError as e:
            if is_transient(e):
                print(f"::warning::Skipped run {run['html_url']}, GitHub keeps failing: {describe_error(e)}")
            else:
                print(f"Could not handle run {run['html_url']}: {describe_error(e)}", file=sys.stderr)
                ok = False
    return ok


def main(argv: list[str]) -> int:
    if len(argv) != 1 or argv[0] not in ("slack", "pr-comments"):
        print("Usage: spot_interruptions.py slack|pr-comments", file=sys.stderr)
        return 2
    repo = os.environ["GITHUB_REPOSITORY"]
    if argv[0] == "slack":
        run_slack(repo, os.environ["RUN_ID"], int(os.environ["RUN_ATTEMPT"]))
    elif not run_pr_comments(repo, datetime.now(tz=UTC)):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
