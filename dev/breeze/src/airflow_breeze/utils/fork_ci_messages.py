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
"""
Texts the fork CI reconciler posts on pull requests (AIP-120).

All wording lives here so it can be reviewed in one place. Texts must say that CI is free for the
contributor and finite for the project, must not characterise the recipient, and must give the
exact command to run.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime

STATUS_CONTEXT = "Fork CI / Tests (AMD)"
# The check the "Require CI on main" ruleset requires before a pull request can (auto-)merge. Project
# CI's gate job reports it; for a gated pull request, which never runs project CI, the reconciler
# reports it from the fork run.
CI_PASSED_CONTEXT = "CI passed"
CI_PASSED_DESCRIPTION = "Fork CI passed for this commit"
CI_PASSED_REVOKED_DESCRIPTION = "Fork CI result no longer trusted for this commit"
# A fork run tests the head commit as it is, not merged with the base branch. Further behind than
# this, it says little about the result after merge.
BEHIND_WARNING_COMMITS = 200
MARKER_PREFIX = "<!-- fork-ci-gate: "
MARKER_PATTERN = re.compile(r"<!-- fork-ci-gate: (?P<payload>.*?) -->")
DESIGN_URL = (
    "https://github.com/apache/airflow/blob/main/dev/breeze/doc/ci/08_fork_ci_for_external_contributors.md"
)
CONTRIBUTING_URL = (
    "https://github.com/apache/airflow/blob/main/contributing-docs/18_contribution_workflow.rst"
)
WHY_AUTOMATED_URL = (
    "https://github.com/apache/airflow/blob/main/contributing-docs/25_maintainer_pr_triage.md"
    "#why-the-first-pass-is-automated"
)

FOOTER = (
    "\n\n---\n"
    "_This comment was posted automatically by the fork CI monitor from templates drafted with an "
    "AI-assisted tool, so it may contain mistakes. An Apache Airflow maintainer, a real person, "
    "will review the pull request once its fork CI is green. "
    f"[Why the first pass is automated]({WHY_AUTOMATED_URL})._"
)

WHY_FORK_CI = (
    "Pull requests from contributors run their CI in the contributor's own fork. GitHub Actions is free "
    "and has no minute limit on public forks, while the CI capacity of the Apache Software Foundation "
    "is shared by every ASF project and is finite."
)


@dataclass(frozen=True)
class Marker:
    state: str
    sha: str
    since: datetime
    # Commits the head commit was behind its base branch when its fork CI turned green.
    behind: int | None = None


def render_marker(marker: Marker) -> str:
    data: dict[str, object] = {"state": marker.state, "sha": marker.sha, "since": marker.since.isoformat()}
    if marker.behind is not None:
        data["behind"] = marker.behind
    return f"{MARKER_PREFIX}{json.dumps(data, sort_keys=True)} -->"


def parse_marker(body: str) -> Marker | None:
    """Return the marker of a comment, None when it has none; raise ValueError when it is malformed."""
    match = MARKER_PATTERN.search(body)
    if match is None:
        return None
    try:
        data = json.loads(match.group("payload"))
        return Marker(
            state=data["state"],
            sha=data["sha"],
            since=datetime.fromisoformat(data["since"]),
            behind=data.get("behind"),
        )
    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
        raise ValueError(f"Malformed fork CI marker: {match.group(0)!r}") from e


def fork_actions_url(fork: str) -> str:
    return f"https://github.com/{fork}/actions/workflows/ci-amd.yml"


def _setup_instructions(fork: str) -> str:
    return (
        "To get CI for this pull request:\n\n"
        "1. Enable the workflow in your fork (once per fork):\n\n"
        f"   ```bash\n   gh workflow enable ci-amd.yml -R {fork}\n   ```\n\n"
        "   If your fork's `main` is older than the workflow, sync it first with "
        f"`gh repo sync {fork} --branch main`.\n"
        "2. Push your branch again. Every push then runs CI in your fork.\n\n"
        "`breeze ci audit` checks your whole local and fork setup and prints the command that fixes "
        f"anything missing. See [the contribution workflow]({CONTRIBUTING_URL})."
    )


_STATE_TEXTS = {
    "setup_required": "**Fork CI: setup required.** CI for this pull request has not run in your fork yet, "
    "because the `Tests (AMD)` workflow is not enabled there.",
    "awaiting_push": "**Fork CI: waiting for a push.** The workflow is enabled in your fork, but it has not "
    "run for the latest commit of this pull request. Push the branch again to start it.",
    "running": "**Fork CI: running.** CI for the latest commit is running in your fork.",
    "failed": "**Fork CI: failed.** CI for the latest commit failed in your fork. Fix the failures and "
    "push again; this pull request stays a draft until the run is green.",
    "green": "**Fork CI: green.** CI for the latest commit passed in your fork. The pull request is "
    "ready for maintainer review.",
    "green_unverified": "**Fork CI: green, needs a maintainer.** CI for the latest commit passed in your "
    "fork, but this pull request changes files that define what CI runs (such as `.github/`, "
    "`dev/breeze/` or `scripts/ci/`), and the fork run used those changed files. A maintainer will "
    "review the change and run project CI for it; the pull request stays a draft until then.",
    "closed": "**Fork CI: closed after inactivity.** CI did not run in your fork in time; see the "
    "comment below for how to continue.",
    "gate_bypassed": "**Fork CI: closed.** Project CI ran for this pull request although its CI runs in "
    "the contributor's fork; see the comment below.",
}


def behind_warning(behind: int, *, base_ref: str) -> str:
    return (
        f"**This branch is {behind} commits behind `{base_ref}`.** Fork CI tests the branch as it is, "
        f"while the merged result also contains those commits, so it can still fail after the merge. "
        f"Rebase on `{base_ref}` and push to test it against the current code."
    )


def state_comment(marker: Marker, *, fork: str, run_url: str | None, base_ref: str = "main") -> str:
    parts = [render_marker(marker), _STATE_TEXTS[marker.state], WHY_FORK_CI]
    if (
        marker.state in ("green", "green_unverified")
        and marker.behind is not None
        and marker.behind >= BEHIND_WARNING_COMMITS
    ):
        parts.append(behind_warning(marker.behind, base_ref=base_ref))
    if run_url:
        parts.append(f"Latest run: {run_url}")
    if marker.state == "setup_required":
        parts.append(_setup_instructions(fork))
    parts.append(f"How this works: [fork CI for external contributors]({DESIGN_URL}).")
    return "\n\n".join(parts) + FOOTER


def released_comment(marker: Marker) -> str:
    return (
        f"{render_marker(marker)}\n\n"
        "**Project CI in use.** A maintainer added the `use project ci` label, so CI for this pull "
        "request runs in apache/airflow. It starts on the next push or re-run." + FOOTER
    )


def ungated_comment(marker: Marker, *, base_ref: str) -> str:
    return (
        f"{render_marker(marker)}\n\n"
        f"**Project CI in use.** The base branch `{base_ref}` does not use fork CI, so CI for this pull "
        "request runs in apache/airflow." + FOOTER
    )


def nudge_comment(*, fork: str) -> str:
    return (
        f"The `Tests (AMD)` workflow is now enabled in {fork}. Push this branch again to start CI "
        "for this pull request; the workflow only runs for pushes made after it was enabled." + FOOTER
    )


def inactivity_close_comment(*, days: int, fork: str) -> str:
    return (
        f"Closing this pull request because CI has not run in your fork for {days} days. {WHY_FORK_CI}\n\n"
        "You are welcome to reopen it at any time after setting up fork CI:\n\n"
        f"{_setup_instructions(fork)}\n\nReopening restarts the {days}-day period." + FOOTER
    )


def tamper_close_comment(*, run_url: str) -> str:
    return (
        "Closing this pull request because project CI ran for it in apache/airflow although it is "
        f"set to run its CI in the contributor's fork: {run_url}\n\n"
        "The pull request changes files under `.github/workflows/`. Such changes cannot be verified "
        "in apache/airflow without a maintainer, because they can change which CI runs. If the "
        "workflow change is wanted, a maintainer can reopen the pull request and add the "
        "`use project ci` label." + FOOTER
    )


_STATUS_DESCRIPTIONS = {
    "setup_required": "Fork CI not set up - see the pull request comment",
    "awaiting_push": "Fork CI enabled, waiting for a push - see the pull request comment",
    "running": "Fork CI running - see Details",
    "failed": "Fork CI failed - see Details",
    "green": "Fork CI passed - see Details",
    "green_unverified": "Fork CI passed, but CI files changed - a maintainer must run project CI",
    "gate_bypassed": "Project CI ran for a gated pull request - see Details",
    "released": "Project CI in use (label `use project ci`) - see the checks",
    "ungated": "Project CI in use (base branch without fork CI) - see the checks",
}

STATUS_STATES = {
    "setup_required": "error",
    "awaiting_push": "pending",
    "running": "pending",
    "failed": "failure",
    "green": "success",
    "green_unverified": "pending",
    "gate_bypassed": "error",
    "released": "success",
    "ungated": "success",
}


def status_description(state: str) -> str:
    return _STATUS_DESCRIPTIONS[state]
