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
Fork CI eligibility (AIP-120).

Decides whether a CI run in ``apache/airflow`` should use project CI or leave the pull request
to CI in the contributor's fork. See dev/breeze/doc/ci/08_fork_ci_for_external_contributors.md.

The gate job in ``ci-amd.yml`` runs this file with the runner's ``python3`` from a sparse
checkout, without installing breeze, so it must only import from the standard library.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any

APACHE_REPO = "apache/airflow"

LABEL_FORK_CI_REQUIRED = "fork ci required"
LABEL_USE_PROJECT_CI = "use project ci"
LABEL_USE_FORK_CI = "use fork ci"
LABEL_READY_FOR_REVIEW = "ready for maintainer review"

DEFAULT_CLOSE_AFTER_DAYS = 7
DEFAULT_ENABLED_SINCE = datetime(2026, 1, 1, tzinfo=UTC)

# The push patterns ci-amd.yml used before its push trigger became '**'. GitHub branch filters
# use the same `+`, `?`, `*` and `[...]` semantics as these regular expressions.
CANONICAL_PUSH_BRANCH_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(pattern)
    for pattern in (
        r"^v[0-9]+-[0-9]+-test$",
        r"^providers-[a-z]+-?[a-z]*/v[0-9]+-[0-9]+$",
        r"^chart/v[0-9]+-[0-9]+x-test$",
        r"^airflow-ctl/v[0-9]+-[0-9]+-test$",
    )
)

TRUSTED_ASSOCIATIONS = frozenset({"MEMBER", "OWNER", "COLLABORATOR"})


class Reason(str, Enum):
    SCHEDULED = "scheduled"
    FORK_SCHEDULE = "fork_schedule"
    FORK_PUSH = "fork_push"
    CANONICAL_BRANCH = "canonical_branch"
    ADHOC_BRANCH = "adhoc_branch"
    LABEL_PROJECT_CI = "label_project_ci"
    LABEL_FORK_CI = "label_fork_ci"
    PILOT = "pilot"
    GATE_DISABLED = "gate_disabled"
    HEAD_IS_UPSTREAM = "head_is_upstream"
    BEFORE_ENABLED_SINCE = "before_enabled_since"
    TRUSTED_AUTHOR = "trusted_author"
    ALLOWLISTED = "allowlisted"
    EXTERNAL = "external"


GATED_REASONS = frozenset({Reason.EXTERNAL, Reason.PILOT, Reason.LABEL_FORK_CI})
TAMPER_CHECKED_REASONS = frozenset({Reason.EXTERNAL, Reason.PILOT})


@dataclass(frozen=True)
class GateConfig:
    enabled: bool = False
    enabled_since: datetime = DEFAULT_ENABLED_SINCE
    close_after_days: int = DEFAULT_CLOSE_AFTER_DAYS
    allowlist: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    pilot: Mapping[str, datetime] = field(default_factory=dict)

    def is_allowlisted(self, login: str) -> bool:
        wanted = login.casefold()
        return any(wanted == member.casefold() for members in self.allowlist.values() for member in members)

    def pilot_joined_at(self, login: str) -> datetime | None:
        wanted = login.casefold()
        for pilot_login, joined_at in self.pilot.items():
            if pilot_login.casefold() == wanted:
                return joined_at
        return None


def _require_aware_datetime(value: Any, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(
            f"{name} must be a TOML offset date-time such as 2026-10-20T00:00:00Z, got {value!r}"
        )
    return value


def parse_config(text: str) -> GateConfig:
    try:
        data = tomllib.loads(text)
    except tomllib.TOMLDecodeError as e:
        raise ValueError(f"Invalid fork CI gate configuration: {e}") from e

    enabled = data.get("enabled", False)
    if not isinstance(enabled, bool):
        raise ValueError(f"enabled must be true or false, got {enabled!r}")
    enabled_since = _require_aware_datetime(data.get("enabled_since", DEFAULT_ENABLED_SINCE), "enabled_since")
    close_after_days = data.get("close_after_days", DEFAULT_CLOSE_AFTER_DAYS)
    if isinstance(close_after_days, bool) or not isinstance(close_after_days, int) or close_after_days < 1:
        raise ValueError(f"close_after_days must be a positive integer, got {close_after_days!r}")

    allowlist: dict[str, tuple[str, ...]] = {}
    for org, logins in data.get("allowlist", {}).items():
        if not isinstance(logins, list) or not all(isinstance(login, str) for login in logins):
            raise ValueError(f"allowlist entry {org!r} must be a list of GitHub logins, got {logins!r}")
        allowlist[org] = tuple(logins)

    pilot = {
        login.casefold(): _require_aware_datetime(joined_at, f"pilot entry {login!r}")
        for login, joined_at in data.get("pilot", {}).items()
    }
    return GateConfig(
        enabled=enabled,
        enabled_since=enabled_since,
        close_after_days=close_after_days,
        allowlist=allowlist,
        pilot=pilot,
    )


def load_config(path: Path) -> GateConfig:
    return parse_config(path.read_text())


@dataclass(frozen=True)
class GateEvent:
    event_name: str
    repository: str
    ref: str
    head_repository: str = ""
    author_login: str = ""
    author_association: str = ""
    created_at: datetime | None = None
    labels: tuple[str, ...] = ()


@dataclass(frozen=True)
class Decision:
    run: bool
    reason: Reason


def _is_canonical_branch(ref: str) -> bool:
    branch = ref.removeprefix("refs/heads/")
    return any(pattern.match(branch) for pattern in CANONICAL_PUSH_BRANCH_PATTERNS)


def decide(event: GateEvent, config: GateConfig) -> Decision:
    """Apply the eligibility rules of the design in order; the first matching rule wins."""
    if event.event_name == "schedule" and event.repository != APACHE_REPO:
        return Decision(run=False, reason=Reason.FORK_SCHEDULE)
    if event.event_name in ("schedule", "workflow_dispatch"):
        return Decision(run=True, reason=Reason.SCHEDULED)
    if event.event_name == "push":
        if event.repository != APACHE_REPO:
            return Decision(run=True, reason=Reason.FORK_PUSH)
        if _is_canonical_branch(event.ref):
            return Decision(run=True, reason=Reason.CANONICAL_BRANCH)
        return Decision(run=False, reason=Reason.ADHOC_BRANCH)
    if LABEL_USE_PROJECT_CI in event.labels:
        return Decision(run=True, reason=Reason.LABEL_PROJECT_CI)
    if LABEL_USE_FORK_CI in event.labels:
        return Decision(run=False, reason=Reason.LABEL_FORK_CI)
    joined_at = config.pilot_joined_at(event.author_login)
    if joined_at is not None and event.created_at is not None and event.created_at >= joined_at:
        return Decision(run=False, reason=Reason.PILOT)
    if not config.enabled:
        return Decision(run=True, reason=Reason.GATE_DISABLED)
    if event.head_repository == APACHE_REPO:
        return Decision(run=True, reason=Reason.HEAD_IS_UPSTREAM)
    if event.created_at is not None and event.created_at < config.enabled_since:
        return Decision(run=True, reason=Reason.BEFORE_ENABLED_SINCE)
    if event.author_association in TRUSTED_ASSOCIATIONS:
        return Decision(run=True, reason=Reason.TRUSTED_AUTHOR)
    if config.is_allowlisted(event.author_login):
        return Decision(run=True, reason=Reason.ALLOWLISTED)
    return Decision(run=False, reason=Reason.EXTERNAL)


def parse_github_datetime(value: str) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def fetch_pull_request_labels(repository: str, number: str) -> tuple[str, ...] | None:
    """Read the labels live, so a label added after the event is seen on a re-run."""
    result = subprocess.run(
        ["gh", "api", f"repos/{repository}/pulls/{number}", "--jq", "[.labels[].name]"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        print(f"Could not read labels of pull request #{number}: {result.stderr.strip()}", file=sys.stderr)
        return None
    try:
        labels = json.loads(result.stdout)
    except json.JSONDecodeError:
        print(f"Unexpected label output for pull request #{number}: {result.stdout!r}", file=sys.stderr)
        return None
    return tuple(labels)


def _write_line(env_var: str, line: str, env: Mapping[str, str]) -> None:
    path = env.get(env_var)
    if path:
        with open(path, "a") as f:
            f.write(line + "\n")


def evaluate_from_env(env: Mapping[str, str]) -> Decision:
    event_name = env.get("GITHUB_EVENT_NAME", "")
    repository = env.get("GITHUB_REPOSITORY", "")
    labels: tuple[str, ...] = ()
    if event_name == "pull_request":
        fetched = fetch_pull_request_labels(repository, env.get("PR_NUMBER", ""))
        if fetched is None:
            # Never fail open: anyone able to exhaust the shared token quota would otherwise switch
            # the gate off for every pull request. The event's labels miss only labels added since.
            print("Falling back to the labels of the triggering event", file=sys.stderr)
            fetched = tuple(json.loads(env.get("PR_EVENT_LABELS") or "[]"))
        labels = fetched
    event = GateEvent(
        event_name=event_name,
        repository=repository,
        ref=env.get("GITHUB_REF", ""),
        head_repository=env.get("PR_HEAD_REPO", ""),
        author_login=env.get("PR_AUTHOR", ""),
        author_association=env.get("PR_AUTHOR_ASSOCIATION", ""),
        created_at=parse_github_datetime(env.get("PR_CREATED_AT", "")),
        labels=labels,
    )
    config = load_config(Path(env.get("GATE_CONFIG", ".github/fork-ci-gate.toml")))
    return decide(event, config)


def main(env: Mapping[str, str] | None = None) -> int:
    env = os.environ if env is None else env
    decision = evaluate_from_env(env)
    project_ci = "true" if decision.run else "false"
    print(f"project-ci={project_ci} reason={decision.reason.value}")
    _write_line("GITHUB_OUTPUT", f"project-ci={project_ci}", env)
    _write_line("GITHUB_OUTPUT", f"reason={decision.reason.value}", env)
    verdict = "runs" if decision.run else "is skipped"
    _write_line(
        "GITHUB_STEP_SUMMARY", f"Project CI {verdict} (fork CI gate reason: `{decision.reason.value}`)", env
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
