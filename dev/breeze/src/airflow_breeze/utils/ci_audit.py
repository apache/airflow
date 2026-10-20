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
Checks behind `breeze ci audit`: is this checkout and fork set up for fork CI (AIP-120)?

Every check is read-only. A failing check offers the commands that fix it; `--fix` runs them after
confirmation. See "breeze ci audit" in dev/breeze/doc/ci/08_fork_ci_for_external_contributors.md.
"""

from __future__ import annotations

import json
import re
import subprocess
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from packaging.version import Version

from airflow_breeze.global_constants import APACHE_AIRFLOW_GITHUB_REPOSITORY, MIN_GH_VERSION
from airflow_breeze.utils.git_remotes import find_remote, find_remote_for_repo, parse_remotes

if TYPE_CHECKING:
    from airflow_breeze.utils.fork_ci_gate import GateConfig

UPSTREAM_URL = f"https://github.com/{APACHE_AIRFLOW_GITHUB_REPOSITORY}.git"
CI_WORKFLOW_FILE = "ci-amd.yml"
# Commits a fork's main may lag behind upstream main before a sync is suggested.
MAX_FORK_MAIN_BEHIND = 200

Runner = Callable[[Sequence[str]], subprocess.CompletedProcess]


class CheckStatus(str, Enum):
    OK = "ok"
    FAIL = "fail"
    INFO = "info"


class Role(str, Enum):
    COMMITTER = "committer"
    COLLABORATOR = "collaborator"
    EXEMPTED = "exempted organisation member"
    PILOT = "fork CI pilot participant"
    EXTERNAL = "external contributor"


@dataclass(frozen=True)
class CheckResult:
    number: int
    name: str
    status: CheckStatus
    finding: str
    fix: tuple[str, ...] = ()


def run_tool(command: Sequence[str]) -> subprocess.CompletedProcess:
    """Run a command for the audit; a missing executable is a failed command, not a crash."""
    try:
        return subprocess.run(list(command), capture_output=True, text=True, check=False)
    except OSError as e:
        return subprocess.CompletedProcess(list(command), 127, stdout="", stderr=str(e))


def _ok(result: subprocess.CompletedProcess) -> bool:
    return result.returncode == 0


def _out(result: subprocess.CompletedProcess) -> str:
    return (result.stdout or "").strip()


def redact_url(url: str) -> str:
    """Drop the credentials an http(s) remote URL may carry, so a finding never prints a token."""
    return re.sub(r"^(https?://)[^@/]+@", r"\1", url)


def _is_not_found(result: subprocess.CompletedProcess) -> bool:
    return "404" in (result.stderr or "") or "Not Found" in (result.stderr or "")


def get_role(*, login: str, run: Runner, config: GateConfig, collaborators: frozenset[str]) -> Role:
    # In the gate's order: a pilot entry gates a pull request whatever its author's role.
    if config.pilot_joined_at(login) is not None:
        return Role.PILOT
    membership = run(["gh", "api", "user/memberships/orgs/apache", "--jq", ".state"])
    if _ok(membership) and _out(membership) == "active":
        return Role.COMMITTER
    if login.casefold() in {c.casefold() for c in collaborators}:
        return Role.COLLABORATOR
    if config.is_allowlisted(login):
        return Role.EXEMPTED
    return Role.EXTERNAL


def fork_ci_required(role: Role, config: GateConfig) -> bool | None:
    """True when fork CI must be on, False when it must be off, None when either is fine."""
    if role == Role.PILOT:
        return True
    if role == Role.EXTERNAL:
        return True if config.enabled else None
    return False


def run_audit(*, run: Runner, config: GateConfig, collaborators: frozenset[str]) -> list[CheckResult]:
    results: list[CheckResult] = []

    gh_version = run(["gh", "--version"])
    if not _ok(gh_version):
        results.append(
            CheckResult(
                1, "gh", CheckStatus.FAIL, "`gh` is not installed", ("echo See https://cli.github.com",)
            )
        )
        return results
    version_match = re.search(r"gh version (\d+\.\d+\.\d+)", _out(gh_version))
    if version_match is None or Version(version_match.group(1)) < Version(MIN_GH_VERSION):
        results.append(
            CheckResult(1, "gh", CheckStatus.FAIL, f"`gh` is older than {MIN_GH_VERSION}; upgrade it")
        )
        return results
    user = run(["gh", "api", "user", "--jq", ".login"])
    if not _ok(user):
        results.append(
            CheckResult(1, "gh", CheckStatus.FAIL, "`gh` is not authenticated", ("gh auth login",))
        )
        return results
    login = _out(user)
    results.append(CheckResult(1, "gh", CheckStatus.OK, f"authenticated as {login}"))

    # The role decides whether the fork setup matters at all: a committer working from an
    # apache/airflow clone without a fork is fine while their pull requests run project CI.
    role = get_role(login=login, run=run, config=config, collaborators=collaborators)
    required = fork_ci_required(role, config)
    setup_status = CheckStatus.INFO if required is False else CheckStatus.FAIL
    setup_note = "; only needed for fork CI, which your pull requests do not use" if required is False else ""

    remotes = parse_remotes(_out(run(["git", "remote", "-v"])))
    upstream = find_remote(remotes, "upstream")
    apache = find_remote_for_repo(remotes, APACHE_AIRFLOW_GITHUB_REPOSITORY)
    origin = find_remote(remotes, "origin")
    # Whether the remotes fix already adds `origin`, so the fork fix must not add it again.
    adds_origin = False
    if (
        upstream is not None
        and upstream.repo
        and upstream.repo.casefold() == APACHE_AIRFLOW_GITHUB_REPOSITORY
    ):
        if origin is not None and origin.repo and origin.repo.casefold() == APACHE_AIRFLOW_GITHUB_REPOSITORY:
            results.append(
                CheckResult(
                    2,
                    "remotes",
                    setup_status,
                    f"`origin` points at apache/airflow; it should be your fork{setup_note}",
                    (f"git remote set-url origin https://github.com/{login}/airflow.git",),
                )
            )
        else:
            results.append(CheckResult(2, "remotes", CheckStatus.OK, "`upstream` is apache/airflow"))
    elif apache is not None and upstream is None:
        fix = [f"git remote rename {apache.name} upstream"]
        if apache.name == "origin":
            fix.append(f"git remote add origin https://github.com/{login}/airflow.git")
            adds_origin = True
        results.append(
            CheckResult(
                2,
                "remotes",
                setup_status,
                f"apache/airflow is the remote `{apache.name}`; Airflow's convention is `upstream`{setup_note}",
                tuple(fix),
            )
        )
    elif upstream is not None:
        results.append(
            CheckResult(
                2,
                "remotes",
                CheckStatus.FAIL,
                f"`upstream` points at {redact_url(upstream.url)}, not apache/airflow",
                (f"git remote set-url upstream {UPSTREAM_URL}",),
            )
        )
    else:
        results.append(
            CheckResult(
                2,
                "remotes",
                CheckStatus.FAIL,
                "no `upstream` remote pointing at apache/airflow",
                (f"git remote add upstream {UPSTREAM_URL}",),
            )
        )

    fork = origin.repo if origin is not None and origin.repo else None
    fork_ok = False
    if fork is None or fork.casefold() == APACHE_AIRFLOW_GITHUB_REPOSITORY:
        results.append(
            CheckResult(
                3,
                "fork",
                setup_status,
                f"no `origin` remote pointing at your fork of apache/airflow{setup_note}",
                (
                    f"gh repo fork {APACHE_AIRFLOW_GITHUB_REPOSITORY} --remote=false",
                    *(
                        ()
                        if adds_origin
                        else (f"git remote add origin https://github.com/{login}/airflow.git",)
                    ),
                ),
            )
        )
    else:
        repo_info = run(
            ["gh", "api", f"repos/{fork}", "--jq", "{owner: .owner.login, parent: .parent.full_name}"]
        )
        info = json.loads(_out(repo_info)) if _ok(repo_info) else None
        if info is None and _is_not_found(repo_info):
            results.append(
                CheckResult(
                    3,
                    "fork",
                    setup_status,
                    f"{fork} does not exist{setup_note}",
                    (f"gh repo fork {APACHE_AIRFLOW_GITHUB_REPOSITORY} --remote=false",),
                )
            )
        elif info is None:
            results.append(
                CheckResult(
                    3,
                    "fork",
                    CheckStatus.INFO,
                    f"could not check {fork}: {(repo_info.stderr or '').strip() or 'gh api failed'}",
                )
            )
        elif (info.get("parent") or "").casefold() != APACHE_AIRFLOW_GITHUB_REPOSITORY:
            results.append(
                CheckResult(3, "fork", CheckStatus.FAIL, f"{fork} is not a fork of apache/airflow")
            )
        elif (info.get("owner") or "").casefold() != login.casefold():
            results.append(CheckResult(3, "fork", CheckStatus.FAIL, f"{fork} is not owned by {login}"))
        else:
            fork_ok = True
            results.append(CheckResult(3, "fork", CheckStatus.OK, f"{fork} is your fork of apache/airflow"))

    results.append(CheckResult(4, "role", CheckStatus.INFO, f"you are a {role.value}"))

    if fork_ok and fork is not None:
        workflow = run(["gh", "api", f"repos/{fork}/actions/workflows/{CI_WORKFLOW_FILE}", "--jq", ".state"])
        state = _out(workflow) if _ok(workflow) else "missing" if _is_not_found(workflow) else None
        if state is None:
            results.append(
                CheckResult(
                    5,
                    "fork workflows",
                    CheckStatus.INFO,
                    f"could not check `Tests (AMD)` in {fork}: {(workflow.stderr or '').strip() or 'gh api failed'}",
                )
            )
        elif required is True and state != "active":
            enable: tuple[str, ...] = (f"gh workflow enable {CI_WORKFLOW_FILE} -R {fork}",)
            if state == "missing":
                enable = (f"gh repo sync {fork} --branch main", *enable)
            results.append(
                CheckResult(
                    5,
                    "fork workflows",
                    CheckStatus.FAIL,
                    f"`Tests (AMD)` is {state} in {fork}; enable it",
                    enable,
                )
            )
        elif required is False and state == "active":
            # Not a failure: a committer trying fork CI with the `use fork ci` label needs it on.
            results.append(
                CheckResult(
                    5,
                    "fork workflows",
                    CheckStatus.INFO,
                    f"`Tests (AMD)` is enabled in {fork}, but your pull requests run project CI, so each "
                    "push runs CI twice; disable it unless you use the `use fork ci` label",
                    (f"gh workflow disable {CI_WORKFLOW_FILE} -R {fork}",),
                )
            )
        elif required is None:
            results.append(
                CheckResult(
                    5,
                    "fork workflows",
                    CheckStatus.INFO,
                    f"`Tests (AMD)` is {state} in {fork}; fork CI is not required yet",
                )
            )
        else:
            results.append(
                CheckResult(5, "fork workflows", CheckStatus.OK, f"`Tests (AMD)` is {state} in {fork}")
            )

        compare = run(
            [
                "gh",
                "api",
                f"repos/{APACHE_AIRFLOW_GITHUB_REPOSITORY}/compare/{fork.split('/')[0]}:main...main",
                "--jq",
                ".ahead_by",
            ]
        )
        behind = int(_out(compare)) if _ok(compare) and _out(compare).isdigit() else None
        if behind is None:
            results.append(
                CheckResult(6, "fork main", CheckStatus.INFO, "could not compare your fork's main")
            )
        elif behind > MAX_FORK_MAIN_BEHIND:
            results.append(
                CheckResult(
                    6,
                    "fork main",
                    CheckStatus.FAIL,
                    f"your fork's main is {behind} commits behind apache/airflow",
                    (f"gh repo sync {fork} --branch main",),
                )
            )
        else:
            results.append(
                CheckResult(6, "fork main", CheckStatus.OK, f"{behind} commits behind apache/airflow")
            )

    branch = _out(run(["git", "rev-parse", "--abbrev-ref", "HEAD"]))
    tracking = run(["git", "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"])
    if branch in ("main", "HEAD", ""):
        finding = "create a branch for your change; pull requests from your fork's main are hard to update"
    elif not _ok(tracking):
        finding = f"`{branch}` has no upstream; push it with `git push -u origin {branch}`"
    elif not _out(tracking).startswith("origin/"):
        finding = (
            f"`{branch}` tracks {_out(tracking)}; push it to your fork with `git push -u origin {branch}`"
        )
    else:
        finding = f"`{branch}` tracks {_out(tracking)}"
    results.append(CheckResult(7, "branch", CheckStatus.INFO, finding))
    return results
