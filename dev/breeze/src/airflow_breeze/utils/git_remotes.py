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
"""Git remotes of the local checkout, as `breeze ci upgrade` and `breeze ci audit` understand them."""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass

from airflow_breeze.utils.run_utils import run_command

# Every form git and GitHub accept for a GitHub repository: http or https with optional
# credentials, `www.` or a port, scp-like ssh and ssh URLs with an optional port. The host is
# case-insensitive. The repository must be the whole path, so `apache/airflow-site` never
# matches `apache/airflow`.
GITHUB_REMOTE_URL = re.compile(
    r"^(?:https?://(?:[^@/]+@)?(?:www\.)?github\.com(?::\d+)?/"
    r"|git@github\.com:"
    r"|ssh://git@(?:www\.)?github\.com(?::\d+)?/)"
    r"(?P<repo>[^/\s]+/[^/\s]+?)(?:\.git)?/?$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class Remote:
    name: str
    url: str
    repo: str | None


def get_github_repo(url: str) -> str | None:
    """Return ``owner/name`` for a GitHub remote URL, None for any other URL."""
    match = GITHUB_REMOTE_URL.match(url)
    return match.group("repo") if match else None


def parse_remotes(git_remote_v_output: str) -> list[Remote]:
    """Parse ``git remote -v`` output into one entry per remote, using its fetch URL."""
    remotes: dict[str, Remote] = {}
    for line in git_remote_v_output.splitlines():
        parts = line.split()
        if len(parts) < 2 or parts[0] in remotes:
            continue
        remotes[parts[0]] = Remote(name=parts[0], url=parts[1], repo=get_github_repo(parts[1]))
    return list(remotes.values())


def find_remote_for_repo(remotes: Sequence[Remote], repo: str) -> Remote | None:
    wanted = repo.casefold()
    return next((remote for remote in remotes if (remote.repo or "").casefold() == wanted), None)


def find_remote(remotes: Sequence[Remote], name: str) -> Remote | None:
    return next((remote for remote in remotes if remote.name == name), None)


def get_remotes() -> list[Remote]:
    result = run_command(["git", "remote", "-v"], capture_output=True, text=True, check=False)
    return parse_remotes(result.stdout) if result.returncode == 0 else []
