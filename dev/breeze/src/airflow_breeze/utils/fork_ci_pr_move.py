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
Move a single pull request between project CI and fork CI (AIP-120), for a maintainer.

The fork CI monitor reacts to the `use project ci` and `use fork ci` labels only on its next run, so
a fork `CI passed` success posted before `use project ci` was added still lets the pull request
merge until then. These run with the maintainer's own `gh` credentials and do in order what the
label alone leaves to that run.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow_breeze.utils.fork_ci_gate import LABEL_USE_FORK_CI, LABEL_USE_PROJECT_CI
from airflow_breeze.utils.fork_ci_messages import (
    CI_PASSED_CONTEXT,
    CI_PASSED_WAITING_PROJECT_CI_DESCRIPTION,
    is_fork_ci_passed_description,
)

if TYPE_CHECKING:
    from airflow_breeze.utils.fork_ci_github import GitHubClient


def read_open_pull_request(client: GitHubClient, pr_number: int) -> dict[str, Any]:
    pr = client.rest_get(f"repos/{client.repo}/pulls/{pr_number}")
    if pr.get("state") != "open":
        raise ValueError(f"pull request #{pr_number} is not open")
    return pr


def find_ci_passed_status(client: GitHubClient, sha: str) -> dict[str, Any] | None:
    # Paginated: the fork CI monitor posts a status per failed job, so a commit can have many contexts.
    statuses = client.rest_get(
        f"repos/{client.repo}/commits/{sha}/status?per_page=100", paginate=True, items_key="statuses"
    )
    return next((status for status in statuses if status["context"] == CI_PASSED_CONTEXT), None)


def move_to_project_ci(client: GitHubClient, pr_number: int, *, restart: bool = True) -> list[str]:
    """
    Withhold a fork `CI passed` success, label the pull request `use project ci` and restart it.

    The success is withheld before the label is added, so there is no moment at which the pull
    request carries the label and can still merge on the fork result. The status and its description
    are those the monitor writes when it releases the pull request, so its next run changes nothing.
    A success that project CI earned is kept. Closing and reopening starts project CI, which the label
    does not, and the monitor deleted the gated runs there would be to re-run.

    :return: what was done, for the maintainer.
    """
    pr = read_open_pull_request(client, pr_number)
    head_sha = pr["head"]["sha"]
    labels = {label["name"] for label in pr.get("labels") or []}
    done = []
    status = find_ci_passed_status(client, head_sha)
    if (
        status is not None
        and status["state"] == "success"
        and is_fork_ci_passed_description(status.get("description"))
    ):
        client.set_status(
            pr_number,
            head_sha,
            "pending",
            f"https://github.com/{client.repo}/pull/{pr_number}/checks",
            CI_PASSED_WAITING_PROJECT_CI_DESCRIPTION,
            context=CI_PASSED_CONTEXT,
        )
        done.append(f"withheld the fork CI `{CI_PASSED_CONTEXT}` success on {head_sha[:10]}")
    if LABEL_USE_FORK_CI in labels:
        client.remove_label(pr_number, LABEL_USE_FORK_CI)
        done.append(f"removed `{LABEL_USE_FORK_CI}`")
    if LABEL_USE_PROJECT_CI not in labels:
        client.add_labels(pr_number, [LABEL_USE_PROJECT_CI])
        done.append(f"added `{LABEL_USE_PROJECT_CI}`")
    if restart:
        client.close(pr_number)
        client.reopen(pr_number)
        done.append("closed and reopened it to start project CI")
    return done


def move_to_fork_ci(client: GitHubClient, pr_number: int) -> list[str]:
    """
    Label the pull request `use fork ci` instead of `use project ci`.

    A `CI passed` success that project CI earned stays, as it still holds for the commit. The monitor
    gates the pull request on its next run, and fork CI runs on the author's next push to their fork.

    :return: what was done, for the maintainer.
    """
    pr = read_open_pull_request(client, pr_number)
    labels = {label["name"] for label in pr.get("labels") or []}
    done = []
    if LABEL_USE_PROJECT_CI in labels:
        client.remove_label(pr_number, LABEL_USE_PROJECT_CI)
        done.append(f"removed `{LABEL_USE_PROJECT_CI}`")
    if LABEL_USE_FORK_CI not in labels:
        client.add_labels(pr_number, [LABEL_USE_FORK_CI])
        done.append(f"added `{LABEL_USE_FORK_CI}`")
    return done
