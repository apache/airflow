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
Open a pull request in ``$GITHUB_DEMO_REPO``, wait until it is merged or closed, and say which (live mode).

``open_pr`` opens the pull request through the ``github_default`` connection. ``wait_for_close`` then parks in
``awaiting_input``, holding no worker or pool slot, until GitHub's ``pull_request`` ``closed`` delivery for that
pull request reaches ``/webhooks/github``. ``report`` prints whether it was merged or closed without merging.
"""

from __future__ import annotations

import os
import re
from datetime import timedelta

from airflow.sdk import Param, dag, task

REPOSITORY = os.environ.get("GITHUB_DEMO_REPO") or "my-org/configs"


@dag(
    schedule=None,
    catchup=False,
    params={"title": Param("Opened by Airflow", type="string", description="Title of the pull request")},
)
def pr_open_and_wait():
    @task
    def open_pr(*, params, run_id) -> int:
        if not os.environ.get("GITHUB_DEMO_REPO"):
            raise ValueError("Set GITHUB_DEMO_REPO and the github_default connection to open a pull request")
        from airflow.providers.github.hooks.github import GithubHook

        repo = GithubHook(github_conn_id="github_default").get_conn().get_repo(REPOSITORY)
        base = repo.default_branch
        branch = "airflow/" + re.sub(r"[^A-Za-z0-9._-]", "-", run_id)
        repo.create_git_ref(ref=f"refs/heads/{branch}", sha=repo.get_branch(base).commit.sha)
        repo.create_file(
            path=f"airflow-runs/{branch.removeprefix('airflow/')}.md",
            message=f"Add a note for Airflow run {run_id}",
            content=f"Opened by the Airflow Dag run {run_id}.\n",
            branch=branch,
        )
        pull_request = repo.create_pull(
            base=base,
            head=branch,
            title=params["title"],
            body=f"Opened by the Airflow Dag `pr_open_and_wait`, run `{run_id}`. Merge or close it.",
        )
        print(f"Opened #{pull_request.number}: {pull_request.html_url}")
        print("Merge or close it on GitHub; wait_for_close resumes when GitHub's delivery arrives.")
        return pull_request.number

    @task.webhook_wait(webhook="github", events=["pull_request"], timeout=timedelta(days=1))
    def wait_for_close(number: int) -> dict:
        return {"repository": REPOSITORY, "number": number, "action": "closed"}

    @task
    def report(event: dict) -> None:
        pull_request = event["payload"]["pull_request"]
        who = event["payload"]["sender"]["login"]
        if pull_request["merged"]:
            print(
                f"PR #{pull_request['number']} {pull_request['title']!r} was MERGED by "
                f"{(pull_request.get('merged_by') or {}).get('login', who)} "
                f"as {pull_request['merge_commit_sha'][:7]}: {pull_request['html_url']}"
            )
        else:
            print(
                f"PR #{pull_request['number']} {pull_request['title']!r} was CLOSED without merging "
                f"by {who}: {pull_request['html_url']}"
            )
        print(f"(webhook delivery {event['delivery_id']}, received at {event['received_at']})")

    report(wait_for_close(open_pr()))


pr_open_and_wait()
