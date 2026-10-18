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
A merge gate with WaitForWebhookOperator: apply a change only once its pull request is merged.

Triggered with the Dag param ``pr_number``, it waits for that pull request in ``my-org/configs`` (send-test
mode). Triggered without it, ``open_pr`` opens a pull request in ``$GITHUB_DEMO_REPO`` through the
``github_default`` connection and waits for that one (live mode).
"""

from __future__ import annotations

import os
import re
from datetime import timedelta

from airflow.providers.standard.operators.webhooks import WaitForWebhookOperator
from airflow.sdk import Param, dag, task

REPOSITORY = os.environ.get("GITHUB_DEMO_REPO") or "my-org/configs"


@dag(schedule=None, catchup=False, params={"pr_number": Param(None, type=["null", "integer"])})
def merge_gate_operator():
    @task
    def open_pr(**context) -> int:
        if (number := context["params"]["pr_number"]) is not None:
            return number
        if not os.environ.get("GITHUB_DEMO_REPO"):
            raise ValueError(
                "Trigger with the pr_number param, or set GITHUB_DEMO_REPO to open a pull request"
            )
        from airflow.providers.github.hooks.github import GithubHook

        repo = GithubHook().get_conn().get_repo(REPOSITORY)
        branch = "airflow-demo/" + re.sub(r"[^A-Za-z0-9._-]", "-", context["run_id"])
        repo.create_git_ref(ref=f"refs/heads/{branch}", sha=repo.get_branch(repo.default_branch).commit.sha)
        repo.create_file(
            path=f"{branch}.txt", message="Update configs", content=context["run_id"], branch=branch
        )
        pull_request = repo.create_pull(
            base=repo.default_branch, head=branch, title="Update configs", body="Opened by Airflow"
        )
        print(f"Opened {pull_request.html_url}; merge or close it to resume wait_for_merge")
        return pull_request.number

    wait_for_merge = WaitForWebhookOperator(
        task_id="wait_for_merge",
        webhook="github",
        events=["pull_request"],
        match={"repository": REPOSITORY, "number": open_pr(), "action": "closed"},
        timeout=timedelta(days=3),
    )

    @task
    def apply(delivery: dict) -> str:
        pull_request = delivery["payload"]["pull_request"]
        if not pull_request["merged"]:
            raise RuntimeError(f"Pull request {pull_request['number']} was closed without merging")
        return pull_request["merge_commit_sha"]

    apply(wait_for_merge.output)


merge_gate_operator()
