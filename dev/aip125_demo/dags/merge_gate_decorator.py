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
The merge gate of merge_gate_operator.py, with @task.webhook_wait instead of the operator.

The decorated function runs once on the worker, before the task parks, and returns what the delivery must
match; the task's XCom is the delivery that resumes it.
"""

from __future__ import annotations

import os
import re
from datetime import timedelta

from airflow.sdk import Param, dag, task

REPOSITORY = os.environ.get("GITHUB_DEMO_REPO") or "my-org/configs"


@dag(schedule=None, catchup=False, params={"pr_number": Param(None, type=["null", "integer"])})
def merge_gate_decorator():
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

    @task.webhook_wait(webhook="github", events=["pull_request"], timeout=timedelta(days=3))
    def wait_for_merge(number: int) -> dict:
        return {"repository": REPOSITORY, "number": number, "action": "closed"}

    @task
    def apply(delivery: dict) -> str:
        pull_request = delivery["payload"]["pull_request"]
        if not pull_request["merged"]:
            raise RuntimeError(f"Pull request {pull_request['number']} was closed without merging")
        return pull_request["merge_commit_sha"]

    apply(wait_for_merge(open_pr()))


merge_gate_decorator()
