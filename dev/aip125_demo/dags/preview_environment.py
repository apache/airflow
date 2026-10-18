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
A preview environment per pull request, cancelled by the pull request's own deliveries (Phase 3).

The label ``deploy-preview`` starts a run (a ``WebhookTopic``). Closing the pull request fails the run
through the Dag's ``cancel_on``: the scheduler kills ``serve_preview`` and ``run_e2e_tests``, and
``tear_down``, the Dag's ``on_failure_callback``, is called with the reason ``cancelled``. A new commit
(``synchronize``) only skips ``run_e2e_tests``, through the task's ``cancel_on``, and the preview keeps
running. A run triggered by hand with the conf ``{"pr": "<repository>#<number>"}`` is cancelled by the
close of that pull request too.

The repository is ``$PREVIEW_DEMO_REPO``, ``my-org/shop`` by default, so that its deliveries change none of
the outcomes the Phase 1 and 2 checks of ``demo.sh`` expect. The tasks run in the pool ``previews``, which
``demo.sh`` creates, so that ``default_pool`` shows only what the merge gates hold.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

from airflow.sdk import BaseOperator, Param, WebhookMatch, WebhookTopic, dag, task

REPOSITORY = os.environ.get("PREVIEW_DEMO_REPO") or "my-org/shop"
# demo.sh show raises it to outlast a presentation; in the checks, a closed or synchronize delivery ends
# each of these tasks.
SECONDS = int(os.environ.get("PREVIEW_DEMO_SECONDS") or 1800)

preview_requested = WebhookTopic(
    webhook="github", path=REPOSITORY, event="pull_request", action="labeled", detail="deploy-preview"
)
pr_closed = WebhookMatch(
    webhook="github",
    events=["pull_request"],
    match={"repository": REPOSITORY, "action": "closed"},
    correlation_key_conf="pr",
)
new_commit = WebhookMatch(
    webhook="github", events=["pull_request"], match={"repository": REPOSITORY, "action": "synchronize"}
)


def tear_down(context):
    """Tear the preview down; the Dag processor calls it when a run fails, as a cancel does."""
    print(f"Tearing down the preview of run {context['run_id']}: {context['reason']}")
    # demo.sh reads the reason from here, as the Dag processor's logs are not served by the REST API.
    if callbacks := os.environ.get("AIP125_DEMO_CALLBACKS"):
        Path(callbacks).mkdir(parents=True, exist_ok=True)
        Path(callbacks, f"{context['dag'].dag_id}.{context['run_id']}.json").write_text(
            json.dumps({"reason": context["reason"]})
        )


class ServePreviewOperator(BaseOperator):
    """Serve the preview until the run is cancelled, or for ``PREVIEW_DEMO_SECONDS``."""

    def execute(self, context):
        deadline = time.monotonic() + SECONDS
        while time.monotonic() < deadline:
            time.sleep(1)

    def on_kill(self):
        print("on_kill: stopping the preview server")


@dag(
    schedule=preview_requested,
    catchup=False,
    max_active_runs=2,
    cancel_on=[pr_closed],
    on_failure_callback=tear_down,
    default_args={"pool": "previews"},
    params={"pr": Param(None, type=["null", "string"])},
)
def preview_environment():
    @task
    def deploy(*, triggering_asset_events, params) -> str:
        events = triggering_asset_events.get(preview_requested, [])
        key = events[-1].extra["correlation_key"] if events else params["pr"]
        print(f"Deploying a preview of {key}")
        return key

    @task(cancel_on=[new_commit], cancel_state="skipped")
    def run_e2e_tests():
        deadline = time.monotonic() + SECONDS
        while time.monotonic() < deadline:
            time.sleep(1)

    deploy() >> [run_e2e_tests(), ServePreviewOperator(task_id="serve_preview")]


preview_environment()
