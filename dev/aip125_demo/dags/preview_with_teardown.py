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
A preview with a setup and a teardown, cancelled when its pull request is closed (Phase 3).

Triggered by hand with the conf ``{"pr": "<repository>#<number>"}``. A cancel fails ``serve`` but leaves the
teardown ``destroy_env`` to run, and the run ends failed once it has. The repository is
``$PREVIEW_DEMO_REPO``, ``my-org/shop`` by default.
"""

from __future__ import annotations

import os
import time

from airflow.sdk import Param, WebhookMatch, dag, task

REPOSITORY = os.environ.get("PREVIEW_DEMO_REPO") or "my-org/shop"
SECONDS = int(os.environ.get("PREVIEW_DEMO_SECONDS") or 1800)

pr_closed = WebhookMatch(
    webhook="github",
    events=["pull_request"],
    match={"repository": REPOSITORY, "action": "closed"},
    correlation_key_conf="pr",
)


@dag(
    schedule=None,
    catchup=False,
    cancel_on=[pr_closed],
    params={"pr": Param(type="string", pattern=r"^[^#]+#\d+$")},
)
def preview_with_teardown():
    @task
    def create_env(*, params) -> None:
        print(f"Creating the environment of {params['pr']}")

    @task
    def serve() -> None:
        deadline = time.monotonic() + SECONDS
        while time.monotonic() < deadline:
            time.sleep(1)

    @task
    def destroy_env(*, params) -> None:
        print(f"Destroying the environment of {params['pr']}")

    created = create_env()
    created >> serve() >> destroy_env().as_teardown(setups=created)


preview_with_teardown()
