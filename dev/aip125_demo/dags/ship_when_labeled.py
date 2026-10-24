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
Ship a pull request once it gets the label ``ship-it``, unless it is closed first (Phase 3).

Triggered by hand with the conf ``{"pr": "<repository>#<number>"}``, ``wait_for_ship_it`` parks on a webhook
wait for the label. The Dag's ``cancel_on`` names the conf key ``pr``, so closing that pull request fails
the run and ends the wait; a run triggered after the close is left alone. The repository is
``$PREVIEW_DEMO_REPO``, ``my-org/shop`` by default.
"""

from __future__ import annotations

import os
from datetime import timedelta

from airflow.providers.standard.operators.webhooks import WaitForWebhookOperator
from airflow.sdk import Param, WebhookMatch, dag, task

REPOSITORY = os.environ.get("PREVIEW_DEMO_REPO") or "my-org/shop"

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
def ship_when_labeled():
    wait_for_ship_it = WaitForWebhookOperator(
        task_id="wait_for_ship_it",
        webhook="github",
        events=["pull_request"],
        match={
            "repository": REPOSITORY,
            "number": "{{ params.pr.rsplit('#', 1)[1] }}",
            "action": "labeled",
            "label": "ship-it",
        },
        timeout=timedelta(days=1),
    )

    @task
    def ship(delivery: dict) -> None:
        print(
            f"Shipping pull request {delivery['attributes']['number']}, labeled by {delivery['delivery_id']}"
        )

    ship(wait_for_ship_it.output)


ship_when_labeled()
