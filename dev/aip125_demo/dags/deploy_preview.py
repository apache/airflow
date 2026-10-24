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
"""The example of AIP-125 section 3.4: deploy a preview when a pull request gets the deploy-preview label."""

from __future__ import annotations

from airflow.sdk import WebhookTopic, dag, task

preview_requested = WebhookTopic(
    webhook="github",
    path="my-org/web",
    event="pull_request",
    action="labeled",
    detail="deploy-preview",
)
# Plain Asset("github/my-org/web/pull_request/labeled/deploy-preview") stays supported.


@dag(schedule=preview_requested, catchup=False)
def deploy_preview():
    @task
    def deploy(*, triggering_asset_events):
        for event in triggering_asset_events[preview_requested]:
            # A payload over [webhooks] max_event_payload_bytes arrives as None, with payload_truncated set.
            print(f"Deploying a preview of pull request {event.extra['payload']['pull_request']['number']}")

    deploy()


deploy_preview()
