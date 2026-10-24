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
Tear down a pull request's preview once it is closed; demo.sh leaves it paused at first.

A GitHub delivery about a pull request has a correlation key, so when only paused Dags subscribe to it, it
records nothing and is kept early for a webhook wait, rather than recorded as ``dag_paused``. Once the Dag
is unpaused, GitHub's redelivery of it is processed again in full and starts the Dag.
"""

from __future__ import annotations

from airflow.sdk import WebhookTopic, dag, task

preview_closed = WebhookTopic(webhook="github", path="my-org/web", event="pull_request", action="closed")


@dag(schedule=preview_closed, catchup=False)
def teardown_preview():
    @task
    def tear_down(*, triggering_asset_events):
        for event in triggering_asset_events[preview_closed]:
            print(
                f"Tearing down the preview of pull request {event.extra['payload']['pull_request']['number']}"
            )

    tear_down()


teardown_preview()
