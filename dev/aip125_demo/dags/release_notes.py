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
Publish release notes when billing is released; demo.sh leaves it paused at first.

A delivery that only paused Dags subscribe to, and that has no correlation key, records nothing and has
the outcome ``dag_paused``. Once the Dag is unpaused, the sender's redelivery of it is processed again and
starts the Dag.
"""

from __future__ import annotations

from airflow.sdk import WebhookTopic, dag, task

billing_released = WebhookTopic(webhook="demo", path="billing", event="released")


@dag(schedule=billing_released, catchup=False)
def release_notes():
    @task
    def publish(*, triggering_asset_events):
        for event in triggering_asset_events[billing_released]:
            print(f"Publishing the release notes of billing {event.extra['payload']['version']}")

    publish()


release_notes()
