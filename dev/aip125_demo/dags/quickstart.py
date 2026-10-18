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
The proof of concept's quick start Dag, unchanged but for this docstring and a comment.

It runs when the ``demo`` webhook receives a ``deployed`` event for the ``billing`` service. It schedules
on a plain ``Asset`` named like the topic, which core keeps matching, so Dags written for the proof of
concept need no change.
"""

from __future__ import annotations

from airflow.sdk import Asset, dag, task

billing_deployed = Asset("demo/billing/deployed")


@dag(schedule=billing_deployed)
def smoke_test_billing():
    @task
    def smoke_test(*, triggering_asset_events=None):
        # Each delivery starts its own run unless [scheduler] batch_asset_events is set.
        for event in triggering_asset_events[billing_deployed]:
            print(f"Smoke-testing billing after deployment: {event.extra['payload']}")

    smoke_test()


smoke_test_billing()
