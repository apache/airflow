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
Refused when parsed: a PartitionedAssetTimetable cannot schedule on a WebhookTopic.

Topic events carry no partition key, so such a Dag would never run. The Dag processor records an import
error for this file instead of the Dag.
"""

from __future__ import annotations

from airflow.sdk import PartitionedAssetTimetable, WebhookTopic, dag, task

preview_requested = WebhookTopic(
    webhook="github",
    path="my-org/web",
    event="pull_request",
    action="labeled",
    detail="deploy-preview",
)


@dag(schedule=PartitionedAssetTimetable(assets=preview_requested), catchup=False)
def partitioned_topic_rejected():
    @task
    def deploy():
        print("Never runs")

    deploy()


partitioned_topic_rejected()
