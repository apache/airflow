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
from __future__ import annotations

import pytest

from airflow.providers.google.cloud.triggers.pubsub import PubsubPullTrigger

TEST_POLL_INTERVAL = 10
TEST_GCP_CONN_ID = "google_cloud_default"
PROJECT_ID = "test_project_id"
MAX_MESSAGES = 5
ACK_MESSAGES = True


@pytest.fixture
def trigger():
    return PubsubPullTrigger(
        project_id=PROJECT_ID,
        subscription="subscription",
        max_messages=MAX_MESSAGES,
        ack_messages=ACK_MESSAGES,
        messages_callback=None,
        poke_interval=TEST_POLL_INTERVAL,
        gcp_conn_id=TEST_GCP_CONN_ID,
        impersonation_chain=None,
    )


class TestPubsubPullTrigger:
    def test_async_pubsub_pull_trigger_serialization_should_execute_successfully(self, trigger):
        """
        Asserts that the PubsubPullTrigger correctly serializes its arguments
        and classpath.
        """
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.google.cloud.triggers.pubsub.PubsubPullTrigger"
        assert kwargs == {
            "project_id": PROJECT_ID,
            "subscription": "subscription",
            "max_messages": MAX_MESSAGES,
            "ack_messages": ACK_MESSAGES,
            "messages_callback": None,
            "poke_interval": TEST_POLL_INTERVAL,
            "gcp_conn_id": TEST_GCP_CONN_ID,
            "impersonation_chain": None,
        }
