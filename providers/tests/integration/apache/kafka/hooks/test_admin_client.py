#
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

import json

import pytest

from airflow.models import Connection
from airflow.providers.apache.kafka.hooks.client import KafkaAdminClientHook
from airflow.utils import db

client_config = {
    "socket.timeout.ms": 1000,
    "bootstrap.servers": "broker:29092",
    "group.id": "my-group",
}


@pytest.mark.integration("kafka")
class TestKafkaAdminClientHook:
    def setup_method(self):
        db.merge_conn(
            Connection(
                conn_id="kafka_d",
                conn_type="kafka",
                extra=json.dumps(client_config),
            )
        )

    def test_hook(self):
        """test the creation of topics"""

        # Standard Init
        hook = KafkaAdminClientHook(kafka_config_id="kafka_d")
        hook.create_topic(topics=[("test_1", 1, 1), ("test_2", 1, 1)])

        kadmin = hook.get_conn
        t = kadmin.list_topics(timeout=10).topics
        assert t.get("test_2")
        hook.delete_topic(topics=["test_1", "test_2"])
