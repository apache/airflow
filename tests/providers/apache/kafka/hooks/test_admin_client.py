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
import logging

import pytest

from airflow.models import Connection
from airflow.providers.apache.kafka.hooks.client import KafkaAdminClientHook
from airflow.utils import db

log = logging.getLogger(__name__)


class TestSampleHook:
    """
    Test Admin Client Hook.
    """

    def setup_method(self):
        db.merge_conn(
            Connection(
                conn_id="kafka_default",
                conn_type="kafka",
                extra=json.dumps(
                    {"socket.timeout.ms": 10, "bootstrap.servers": "localhost:9092", "group.id": "test_group"}
                ),
            )
        )

        db.merge_conn(
            Connection(
                conn_id="kafka_bad",
                conn_type="kafka",
                extra=json.dumps({"socket.timeout.ms": 10}),
            )
        )

    def test_init(self):
        """test initialization of AdminClientHook"""

        # Standard Init
        KafkaAdminClientHook(kafka_config_id="kafka_default")

        # # Not Enough Args
        with pytest.raises(ValueError):
            KafkaAdminClientHook(kafka_config_id="kafka_bad")

    def test_get_conn(self):
        """test get_conn"""

        # Standard Init
        k = KafkaAdminClientHook(kafka_config_id="kafka_default")

        c = k.get_conn

        assert isinstance(c, dict)
        assert c.get("bootstrap.servers") == "localhost:9092"

    def test_create_topic(self):
        """test topic creation"""
        h = KafkaAdminClientHook(kafka_config_id="kafka_default")
        h.create_topic(topics=[("test_1", 3, 3), ("test_2", 1, 1)])
