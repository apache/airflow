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
from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook
from airflow.utils import db

pytestmark = pytest.mark.db_test


log = logging.getLogger(__name__)


class TestProducerHook:
    """
    Test consumer hook.
    """

    def setup_method(self):
        db.merge_conn(
            Connection(
                conn_id="kafka_d",
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
                extra=json.dumps({}),
            )
        )

    def test_init(self):
        """test initialization of AdminClientHook"""

        # Standard Init
        KafkaProducerHook(kafka_config_id="kafka_d")

        # Not Enough Args
        with pytest.raises(ValueError):
            KafkaProducerHook(kafka_config_id="kafka_bad")
