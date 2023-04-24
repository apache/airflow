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
from typing import Any

from airflow.models import Connection

# Import Operator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.utils import db

log = logging.getLogger(__name__)


def _no_op(*args, **kwargs) -> Any:
    """no_op A function that returns its arguments

    :return: whatever was passed in
    :rtype: Any
    """
    return args, kwargs


class TestConsumeFromTopic:
    """
    Test ConsumeFromTopic
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

    def test_operator(self):

        operator = ConsumeFromTopicOperator(
            kafka_config_id="kafka_d",
            topics=["test"],
            apply_function="tests.providers.apache.kafka.operators.test_consume._no_op",
            task_id="test",
            poll_timeout=0.0001,
        )

        # execute the operator (this is essentially a no op as the broker isn't setup)
        operator.execute(context={})

    def test_operator_callable(self):

        operator = ConsumeFromTopicOperator(
            kafka_config_id="kafka_d",
            topics=["test"],
            apply_function=_no_op,
            task_id="test",
            poll_timeout=0.0001,
        )

        # execute the operator (this is essentially a no op as the broker isn't setup)
        operator.execute(context={})
