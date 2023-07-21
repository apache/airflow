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

from airflow.providers.amazon.aws.triggers.sqs import SqsSensorTrigger

TEST_SQS_QUEUE = "test-sqs-queue"
TEST_AWS_CONN_ID = "test-aws-conn-id"
TEST_MAX_MESSAGES = 1
TEST_NUM_BATCHES = 1
TEST_WAIT_TIME_SECONDS = 1
TEST_VISIBILITY_TIMEOUT = 1
TEST_MESSAGE_FILTERING = "literal"
TEST_MESSAGE_FILTERING_MATCH_VALUES = "test-message-filtering-match-values"
TEST_MESSAGE_FILTERING_CONFIG = "test-message-filtering-config"
TEST_DELETE_MESSAGE_ON_RECEPTION = False
TEST_WAITER_DELAY = 1


class TestSqsTriggers:
    @pytest.mark.parametrize(
        "trigger",
        [
            SqsSensorTrigger(
                sqs_queue=TEST_SQS_QUEUE,
                aws_conn_id=TEST_AWS_CONN_ID,
                max_messages=TEST_MAX_MESSAGES,
                num_batches=TEST_NUM_BATCHES,
                wait_time_seconds=TEST_WAIT_TIME_SECONDS,
                visibility_timeout=TEST_VISIBILITY_TIMEOUT,
                message_filtering=TEST_MESSAGE_FILTERING,
                message_filtering_match_values=TEST_MESSAGE_FILTERING_MATCH_VALUES,
                message_filtering_config=TEST_MESSAGE_FILTERING_CONFIG,
                delete_message_on_reception=TEST_DELETE_MESSAGE_ON_RECEPTION,
                waiter_delay=TEST_WAITER_DELAY,
            ),
        ],
    )
    def test_serialize_recreate(self, trigger):
        class_path, args = trigger.serialize()

        class_name = class_path.split(".")[-1]
        clazz = globals()[class_name]
        instance = clazz(**args)

        class_path2, args2 = instance.serialize()

        assert class_path == class_path2
        assert args == args2
