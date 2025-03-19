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
from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger


class TestMessageQueueTrigger:
    @pytest.mark.parametrize(
        ("queue", "expected_trigger_class"),
        [
            ("https://sqs.us-east-1.amazonaws.com/0123456789/Test", SqsSensorTrigger),
        ],
    )
    def test_provider_integrations(self, queue, expected_trigger_class):
        trigger = MessageQueueTrigger(queue=queue)
        assert isinstance(trigger.trigger, expected_trigger_class)
