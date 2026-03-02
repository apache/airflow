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
from moto import mock_aws

from airflow.providers.amazon.aws.hooks.eventbridge import EventBridgeHook


@mock_aws
class TestEventBridgeHook:
    def test_conn_returns_a_boto3_connection(self):
        hook = EventBridgeHook(aws_conn_id="aws_default")
        assert hook.conn is not None

    def test_put_rule(self):
        hook = EventBridgeHook(aws_conn_id="aws_default")
        response = hook.put_rule(
            name="test",
            event_pattern='{"source": ["aws.s3"]}',
            state="ENABLED",
        )
        assert "RuleArn" in response

    def test_put_rule_with_bad_json_fails(self):
        hook = EventBridgeHook(aws_conn_id="aws_default")
        with pytest.raises(ValueError, match="`event_pattern` must be a valid JSON string."):
            hook.put_rule(
                name="test",
                event_pattern="invalid json",
                state="ENABLED",
            )
