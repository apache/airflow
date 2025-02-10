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

import pytest
from moto import mock_aws

from airflow.providers.amazon.aws.hooks.sns import SnsHook


@mock_aws
class TestSnsHook:
    def test_get_conn_returns_a_boto3_connection(self):
        hook = SnsHook(aws_conn_id="aws_default")
        assert hook.get_conn() is not None

    def test_publish_to_target_with_subject(self):
        hook = SnsHook(aws_conn_id="aws_default")

        message = "Hello world"
        topic_name = "test-topic"
        subject = "test-subject"
        target = hook.get_conn().create_topic(Name=topic_name).get("TopicArn")

        response = hook.publish_to_target(target, message, subject)

        assert "MessageId" in response

    def test_publish_to_target_with_attributes(self):
        hook = SnsHook(aws_conn_id="aws_default")

        message = "Hello world"
        topic_name = "test-topic"
        target = hook.get_conn().create_topic(Name=topic_name).get("TopicArn")

        response = hook.publish_to_target(
            target,
            message,
            message_attributes={
                "test-string": "string-value",
                "test-number": 123456,
                "test-array": ["first", "second", "third"],
                "test-binary": b"binary-value",
            },
        )

        assert "MessageId" in response

    def test_publish_to_target_plain(self):
        hook = SnsHook(aws_conn_id="aws_default")

        message = "Hello world"
        topic_name = "test-topic"
        target = hook.get_conn().create_topic(Name=topic_name).get("TopicArn")

        response = hook.publish_to_target(target, message)

        assert "MessageId" in response

    def test_publish_to_target_error(self):
        hook = SnsHook(aws_conn_id="aws_default")

        message = "Hello world"
        topic_name = "test-topic"
        target = hook.get_conn().create_topic(Name=topic_name).get("TopicArn")

        error_message = (
            r"Values in MessageAttributes must be one of bytes, str, int, float, or iterable; got .*"
        )
        with pytest.raises(TypeError, match=error_message):
            hook.publish_to_target(
                target,
                message,
                message_attributes={
                    "test-non-iterable": object(),
                },
            )
