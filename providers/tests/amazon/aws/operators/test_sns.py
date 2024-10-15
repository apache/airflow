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

from unittest import mock

import pytest

from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator

from providers.tests.amazon.aws.utils.test_template_fields import validate_template_fields

TASK_ID = "sns_publish_job"
AWS_CONN_ID = "custom_aws_conn"
TARGET_ARN = "arn:aws:sns:eu-central-1:1234567890:test-topic"
MESSAGE = "Message to send"
SUBJECT = "Subject to send"
MESSAGE_ATTRIBUTES = {"test-attribute": "Attribute to send"}


class TestSnsPublishOperator:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self):
        self.default_op_kwargs = {
            "task_id": TASK_ID,
            "target_arn": TARGET_ARN,
            "message": MESSAGE,
            "subject": SUBJECT,
            "message_attributes": MESSAGE_ATTRIBUTES,
        }

    def test_init(self):
        op = SnsPublishOperator(**self.default_op_kwargs)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

        op = SnsPublishOperator(
            **self.default_op_kwargs,
            aws_conn_id=AWS_CONN_ID,
            region_name="us-west-1",
            verify="/spam/egg.pem",
            botocore_config={"read_timeout": 42},
        )
        assert op.hook.aws_conn_id == AWS_CONN_ID
        assert op.hook._region_name == "us-west-1"
        assert op.hook._verify == "/spam/egg.pem"
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42

    @mock.patch.object(SnsPublishOperator, "hook")
    def test_execute(self, mocked_hook):
        hook_response = {"MessageId": "foobar"}
        mocked_hook.publish_to_target.return_value = hook_response

        op = SnsPublishOperator(**self.default_op_kwargs)
        assert op.execute({}) == hook_response

        mocked_hook.publish_to_target.assert_called_once_with(
            message=MESSAGE,
            message_attributes=MESSAGE_ATTRIBUTES,
            subject=SUBJECT,
            target_arn=TARGET_ARN,
        )

    def test_template_fields(self):
        operator = SnsPublishOperator(**self.default_op_kwargs)
        validate_template_fields(operator)
