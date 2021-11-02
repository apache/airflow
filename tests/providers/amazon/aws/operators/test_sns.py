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
#

import unittest
from unittest import mock

from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator

TASK_ID = "sns_publish_job"
AWS_CONN_ID = "custom_aws_conn"
TARGET_ARN = "arn:aws:sns:eu-central-1:1234567890:test-topic"
MESSAGE = "Message to send"
SUBJECT = "Subject to send"
MESSAGE_ATTRIBUTES = {"test-attribute": "Attribute to send"}


class TestSnsPublishOperator(unittest.TestCase):
    def test_init(self):
        # Given / When
        operator = SnsPublishOperator(
            task_id=TASK_ID,
            aws_conn_id=AWS_CONN_ID,
            target_arn=TARGET_ARN,
            message=MESSAGE,
            subject=SUBJECT,
            message_attributes=MESSAGE_ATTRIBUTES,
        )

        # Then
        assert TASK_ID == operator.task_id
        assert AWS_CONN_ID == operator.aws_conn_id
        assert TARGET_ARN == operator.target_arn
        assert MESSAGE == operator.message
        assert SUBJECT == operator.subject
        assert MESSAGE_ATTRIBUTES == operator.message_attributes

    @mock.patch('airflow.providers.amazon.aws.operators.sns.AwsSnsHook')
    def test_execute(self, mock_hook):
        # Given
        hook_response = {'MessageId': 'foobar'}

        hook_instance = mock_hook.return_value
        hook_instance.publish_to_target.return_value = hook_response

        operator = SnsPublishOperator(
            task_id=TASK_ID,
            aws_conn_id=AWS_CONN_ID,
            target_arn=TARGET_ARN,
            message=MESSAGE,
            subject=SUBJECT,
            message_attributes=MESSAGE_ATTRIBUTES,
        )

        # When
        result = operator.execute(None)

        # Then
        assert hook_response == result
