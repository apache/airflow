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

from airflow.providers.amazon.aws.hooks.sns import AwsSnsHook

try:
    from moto import mock_sns
except ImportError:
    mock_sns = None


@unittest.skipIf(mock_sns is None, 'moto package not present')
class TestAwsSnsHook(unittest.TestCase):

    @mock_sns
    def test_get_conn_returns_a_boto3_connection(self):
        hook = AwsSnsHook(aws_conn_id='aws_default')
        self.assertIsNotNone(hook.get_conn())

    @mock_sns
    def test_publish_to_target(self):
        hook = AwsSnsHook(aws_conn_id='aws_default')

        message = "Hello world"
        topic_name = "test-topic"
        subject = "test-subject"
        target = hook.get_conn().create_topic(Name=topic_name).get('TopicArn')

        response = hook.publish_to_target(target, message, subject)

        self.assertTrue('MessageId' in response)

    @mock_sns
    def test_publish_to_target_without_subject(self):
        hook = AwsSnsHook(aws_conn_id='aws_default')

        message = "Hello world"
        topic_name = "test-topic"
        target = hook.get_conn().create_topic(Name=topic_name).get('TopicArn')

        response = hook.publish_to_target(target, message)

        self.assertTrue('MessageId' in response)


if __name__ == '__main__':
    unittest.main()
