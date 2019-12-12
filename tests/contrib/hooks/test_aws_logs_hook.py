# -*- coding: utf-8 -*-
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

from airflow.contrib.hooks.aws_logs_hook import AwsLogsHook

try:
    from moto import mock_logs
except ImportError:
    mock_logs = None


class TestAwsLogsHook(unittest.TestCase):

    @unittest.skipIf(mock_logs is None, 'mock_logs package not present')
    @mock_logs
    def test_get_conn_returns_a_boto3_connection(self):
        hook = AwsLogsHook(aws_conn_id='aws_default',
                           region_name="us-east-1")
        self.assertIsNotNone(hook.get_conn())

    @unittest.skipIf(mock_logs is None, 'mock_logs package not present')
    # moto.logs does not support proper pagination so we cannot test that yet
    # https://github.com/spulec/moto/issues/2259
    @mock_logs
    def test_get_log_events(self):
        log_group_name = 'example-group'
        log_stream_name = 'example-log-stream'

        hook = AwsLogsHook(aws_conn_id='aws_default',
                           region_name="us-east-1")

        # First we create some log events
        conn = hook.get_conn()
        conn.create_log_group(logGroupName=log_group_name)
        conn.create_log_stream(logGroupName=log_group_name, logStreamName=log_stream_name)

        input_events = [
            {
                'timestamp': 1,
                'message': 'Test Message 1'
            }
        ]

        conn.put_log_events(logGroupName=log_group_name,
                            logStreamName=log_stream_name,
                            logEvents=input_events)

        events = hook.get_log_events(
            log_group=log_group_name,
            log_stream_name=log_stream_name
        )

        # Iterate through entire generator
        events = list(events)
        count = len(events)

        assert count == 1
        assert events[0]['timestamp'] == input_events[0]['timestamp']
        assert events[0]['message'] == input_events[0]['message']


if __name__ == '__main__':
    unittest.main()
