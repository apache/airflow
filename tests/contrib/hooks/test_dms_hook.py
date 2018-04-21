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
import boto3

from airflow import configuration
from airflow.contrib.hooks.dms_hook import DMSHook


try:
    from moto import mock_dms
except ImportError:
    mock_dms = None


class TestDMSHook(unittest.TestCase):
    @mock_dms
    def setUp(self):
        configuration.load_test_config()

    @unittest.skipIf(mock_dms is None, 'mock_dms package not present')
    @mock_dms
    def test_get_conn_returns_a_boto3_connection(self):
        hook = DMSHook(aws_conn_id='dms_default')
        self.assertIsNotNone(hook.get_conn().describe_replication_tasks())

    @unittest.skipIf(mock_dms is None, 'mock_dms package not present')
    @mock_dms
    def test_create_replication_task_uses_the_dms_config_to_create_a_task(self):
        client = boto3.client('dms', region_name='us-east-1')
        if len(client.describe_replication_tasks()['Clusters']):
            raise ValueError('AWS not properly mocked')

        hook = DMSHook(aws_conn_id='aws_default', dms_conn_id='dms_default')
        task = hook.create_replication_task({'Name': 'test_cluster'})

        self.assertEqual(client.describe_replication_tasks()['Clusters'][0]['Id'], task['JobFlowId'])

    @unittest.skipIf(mock_dms is None, 'mock_dms package not present')
    @mock_dms
    def test_stop_replication_task(self):
        hook = DMSHook(aws_conn_id='aws_default', dms_conn_id='dms_default')
        replication_task_arn = 'test'
        task = hook.stop_replication_task(replication_task_arn)

        # self.assertEqual(client.describe_replication_tasks()['Clusters'][0]['Id'], task['JobFlowId'])

    @unittest.skipIf(mock_dms is None, 'mock_dms package not present')
    @mock_dms
    def test_start_replication_task(self):
        hook = DMSHook(aws_conn_id='aws_default', dms_conn_id='dms_default')
        replication_task_arn = 'test'
        start_replication_task_type = 'full_load'
        task = hook.start_replication_task(replication_task_arn, start_replication_task_type)

        # self.assertEqual(client.describe_replication_tasks()['Clusters'][0]['Id'], task['JobFlowId'])

    @unittest.skipIf(mock_dms is None, 'mock_dms package not present')
    @mock_dms
    def test_delete_replication_task(self):
        hook = DMSHook(aws_conn_id='aws_default', dms_conn_id='dms_default')
        replication_task_arn = 'test'
        task = hook.delete_replication_task(replication_task_arn)

        # self.assertEqual(client.describe_replication_tasks()['Clusters'][0]['Id'], task['JobFlowId'])

    @unittest.skipIf(mock_dms is None, 'mock_dms package not present')
    @mock_dms
    def test_describe_replication_task(self):
        hook = DMSHook(aws_conn_id='aws_default', dms_conn_id='dms_default')
        replication_task_arn = 'test'
        task = hook.describe_replication_task(replication_task_arn)

        # self.assertEqual(client.describe_replication_tasks()['Clusters'][0]['Id'], task['JobFlowId'])

if __name__ == '__main__':
    unittest.main()
