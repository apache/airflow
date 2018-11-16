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
import json
import datetime
import unittest

from airflow.exceptions import AirflowException
from airflow.contrib.hooks.gcp_transfer_hook import GCPTransferServiceHook
from airflow.contrib.hooks.gcp_transfer_hook import TIME_TO_SLEEP_IN_SECONDS

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None


class TestGCPTransferServiceHook(unittest.TestCase):
    def setUp(self):
        with mock.patch.object(GCPTransferServiceHook, '__init__', return_value=None):
            self.conn = mock.Mock()
            self.transfer_hook = GCPTransferServiceHook()
            self.transfer_hook._conn = self.conn

    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.GCPTransferServiceHook.wait_for_transfer_job')
    def test_create_transfer_job(self, mock_wait):
        mock_create = self.conn.transferJobs.return_value.create
        mock_execute = mock_create.return_value.execute
        mock_execute.return_value = {
            'projectId': 'test-project',
            'name': 'transferJobs/test-job',
        }
        now = datetime.datetime.utcnow()
        transfer_spec = {
            'awsS3DataSource': {'bucketName': 'test-s3-bucket'},
            'gcsDataSink': {'bucketName': 'test-gcs-bucket'}
        }
        self.transfer_hook.create_transfer_job('test-project', transfer_spec)
        mock_create.assert_called_once_with(body={
            'status': 'ENABLED',
            'projectId': 'test-project',
            'transferSpec': transfer_spec,
            'schedule': {
                'scheduleStartDate': {
                    'day': now.day,
                    'month': now.month,
                    'year': now.year,
                },
                'scheduleEndDate': {
                    'day': now.day,
                    'month': now.month,
                    'year': now.year,
                }
            }
        })
        mock_wait.assert_called_once_with(mock_execute.return_value, conn=self.conn)

    @mock.patch('time.sleep')
    def test_wait_for_transfer_job(self, mock_sleep):
        mock_list = self.conn.transferOperations.return_value.list
        mock_execute = mock_list.return_value.execute
        mock_execute.side_effect = [
            {'operations': [{'metadata': {'status': 'IN_PROGRESS'}}]},
            {'operations': [{'metadata': {'status': 'SUCCESS'}}]},
        ]
        self.transfer_hook.wait_for_transfer_job({
            'projectId': 'test-project',
            'name': 'transferJobs/test-job',
        })
        self.assertTrue(mock_list.called)
        list_args, list_kwargs = mock_list.call_args_list[0]
        self.assertEqual(list_kwargs.get('name'), 'transferOperations')
        self.assertEqual(
            json.loads(list_kwargs.get('filter')),
            {
                'project_id': 'test-project',
                'job_names': ['transferJobs/test-job']
            },
        )
        mock_sleep.assert_called_once_with(TIME_TO_SLEEP_IN_SECONDS)

    def test_wait_for_transfer_job_failed(self):
        mock_list = self.conn.transferOperations.return_value.list
        mock_execute = mock_list.return_value.execute
        mock_execute.side_effect = [
            {'operations': [{'name': 'test-job', 'metadata': {'status': 'FAILED'}}]},
        ]
        with self.assertRaises(AirflowException):
            self.transfer_hook.wait_for_transfer_job({
                'projectId': 'test-project',
                'name': 'transferJobs/test-job',
            })
