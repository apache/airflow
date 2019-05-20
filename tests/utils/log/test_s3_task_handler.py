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

from unittest import mock
import unittest
import os

from airflow import configuration
from airflow.utils.log.s3_task_handler import S3TaskHandler
from airflow.utils.state import State
from airflow.utils.timezone import datetime
from airflow.hooks.S3_hook import S3Hook
from airflow.models import TaskInstance, DAG
from airflow.operators.dummy_operator import DummyOperator

try:
    import boto3
    import moto
    from moto import mock_s3
except ImportError:
    mock_s3 = None


@unittest.skipIf(mock_s3 is None,
                 "Skipping test because moto.mock_s3 is not available")
@mock_s3
class TestS3TaskHandler(unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.remote_log_base = 's3://bucket/remote/log/location'
        self.remote_log_location = 's3://bucket/remote/log/location/1.log'
        self.remote_log_key = 'remote/log/location/1.log'
        self.local_log_location = 'local/log/location'
        self.filename_template = '{try_number}.log'
        self.s3_task_handler = S3TaskHandler(
            self.local_log_location,
            self.remote_log_base,
            self.filename_template
        )

        configuration.load_test_config()
        date = datetime(2016, 1, 1)
        self.dag = DAG('dag_for_testing_file_task_handler', start_date=date)
        task = DummyOperator(task_id='task_for_testing_file_log_handler', dag=self.dag)
        self.ti = TaskInstance(task=task, execution_date=date)
        self.ti.try_number = 1
        self.ti.state = State.RUNNING
        self.addCleanup(self.dag.clear)

        self.conn = boto3.client('s3')
        # We need to create the bucket since this is all in Moto's 'virtual'
        # AWS account
        moto.core.moto_api_backend.reset()
        self.conn.create_bucket(Bucket="bucket")

    def tearDown(self):
        if self.s3_task_handler.handler:
            try:
                os.remove(self.s3_task_handler.handler.baseFilename)
            except Exception:
                pass

    def test_hook(self):
        self.assertIsInstance(self.s3_task_handler.hook, S3Hook)

    def test_hook_raises(self):
        handler = self.s3_task_handler
        with mock.patch.object(handler.log, 'error') as mock_error:
            with mock.patch("airflow.hooks.S3_hook.S3Hook") as mock_hook:
                mock_hook.side_effect = Exception('Failed to connect')
                # Initialize the hook
                handler.hook

            mock_error.assert_called_once_with(
                'Could not create an S3Hook with connection id "%s". Please make '
                'sure that airflow[aws] is installed and the S3 connection exists.',
                ''
            )

    def test_log_exists(self):
        self.conn.put_object(Bucket='bucket', Key=self.remote_log_key, Body=b'')
        self.assertTrue(self.s3_task_handler.s3_log_exists(self.remote_log_location))

    def test_log_exists_none(self):
        self.assertFalse(self.s3_task_handler.s3_log_exists(self.remote_log_location))

    def test_log_exists_raises(self):
        self.assertFalse(self.s3_task_handler.s3_log_exists('s3://nonexistentbucket/foo'))

    def test_log_exists_no_hook(self):
        with mock.patch("airflow.hooks.S3_hook.S3Hook") as mock_hook:
            mock_hook.side_effect = Exception('Failed to connect')
            self.assertFalse(self.s3_task_handler.s3_log_exists(self.remote_log_location))

    def test_read(self):
        self.conn.put_object(Bucket='bucket', Key=self.remote_log_key, Body=b'Log line\n')
        self.assertEqual(
            self.s3_task_handler.read(self.ti),
            (['*** Reading remote log from s3://bucket/remote/log/location/1.log.\n'
             'Log line\n\n'], [{'end_of_log': True}])
        )

    def test_read_when_s3_log_missing(self):
        log, metadata = self.s3_task_handler.read(self.ti)

        self.assertEqual(1, len(log))
        self.assertEqual(len(log), len(metadata))
        self.assertIn('*** Log file does not exist:', log[0])
        self.assertEqual({'end_of_log': True}, metadata[0])

    def test_read_raises_return_error(self):
        handler = self.s3_task_handler
        url = 's3://nonexistentbucket/foo'
        with mock.patch.object(handler.log, 'error') as mock_error:
            result = handler.s3_read(url, return_error=True)
            msg = 'Could not read logs from %s' % url
            self.assertEqual(result, msg)
            mock_error.assert_called_once_with(msg, exc_info=True)

    def test_write(self):
        with mock.patch.object(self.s3_task_handler.log, 'error') as mock_error:
            self.s3_task_handler.s3_write('text', self.remote_log_location)
            # We shouldn't expect any error logs in the default working case.
            mock_error.assert_not_called()
        body = boto3.resource('s3').Object('bucket', self.remote_log_key).get()['Body'].read()

        self.assertEqual(body, b'text')

    def test_write_existing(self):
        self.conn.put_object(Bucket='bucket', Key=self.remote_log_key, Body=b'previous ')
        self.s3_task_handler.s3_write('text', self.remote_log_location)
        body = boto3.resource('s3').Object('bucket', self.remote_log_key).get()['Body'].read()

        self.assertEqual(body, b'previous \ntext')

    def test_write_raises(self):
        handler = self.s3_task_handler
        url = 's3://nonexistentbucket/foo'
        with mock.patch.object(handler.log, 'error') as mock_error:
            handler.s3_write('text', url)
            self.assertEqual
            mock_error.assert_called_once_with(
                'Could not write logs to %s', url, exc_info=True)

    def test_close(self):
        self.s3_task_handler.set_context(self.ti)
        self.assertTrue(self.s3_task_handler.upload_on_close)

        self.s3_task_handler.close()
        # Should not raise
        boto3.resource('s3').Object('bucket', self.remote_log_key).get()

    def test_close_no_upload(self):
        self.ti.raw = True
        self.s3_task_handler.set_context(self.ti)
        self.assertFalse(self.s3_task_handler.upload_on_close)
        self.s3_task_handler.close()

        with self.assertRaises(self.conn.exceptions.NoSuchKey):
            boto3.resource('s3').Object('bucket', self.remote_log_key).get()
