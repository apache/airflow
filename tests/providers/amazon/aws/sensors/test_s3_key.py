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

import unittest
from unittest import mock

from parameterized import parameterized

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor


class TestS3KeySensor(unittest.TestCase):

    def test_bucket_name_none_and_bucket_key_as_relative_path(self):
        """
        Test if exception is raised when bucket_name is None
        and bucket_key is provided as relative path rather than s3:// url.
        :return:
        """
        with self.assertRaises(AirflowException):
            S3KeySensor(
                task_id='s3_key_sensor',
                bucket_key="file_in_bucket")

    def test_bucket_name_provided_and_bucket_key_is_s3_url(self):
        """
        Test if exception is raised when bucket_name is provided
        while bucket_key is provided as a full s3:// url.
        :return:
        """
        with self.assertRaises(AirflowException):
            S3KeySensor(
                task_id='s3_key_sensor',
                bucket_key="s3://test_bucket/file",
                bucket_name='test_bucket')

    @parameterized.expand([
        ['s3://bucket/key', None, 'key', 'bucket'],
        ['key', 'bucket', 'key', 'bucket'],
    ])
    def test_parse_bucket_key(self, key, bucket, parsed_key, parsed_bucket):
        op = S3KeySensor(
            task_id='s3_key_sensor',
            bucket_key=key,
            bucket_name=bucket,
        )
        self.assertEqual(op.bucket_key, parsed_key)
        self.assertEqual(op.bucket_name, parsed_bucket)

    @mock.patch('airflow.providers.amazon.aws.hooks.s3.S3Hook')
    def test_poke(self, mock_hook):
        op = S3KeySensor(
            task_id='s3_key_sensor',
            bucket_key='s3://test_bucket/file')

        mock_check_for_key = mock_hook.return_value.check_for_key
        mock_check_for_key.return_value = False
        self.assertFalse(op.poke(None))
        mock_check_for_key.assert_called_once_with(op.bucket_key, op.bucket_name)

        mock_hook.return_value.check_for_key.return_value = True
        self.assertTrue(op.poke(None))

    @mock.patch('airflow.providers.amazon.aws.hooks.s3.S3Hook')
    def test_poke_wildcard(self, mock_hook):
        op = S3KeySensor(
            task_id='s3_key_sensor',
            bucket_key='s3://test_bucket/file',
            wildcard_match=True)

        mock_check_for_wildcard_key = mock_hook.return_value.check_for_wildcard_key
        mock_check_for_wildcard_key.return_value = False
        self.assertFalse(op.poke(None))
        mock_check_for_wildcard_key.assert_called_once_with(op.bucket_key, op.bucket_name)

        mock_check_for_wildcard_key.return_value = True
        self.assertTrue(op.poke(None))
