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

import errno
import io
import os
import shutil
import sys
import unittest
from tempfile import mkdtemp
from unittest import mock

import boto3
from moto import mock_s3

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.operators.s3_file_transform import S3FileTransformOperator


class TestS3FileTransformOperator(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = mkdtemp(prefix='test_tmpS3FileTransform_')
        self.transform_script = os.path.join(self.tmp_dir, "transform.py")
        os.mknod(self.transform_script)

    def tearDown(self):
        try:
            shutil.rmtree(self.tmp_dir)
        except OSError as e:
            # ENOENT - no such file or directory
            if e.errno != errno.ENOENT:
                raise e

    @mock.patch('subprocess.Popen')
    @mock.patch.object(S3FileTransformOperator, 'log')
    @mock_s3
    def test_execute_with_transform_script(self, mock_log, mock_popen):
        process_output = [b"Foo", b"Bar", b"Baz"]

        process = mock_popen.return_value
        process.stdout.readline.side_effect = process_output
        process.wait.return_value = None
        process.returncode = 0

        bucket = "bucket"
        input_key = "foo"
        output_key = "bar"
        bio = io.BytesIO(b"input")

        conn = boto3.client('s3')
        conn.create_bucket(Bucket=bucket)
        conn.upload_fileobj(Bucket=bucket, Key=input_key, Fileobj=bio)

        s3_url = "s3://{0}/{1}"
        op = S3FileTransformOperator(
            source_s3_key=s3_url.format(bucket, input_key),
            dest_s3_key=s3_url.format(bucket, output_key),
            transform_script=self.transform_script,
            replace=True,
            task_id="task_id")
        op.execute(None)

        mock_log.info.assert_has_calls([
            mock.call(line.decode(sys.getdefaultencoding())) for line in process_output
        ])

    @mock.patch('subprocess.Popen')
    @mock_s3
    def test_execute_with_failing_transform_script(self, mock_popen):
        process = mock_popen.return_value
        process.stdout.readline.side_effect = []
        process.wait.return_value = None
        process.returncode = 42

        bucket = "bucket"
        input_key = "foo"
        output_key = "bar"
        bio = io.BytesIO(b"input")

        conn = boto3.client('s3')
        conn.create_bucket(Bucket=bucket)
        conn.upload_fileobj(Bucket=bucket, Key=input_key, Fileobj=bio)

        s3_url = "s3://{0}/{1}"
        op = S3FileTransformOperator(
            source_s3_key=s3_url.format(bucket, input_key),
            dest_s3_key=s3_url.format(bucket, output_key),
            transform_script=self.transform_script,
            replace=True,
            task_id="task_id")

        with self.assertRaises(AirflowException) as e:
            op.execute(None)

        self.assertEqual('Transform script failed: 42', str(e.exception))

    @mock.patch('airflow.providers.amazon.aws.hooks.s3.S3Hook.select_key', return_value="input")
    @mock_s3
    def test_execute_with_select_expression(self, mock_select_key):
        bucket = "bucket"
        input_key = "foo"
        output_key = "bar"
        bio = io.BytesIO(b"input")

        conn = boto3.client('s3')
        conn.create_bucket(Bucket=bucket)
        conn.upload_fileobj(Bucket=bucket, Key=input_key, Fileobj=bio)

        s3_url = "s3://{0}/{1}"
        select_expression = "SELECT * FROM S3Object s"
        op = S3FileTransformOperator(
            source_s3_key=s3_url.format(bucket, input_key),
            dest_s3_key=s3_url.format(bucket, output_key),
            select_expression=select_expression,
            replace=True,
            task_id="task_id")
        op.execute(None)

        mock_select_key.assert_called_once_with(
            key=s3_url.format(bucket, input_key),
            expression=select_expression
        )
