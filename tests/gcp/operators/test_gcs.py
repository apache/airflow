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

from airflow.gcp.operators.gcs import (
    GcsFileTransformOperator, GoogleCloudStorageBucketCreateAclEntryOperator,
    GoogleCloudStorageCreateBucketOperator, GoogleCloudStorageDeleteOperator,
    GoogleCloudStorageDownloadOperator, GoogleCloudStorageListOperator,
    GoogleCloudStorageObjectCreateAclEntryOperator,
)
from tests.compat import mock

TASK_ID = 'test-gcs-operator'
TEST_BUCKET = 'test-bucket'
TEST_PROJECT = 'test-project'
DELIMITER = '.csv'
PREFIX = 'TEST'
MOCK_FILES = ["TEST1.csv", "TEST2.csv", "TEST3.csv"]
TEST_OBJECT = 'dir1/test-object'
LOCAL_FILE_PATH = '/home/airflow/gcp/test-object'


class TestGoogleCloudStorageCreateBucket(unittest.TestCase):

    @mock.patch('airflow.gcp.operators.gcs.GoogleCloudStorageHook')
    def test_execute(self, mock_hook):
        operator = GoogleCloudStorageCreateBucketOperator(
            task_id=TASK_ID,
            bucket_name=TEST_BUCKET,
            resource={"lifecycle": {"rule": [{"action": {"type": "Delete"}, "condition": {"age": 7}}]}},
            storage_class='MULTI_REGIONAL',
            location='EU',
            labels={'env': 'prod'},
            project_id=TEST_PROJECT
        )

        operator.execute(None)
        mock_hook.return_value.create_bucket.assert_called_once_with(
            bucket_name=TEST_BUCKET, storage_class='MULTI_REGIONAL',
            location='EU', labels={'env': 'prod'},
            project_id=TEST_PROJECT,
            resource={'lifecycle': {'rule': [{'action': {'type': 'Delete'}, 'condition': {'age': 7}}]}}
        )


class TestGoogleCloudStorageAcl(unittest.TestCase):
    @mock.patch('airflow.gcp.operators.gcs.GoogleCloudStorageHook')
    def test_bucket_create_acl(self, mock_hook):
        operator = GoogleCloudStorageBucketCreateAclEntryOperator(
            bucket="test-bucket",
            entity="test-entity",
            role="test-role",
            user_project="test-user-project",
            task_id="id"
        )
        operator.execute(None)
        mock_hook.return_value.insert_bucket_acl.assert_called_once_with(
            bucket_name="test-bucket",
            entity="test-entity",
            role="test-role",
            user_project="test-user-project"
        )

    @mock.patch('airflow.gcp.operators.gcs.GoogleCloudStorageHook')
    def test_object_create_acl(self, mock_hook):
        operator = GoogleCloudStorageObjectCreateAclEntryOperator(
            bucket="test-bucket",
            object_name="test-object",
            entity="test-entity",
            generation=42,
            role="test-role",
            user_project="test-user-project",
            task_id="id"
        )
        operator.execute(None)
        mock_hook.return_value.insert_object_acl.assert_called_once_with(
            bucket_name="test-bucket",
            object_name="test-object",
            entity="test-entity",
            generation=42,
            role="test-role",
            user_project="test-user-project"
        )


class TestGoogleCloudStorageDeleteOperator(unittest.TestCase):
    @mock.patch('airflow.gcp.operators.gcs.GoogleCloudStorageHook')
    def test_delete_objects(self, mock_hook):
        operator = GoogleCloudStorageDeleteOperator(task_id=TASK_ID,
                                                    bucket_name=TEST_BUCKET,
                                                    objects=MOCK_FILES[0:2])

        operator.execute(None)
        mock_hook.return_value.list.assert_not_called()
        mock_hook.return_value.delete.assert_has_calls(
            calls=[
                mock.call(bucket_name=TEST_BUCKET, object_name=MOCK_FILES[0]),
                mock.call(bucket_name=TEST_BUCKET, object_name=MOCK_FILES[1])
            ],
            any_order=True
        )

    @mock.patch('airflow.gcp.operators.gcs.GoogleCloudStorageHook')
    def test_delete_prefix(self, mock_hook):
        mock_hook.return_value.list.return_value = MOCK_FILES[1:3]
        operator = GoogleCloudStorageDeleteOperator(task_id=TASK_ID,
                                                    bucket_name=TEST_BUCKET,
                                                    prefix=PREFIX)

        operator.execute(None)
        mock_hook.return_value.list.assert_called_once_with(
            bucket_name=TEST_BUCKET, prefix=PREFIX
        )
        mock_hook.return_value.delete.assert_has_calls(
            calls=[
                mock.call(bucket_name=TEST_BUCKET, object_name=MOCK_FILES[1]),
                mock.call(bucket_name=TEST_BUCKET, object_name=MOCK_FILES[2])
            ],
            any_order=True
        )


class TestGoogleCloudStorageDownloadOperator(unittest.TestCase):

    @mock.patch('airflow.gcp.operators.gcs.GoogleCloudStorageHook')
    def test_execute(self, mock_hook):
        operator = GoogleCloudStorageDownloadOperator(task_id=TASK_ID,
                                                      bucket=TEST_BUCKET,
                                                      object_name=TEST_OBJECT,
                                                      filename=LOCAL_FILE_PATH)

        operator.execute(None)
        mock_hook.return_value.download.assert_called_once_with(
            bucket_name=TEST_BUCKET, object_name=TEST_OBJECT, filename=LOCAL_FILE_PATH
        )


class TestGoogleCloudStorageListOperator(unittest.TestCase):

    @mock.patch('airflow.gcp.operators.gcs.GoogleCloudStorageHook')
    def test_execute(self, mock_hook):
        mock_hook.return_value.list.return_value = MOCK_FILES

        operator = GoogleCloudStorageListOperator(task_id=TASK_ID,
                                                  bucket=TEST_BUCKET,
                                                  prefix=PREFIX,
                                                  delimiter=DELIMITER)

        files = operator.execute(None)
        mock_hook.return_value.list.assert_called_once_with(
            bucket_name=TEST_BUCKET, prefix=PREFIX, delimiter=DELIMITER
        )
        self.assertEqual(sorted(files), sorted(MOCK_FILES))


class TestGcsFileTransformOperator(unittest.TestCase):
    @mock.patch('airflow.gcp.operators.gcs.NamedTemporaryFile')
    @mock.patch('airflow.gcp.operators.gcs.subprocess')
    @mock.patch('airflow.gcp.operators.gcs.GoogleCloudStorageHook')
    def test_execute(self, mock_hook, mock_subprocess, mock_tempfile):
        source_bucket = TEST_BUCKET
        source_object = "test.txt"
        destination_bucket = TEST_BUCKET + "-dest"
        destination_object = "transformed_test.txt"
        transform_script = "script.py"

        source = "source"
        destination = "destination"

        # Mock the name attribute...
        mock1 = mock.Mock()
        mock2 = mock.Mock()
        mock1.name = source
        mock2.name = destination

        mock_tempfile.return_value.__enter__.side_effect = [
            mock1,
            mock2
        ]

        mock_subprocess.PIPE = "pipe"
        mock_subprocess.STDOUT = "stdout"
        mock_subprocess.Popen.return_value.stdout.readline = lambda: b''
        mock_subprocess.Popen.return_value.wait.return_value = None
        mock_subprocess.Popen.return_value.returncode = 0

        op = GcsFileTransformOperator(
            task_id=TASK_ID,
            source_bucket=source_bucket,
            source_object=source_object,
            destination_object=destination_object,
            destination_bucket=destination_bucket,
            transform_script=transform_script,
        )
        op.execute(None)

        mock_hook.return_value.download.assert_called_once_with(
            bucket_name=source_bucket,
            object_name=source_object,
            filename=source
        )

        mock_subprocess.Popen.assert_called_once_with(
            args=[transform_script, source, destination],
            stdout="pipe",
            stderr="stdout",
            close_fds=True
        )

        mock_hook.return_value.upload.assert_called_with(
            bucket_name=destination_bucket,
            object_name=destination_object,
            filename=destination
        )
