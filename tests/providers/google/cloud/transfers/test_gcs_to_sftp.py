#!/usr/bin/env python
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

import os
import unittest

import mock

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.transfers.gcs_to_sftp import GCSToSFTPOperator

TASK_ID = "test-gcs-to-sftp-operator"
GCP_CONN_ID = "GCP_CONN_ID"
SFTP_CONN_ID = "SFTP_CONN_ID"
DELEGATE_TO = "DELEGATE_TO"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]

TEST_BUCKET = "test-bucket"
SOURCE_OBJECT_WILDCARD_FILENAME = "test_object*.txt"
SOURCE_OBJECT_NO_WILDCARD = "test_object.txt"
SOURCE_OBJECT_MULTIPLE_WILDCARDS = "csv/*/test_*.csv"

SOURCE_FILES_LIST = [
    "test_object/file1.txt",
    "test_object/file2.txt",
    "test_object/file3.json",
]

DESTINATION_SFTP = "destination_path"


# pylint: disable=unused-argument
class TestGoogleCloudStorageToSFTPOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_sftp.GCSHook")
    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_sftp.SFTPHook")
    def test_execute_copy_single_file(self, sftp_hook, gcs_hook):
        task = GCSToSFTPOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_NO_WILDCARD,
            destination_path=DESTINATION_SFTP,
            move_object=False,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
            delegate_to=DELEGATE_TO,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        task.execute({})
        gcs_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=DELEGATE_TO,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        sftp_hook.assert_called_once_with(SFTP_CONN_ID)

        args, kwargs = gcs_hook.return_value.download.call_args
        self.assertEqual(kwargs["bucket_name"], TEST_BUCKET)
        self.assertEqual(kwargs["object_name"], SOURCE_OBJECT_NO_WILDCARD)

        args, kwargs = sftp_hook.return_value.store_file.call_args
        self.assertEqual(
            args[0], os.path.join(DESTINATION_SFTP, SOURCE_OBJECT_NO_WILDCARD)
        )

        gcs_hook.return_value.delete.assert_not_called()

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_sftp.GCSHook")
    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_sftp.SFTPHook")
    def test_execute_move_single_file(self, sftp_hook, gcs_hook):
        task = GCSToSFTPOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_NO_WILDCARD,
            destination_path=DESTINATION_SFTP,
            move_object=True,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
            delegate_to=DELEGATE_TO,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        task.execute(None)
        gcs_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=DELEGATE_TO,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        sftp_hook.assert_called_once_with(SFTP_CONN_ID)

        args, kwargs = gcs_hook.return_value.download.call_args
        self.assertEqual(kwargs["bucket_name"], TEST_BUCKET)
        self.assertEqual(kwargs["object_name"], SOURCE_OBJECT_NO_WILDCARD)

        args, kwargs = sftp_hook.return_value.store_file.call_args
        self.assertEqual(
            args[0], os.path.join(DESTINATION_SFTP, SOURCE_OBJECT_NO_WILDCARD)
        )

        gcs_hook.return_value.delete.assert_called_once_with(
            TEST_BUCKET, SOURCE_OBJECT_NO_WILDCARD
        )

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_sftp.GCSHook")
    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_sftp.SFTPHook")
    def test_execute_copy_with_wildcard(self, sftp_hook, gcs_hook):
        gcs_hook.return_value.list.return_value = SOURCE_FILES_LIST[:2]
        operator = GCSToSFTPOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_WILDCARD_FILENAME,
            destination_path=DESTINATION_SFTP,
            move_object=False,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
            delegate_to=DELEGATE_TO,
        )
        operator.execute(None)

        gcs_hook.return_value.list.assert_called_with(
            TEST_BUCKET, delimiter=".txt", prefix="test_object"
        )

        call_one, call_two = gcs_hook.return_value.download.call_args_list
        self.assertEqual(call_one[1]["bucket_name"], TEST_BUCKET)
        self.assertEqual(call_one[1]["object_name"], "test_object/file1.txt")

        self.assertEqual(call_two[1]["bucket_name"], TEST_BUCKET)
        self.assertEqual(call_two[1]["object_name"], "test_object/file2.txt")

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_sftp.GCSHook")
    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_sftp.SFTPHook")
    def test_execute_move_with_wildcard(self, sftp_hook, gcs_hook):
        gcs_hook.return_value.list.return_value = SOURCE_FILES_LIST[:2]
        operator = GCSToSFTPOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_WILDCARD_FILENAME,
            destination_path=DESTINATION_SFTP,
            move_object=True,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
            delegate_to=DELEGATE_TO,
        )
        operator.execute(None)

        gcs_hook.return_value.list.assert_called_with(
            TEST_BUCKET, delimiter=".txt", prefix="test_object"
        )

        call_one, call_two = gcs_hook.return_value.delete.call_args_list
        self.assertEqual(call_one[0], (TEST_BUCKET, "test_object/file1.txt"))
        self.assertEqual(call_two[0], (TEST_BUCKET, "test_object/file2.txt"))

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_sftp.GCSHook")
    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_sftp.SFTPHook")
    def test_execute_more_than_one_wildcard_exception(self, sftp_hook, gcs_hook):
        gcs_hook.return_value.list.return_value = SOURCE_FILES_LIST[:2]
        operator = GCSToSFTPOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_MULTIPLE_WILDCARDS,
            destination_path=DESTINATION_SFTP,
            move_object=False,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
            delegate_to=DELEGATE_TO,
        )
        with self.assertRaises(AirflowException):
            operator.execute(None)
