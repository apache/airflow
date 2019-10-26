#!/usr/bin/env python
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

import os
import unittest

from airflow.exceptions import AirflowException
from airflow.operators.sftp_to_gcs import SFTPToGoogleCloudStorageOperator
from tests.compat import mock

TASK_ID = "test-gcs-to-sftp-operator"
GCP_CONN_ID = "GCP_CONN_ID"
SFTP_CONN_ID = "SFTP_CONN_ID"
DELEGATE_TO = "DELEGATE_TO"

TEST_BUCKET = "test-bucket"
SOURCE_OBJECT_WILDCARD_FILENAME = "main_dir/test_object*.json"
SOURCE_OBJECT_NO_WILDCARD = "main_dir/test_object3.json"
SOURCE_OBJECT_MULTIPLE_WILDCARDS = "main_dir/csv/*/test_*.csv"

SOURCE_FILES_LIST = [
    "main_dir/test_object1.txt",
    "main_dir/test_object2.txt",
    "main_dir/test_object3.json",
    "main_dir/sub_dir/test_object1.txt",
    "main_dir/sub_dir/test_object2.txt",
    "main_dir/sub_dir/test_object3.json",
]

DESTINATION_PATH_DIR = "destination_dir"
DESTINATION_PATH_FILE = "destination_dir/copy.txt"


# pylint: disable=unused-argument
class TestSFTPToGoogleCloudStorageOperator(unittest.TestCase):
    @mock.patch("airflow.operators.sftp_to_gcs.GoogleCloudStorageHook")
    @mock.patch("airflow.operators.sftp_to_gcs.SFTPHook")
    def test_execute_copy_single_file(self, sftp_hook, gcs_hook):
        task = SFTPToGoogleCloudStorageOperator(
            task_id=TASK_ID,
            source_path=SOURCE_OBJECT_NO_WILDCARD,
            destination_bucket=TEST_BUCKET,
            destination_path=DESTINATION_PATH_FILE,
            move_object=False,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
            delegate_to=DELEGATE_TO,
        )
        task.execute(None)
        gcs_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID, delegate_to=DELEGATE_TO
        )
        sftp_hook.assert_called_once_with(SFTP_CONN_ID)

        args, kwargs = sftp_hook.return_value.retrieve_file.call_args
        self.assertEqual(args[0], os.path.join(SOURCE_OBJECT_NO_WILDCARD))

        args, kwargs = gcs_hook.return_value.upload.call_args
        self.assertEqual(kwargs["bucket_name"], TEST_BUCKET)
        self.assertEqual(kwargs["object_name"], DESTINATION_PATH_FILE)

        sftp_hook.return_value.delete_file.assert_not_called()

    @mock.patch("airflow.operators.sftp_to_gcs.GoogleCloudStorageHook")
    @mock.patch("airflow.operators.sftp_to_gcs.SFTPHook")
    def test_execute_move_single_file(self, sftp_hook, gcs_hook):
        task = SFTPToGoogleCloudStorageOperator(
            task_id=TASK_ID,
            source_path=SOURCE_OBJECT_NO_WILDCARD,
            destination_bucket=TEST_BUCKET,
            destination_path=DESTINATION_PATH_FILE,
            move_object=True,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
            delegate_to=DELEGATE_TO,
        )
        task.execute(None)
        gcs_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID, delegate_to=DELEGATE_TO
        )
        sftp_hook.assert_called_once_with(SFTP_CONN_ID)

        args, kwargs = sftp_hook.return_value.retrieve_file.call_args
        self.assertEqual(args[0], os.path.join(SOURCE_OBJECT_NO_WILDCARD))

        args, kwargs = gcs_hook.return_value.upload.call_args
        self.assertEqual(kwargs["bucket_name"], TEST_BUCKET)
        self.assertEqual(kwargs["object_name"], DESTINATION_PATH_FILE)

        sftp_hook.return_value.delete_file.assert_called_once_with(
            SOURCE_OBJECT_NO_WILDCARD
        )

    @mock.patch("airflow.operators.sftp_to_gcs.GoogleCloudStorageHook")
    @mock.patch("airflow.operators.sftp_to_gcs.SFTPHook")
    def test_execute_copy_with_wildcard(self, sftp_hook, gcs_hook):
        sftp_hook.return_value.get_tree_map.return_value = [
            ["main_dir/test_object3.json", "main_dir/sub_dir/test_object3.json"],
            [],
            [],
        ]

        task = SFTPToGoogleCloudStorageOperator(
            task_id=TASK_ID,
            source_path=SOURCE_OBJECT_WILDCARD_FILENAME,
            destination_bucket=TEST_BUCKET,
            destination_path=DESTINATION_PATH_DIR,
            move_object=True,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
            delegate_to=DELEGATE_TO,
        )
        task.execute(None)

        sftp_hook.return_value.get_tree_map.assert_called_with(
            "main_dir", prefix="main_dir/test_object", delimiter=".json"
        )

        args_index = 0
        kwargs_index = 1
        call_one, call_two = sftp_hook.return_value.retrieve_file.call_args_list
        self.assertEqual(call_one[args_index][0], "main_dir/test_object3.json")
        self.assertEqual(call_two[args_index][0], "main_dir/sub_dir/test_object3.json")

        call_one, call_two = gcs_hook.return_value.upload.call_args_list
        self.assertEqual(call_one[kwargs_index]["bucket_name"], TEST_BUCKET)
        self.assertEqual(
            call_one[kwargs_index]["object_name"], "destination_dir/test_object3.json"
        )
        self.assertEqual(call_two[kwargs_index]["bucket_name"], TEST_BUCKET)
        self.assertEqual(
            call_two[kwargs_index]["object_name"],
            "destination_dir/sub_dir/test_object3.json",
        )

    @mock.patch("airflow.operators.sftp_to_gcs.GoogleCloudStorageHook")
    @mock.patch("airflow.operators.sftp_to_gcs.SFTPHook")
    def test_execute_move_with_wildcard(self, sftp_hook, gcs_hook):
        sftp_hook.return_value.get_tree_map.return_value = [
            ["main_dir/test_object3.json", "main_dir/sub_dir/test_object3.json"],
            [],
            [],
        ]

        gcs_hook.return_value.list.return_value = SOURCE_FILES_LIST[:2]
        task = SFTPToGoogleCloudStorageOperator(
            task_id=TASK_ID,
            source_path=SOURCE_OBJECT_WILDCARD_FILENAME,
            destination_bucket=TEST_BUCKET,
            destination_path=DESTINATION_PATH_DIR,
            move_object=True,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
            delegate_to=DELEGATE_TO,
        )
        task.execute(None)

        args_index = 0
        call_one, call_two = sftp_hook.return_value.delete_file.call_args_list
        self.assertEqual(call_one[args_index][0], "main_dir/test_object3.json")
        self.assertEqual(call_two[args_index][0], "main_dir/sub_dir/test_object3.json")

    @mock.patch("airflow.operators.sftp_to_gcs.GoogleCloudStorageHook")
    @mock.patch("airflow.operators.sftp_to_gcs.SFTPHook")
    def test_execute_more_than_one_wildcard_exception(self, sftp_hook, gcs_hook):
        task = SFTPToGoogleCloudStorageOperator(
            task_id=TASK_ID,
            source_path=SOURCE_OBJECT_MULTIPLE_WILDCARDS,
            destination_bucket=TEST_BUCKET,
            destination_path=DESTINATION_PATH_FILE,
            move_object=False,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
            delegate_to=DELEGATE_TO,
        )
        with self.assertRaises(AirflowException):
            task.execute(None)
