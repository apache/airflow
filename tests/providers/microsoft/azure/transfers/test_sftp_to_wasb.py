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
from unittest import mock

from airflow import AirflowException
from airflow.providers.microsoft.azure.transfers.sftp_to_wasb import SftpFile, SFTPToWasbOperator

BLOB_PREFIX = "sponge-bob"
CONTAINER_NAME = "test-container"
SOURCE_PATH_NO_WILDCARD = "main_dir/"
EXPECTED_BLOB_NAME = "test_object3.json"
EXPECTED_FILES = [SOURCE_PATH_NO_WILDCARD + EXPECTED_BLOB_NAME]
SOURCE_PATH_MULTIPLE_WILDCARDS = "main_dir/csv/*/test_*"
SFTP_CONN_ID = "ssh_default"
TASK_ID = "test-gcs-to-sftp-operator"
WASB_CONN_ID = "wasb_default"
WILDCARD_PATH = "main_dir/*"
WILDCARD_FILE_NAME = "main_dir/test_object*.json"


# pylint: disable=unused-argument
class TestSFTPToWasbOperator(unittest.TestCase):
    def test_init(self):
        operator = SFTPToWasbOperator(
            task_id=TASK_ID,
            sftp_source_path=SOURCE_PATH_NO_WILDCARD,
            sftp_conn_id=SFTP_CONN_ID,
            container_name=CONTAINER_NAME,
            blob_prefix=BLOB_PREFIX,
            wasb_conn_id=WASB_CONN_ID,
            move_object=False,
        )
        self.assertEqual(operator.sftp_source_path, SOURCE_PATH_NO_WILDCARD)
        self.assertEqual(operator.sftp_conn_id, SFTP_CONN_ID)
        self.assertEqual(operator.container_name, CONTAINER_NAME)
        self.assertEqual(operator.wasb_conn_id, WASB_CONN_ID)
        self.assertEqual(operator.blob_prefix, BLOB_PREFIX)

    @mock.patch('airflow.providers.microsoft.azure.transfers.sftp_to_wasb.WasbHook', autospec=True)
    def test_execute_more_than_one_wildcard_exception(self, mock_hook):
        operator = SFTPToWasbOperator(
            task_id=TASK_ID,
            sftp_source_path=SOURCE_PATH_MULTIPLE_WILDCARDS,
            sftp_conn_id=SFTP_CONN_ID,
            container_name=CONTAINER_NAME,
            blob_prefix=BLOB_PREFIX,
            wasb_conn_id=WASB_CONN_ID,
            move_object=False,
        )
        with self.assertRaises(AirflowException) as cm:
            operator.check_wildcards_limit()

        err = cm.exception
        self.assertIn("Only one wildcard '*' is allowed", str(err))

    def test_source_path_contains_wildcard(self):
        operator = SFTPToWasbOperator(
            task_id=TASK_ID,
            sftp_source_path=WILDCARD_PATH,
            sftp_conn_id=SFTP_CONN_ID,
            container_name=CONTAINER_NAME,
            blob_prefix=BLOB_PREFIX,
            wasb_conn_id=WASB_CONN_ID,
            move_object=False,
        )

        output = operator.source_path_contains_wildcard

        self.assertTrue(output, "This source path contains wildcard")

    def test_source_path_not_contains_wildcard(self):
        operator = SFTPToWasbOperator(
            task_id=TASK_ID,
            sftp_source_path=SOURCE_PATH_NO_WILDCARD,
            sftp_conn_id=SFTP_CONN_ID,
            container_name=CONTAINER_NAME,
            blob_prefix=BLOB_PREFIX,
            wasb_conn_id=WASB_CONN_ID,
            move_object=False,
        )

        output = operator.source_path_contains_wildcard

        self.assertFalse(output, "This source path does not contains wildcard")

    def test_get_sftp_tree_behavior(self):
        operator = SFTPToWasbOperator(
            task_id=TASK_ID,
            sftp_source_path=WILDCARD_PATH,
            sftp_conn_id=SFTP_CONN_ID,
            container_name=CONTAINER_NAME,
            wasb_conn_id=WASB_CONN_ID,
            move_object=False,
        )
        sftp_complete_path, prefix, delimiter = operator.get_tree_behavior()

        self.assertEqual(sftp_complete_path, "main_dir", "not matched at expected complete path")
        self.assertEqual(prefix, "main_dir/", "Prefix must be before the wildcard")
        self.assertEqual(delimiter, "", "Delimiter must be empty")

    @mock.patch('airflow.providers.microsoft.azure.transfers.sftp_to_wasb.WasbHook')
    @mock.patch('airflow.providers.microsoft.azure.transfers.sftp_to_wasb.SFTPHook')
    def test_get_sftp_files_map(self, sftp_hook, mock_hook):
        sftp_hook.return_value.get_tree_map.return_value = [
            EXPECTED_FILES,
            [],
            [],
        ]
        operator = SFTPToWasbOperator(
            task_id=TASK_ID,
            sftp_source_path=SOURCE_PATH_NO_WILDCARD,
            sftp_conn_id=SFTP_CONN_ID,
            container_name=CONTAINER_NAME,
            wasb_conn_id=WASB_CONN_ID,
            move_object=True,
        )
        files = operator.get_sftp_files_map()

        self.assertEqual(len(files), 1, "no matched at expected found files")
        self.assertEqual(files[0].blob_name, EXPECTED_BLOB_NAME, "expected blob name not matched")

    @mock.patch('airflow.providers.microsoft.azure.transfers.sftp_to_wasb.WasbHook')
    @mock.patch('airflow.providers.microsoft.azure.transfers.sftp_to_wasb.SFTPHook')
    def test_upload_wasb(self, sftp_hook, mock_hook):
        operator = SFTPToWasbOperator(
            task_id=TASK_ID,
            sftp_source_path=SOURCE_PATH_NO_WILDCARD,
            sftp_conn_id=SFTP_CONN_ID,
            container_name=CONTAINER_NAME,
            wasb_conn_id=WASB_CONN_ID,
            move_object=True,
        )

        sftp_files = [SftpFile(EXPECTED_FILES[0], EXPECTED_BLOB_NAME)]
        files = operator.copy_files_to_wasb(sftp_files)

        operator.sftp_hook.retrieve_file.assert_has_calls([mock.call("main_dir/test_object3.json", mock.ANY)])

        mock_hook.return_value.load_file.assert_called_once_with(mock.ANY, CONTAINER_NAME, EXPECTED_BLOB_NAME)

        self.assertEqual(len(files), 1, "no matched at expected uploaded files")

    @mock.patch('airflow.providers.microsoft.azure.transfers.sftp_to_wasb.SFTPHook')
    def test_sftp_move(self, sftp_hook):
        sftp_mock = sftp_hook.return_value

        operator = SFTPToWasbOperator(
            task_id=TASK_ID,
            sftp_source_path=SOURCE_PATH_NO_WILDCARD,
            sftp_conn_id=SFTP_CONN_ID,
            container_name=CONTAINER_NAME,
            wasb_conn_id=WASB_CONN_ID,
            move_object=True,
        )

        sftp_file_paths = EXPECTED_FILES
        operator.delete_files(sftp_file_paths)

        sftp_mock.delete_file.assert_has_calls([mock.call(EXPECTED_FILES[0])])

    @mock.patch('airflow.providers.microsoft.azure.transfers.sftp_to_wasb.WasbHook')
    @mock.patch('airflow.providers.microsoft.azure.transfers.sftp_to_wasb.SFTPHook')
    def test_execute(self, sftp_hook, mock_hook):
        sftp_hook.return_value.get_tree_map.return_value = [
            ["main_dir/test_object.json"],
            [],
            [],
        ]

        operator = SFTPToWasbOperator(
            task_id=TASK_ID,
            sftp_source_path=WILDCARD_FILE_NAME,
            sftp_conn_id=SFTP_CONN_ID,
            container_name=CONTAINER_NAME,
            wasb_conn_id=WASB_CONN_ID,
            move_object=False,
        )

        operator.execute(None)

        sftp_hook.return_value.get_tree_map.assert_called_with(
            "main_dir", prefix="main_dir/test_object", delimiter=".json"
        )

        sftp_hook.return_value.retrieve_file.assert_has_calls(
            [mock.call("main_dir/test_object.json", mock.ANY)]
        )

        mock_hook.return_value.load_file.assert_called_once_with(mock.ANY, CONTAINER_NAME, "test_object.json")

        sftp_hook.delete_file.assert_not_called()

    @mock.patch('airflow.providers.microsoft.azure.transfers.sftp_to_wasb.WasbHook')
    @mock.patch('airflow.providers.microsoft.azure.transfers.sftp_to_wasb.SFTPHook')
    def test_execute_moving_files(self, sftp_hook, mock_hook):
        sftp_hook.return_value.get_tree_map.return_value = [
            ["main_dir/test_object.json"],
            [],
            [],
        ]

        operator = SFTPToWasbOperator(
            task_id=TASK_ID,
            sftp_source_path=WILDCARD_FILE_NAME,
            sftp_conn_id=SFTP_CONN_ID,
            container_name=CONTAINER_NAME,
            wasb_conn_id=WASB_CONN_ID,
            blob_prefix=BLOB_PREFIX,
            move_object=True,
        )

        operator.execute(None)

        sftp_hook.return_value.get_tree_map.assert_called_with(
            "main_dir", prefix="main_dir/test_object", delimiter=".json"
        )

        sftp_hook.return_value.retrieve_file.assert_has_calls(
            [mock.call("main_dir/test_object.json", mock.ANY)]
        )

        mock_hook.return_value.load_file.assert_called_once_with(
            mock.ANY, CONTAINER_NAME, BLOB_PREFIX + "test_object.json"
        )

        self.assertTrue(sftp_hook.return_value.delete_file.called, "It did perform move of file")
