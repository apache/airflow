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
import os

import mock

from airflow import AirflowException
from airflow.providers.microsoft.azure.transfers.stfp_to_wasb import STFPToWasbOperator
from airflow.providers.microsoft.azure.transfers.stfp_to_wasb import SFTP_FILE_PATH
from airflow.providers.microsoft.azure.transfers.stfp_to_wasb import BLOB_NAME


TASK_ID = "test-gcs-to-sftp-operator"
WASB_CONN_ID = "wasb_default"
SFTP_CONN_ID = "ssh_default"

CONTAINER_NAME = "test-container"
WILDCARD_PATH = "main_dir/*"
WILDCARD_FILE_NAME = "main_dir/test_object*.json"
SOURCE_OBJECT_NO_WILDCARD = "main_dir/test_object3.json"
SOURCE_OBJECT_MULTIPLE_WILDCARDS = "main_dir/csv/*/test_*.csv"
NEW_BLOB_NAME = "sponge-bob"
EXPECTED_BLOB_NAME = "test_object3.json"


# pylint: disable=unused-argument
class TestSFTPToWasbOperator(unittest.TestCase):

    def test_init(self):
        operator = STFPToWasbOperator(
            task_id=TASK_ID,
            sftp_source_path=SOURCE_OBJECT_NO_WILDCARD,
            sftp_conn_id=SFTP_CONN_ID,
            container_name=CONTAINER_NAME,
            blob_name=NEW_BLOB_NAME,
            wasb_conn_id=WASB_CONN_ID,
            move_object=False
        )
        self.assertEqual(operator.sftp_source_path, SOURCE_OBJECT_NO_WILDCARD)
        self.assertEqual(operator.sftp_conn_id, SFTP_CONN_ID)
        self.assertEqual(operator.container_name, CONTAINER_NAME)
        self.assertEqual(operator.wasb_conn_id, WASB_CONN_ID)

    @mock.patch('airflow.providers.microsoft.azure.transfers.stfp_to_wasb.WasbHook',
                autospec=True)
    def test_execute_more_than_one_wildcard_exception(self, mock_hook):
        operator = STFPToWasbOperator(
            task_id=TASK_ID,
            sftp_source_path=SOURCE_OBJECT_MULTIPLE_WILDCARDS,
            sftp_conn_id=SFTP_CONN_ID,
            container_name=CONTAINER_NAME,
            blob_name=BLOB_NAME,
            wasb_conn_id=WASB_CONN_ID,
            move_object=False
        )
        with self.assertRaises(AirflowException) as cm:
            operator.check_wildcards_limit()

        err = cm.exception
        self.assertIn("Only one wildcard '*' is allowed", str(err))

    @mock.patch('airflow.providers.microsoft.azure.transfers.stfp_to_wasb.WasbHook')
    @mock.patch('airflow.providers.microsoft.azure.transfers.stfp_to_wasb.SFTPHook')
    def test_get_sftp_files_map_with_wildcard(self, sftp_hook, mock_hook):
        sftp_hook.return_value.get_tree_map.return_value = [
            [SOURCE_OBJECT_NO_WILDCARD, SOURCE_OBJECT_NO_WILDCARD],
            [],
            [],
        ]
        operator = STFPToWasbOperator(
            task_id=TASK_ID,
            sftp_source_path=WILDCARD_PATH,
            sftp_conn_id=SFTP_CONN_ID,
            container_name=CONTAINER_NAME,
            blob_name=BLOB_NAME,
            wasb_conn_id=WASB_CONN_ID,
            move_object=False
        )
        _, files = operator.get_sftp_files_map()

        sftp_hook.assert_called_once_with(SFTP_CONN_ID)

        self.assertEqual(len(files), 2, "not matched at expected found files")
        self.assertEqual(files[0][BLOB_NAME], EXPECTED_BLOB_NAME, "expected blob name not matched")

    @mock.patch('airflow.providers.microsoft.azure.transfers.stfp_to_wasb.WasbHook')
    @mock.patch('airflow.providers.microsoft.azure.transfers.stfp_to_wasb.SFTPHook')
    def test_get_sftp_files_map_no_wildcard(self, sftp_hook, mock_hook):
        sftp_hook.return_value.get_tree_map.return_value = [
            [SOURCE_OBJECT_NO_WILDCARD],
            [],
            [],
        ]
        operator = STFPToWasbOperator(
            task_id=TASK_ID,
            sftp_source_path=SOURCE_OBJECT_NO_WILDCARD,
            sftp_conn_id=SFTP_CONN_ID,
            container_name=CONTAINER_NAME,
            blob_name=BLOB_NAME,
            wasb_conn_id=WASB_CONN_ID,
            move_object=True
        )
        _, files = operator.get_sftp_files_map()

        sftp_hook.assert_called_once_with(SFTP_CONN_ID)

        self.assertEqual(len(files), 1, "no matched at expected found files")
        self.assertEqual(files[0][BLOB_NAME], BLOB_NAME, "expected blob name not matched")

    @mock.patch('airflow.providers.microsoft.azure.transfers.stfp_to_wasb.WasbHook')
    @mock.patch('airflow.providers.microsoft.azure.transfers.stfp_to_wasb.SFTPHook')
    def test_upload_wasb(self, sftp_hook, mock_hook):
        operator = STFPToWasbOperator(
            task_id=TASK_ID,
            sftp_source_path=SOURCE_OBJECT_NO_WILDCARD,
            sftp_conn_id=SFTP_CONN_ID,
            container_name=CONTAINER_NAME,
            blob_name=BLOB_NAME,
            wasb_conn_id=WASB_CONN_ID,
            move_object=True
        )

        sftp_files = [{SFTP_FILE_PATH: SOURCE_OBJECT_NO_WILDCARD, BLOB_NAME: BLOB_NAME}]
        files = operator.upload_wasb(sftp_hook, sftp_files)

        sftp_hook.retrieve_file.assert_has_calls(
            [
                mock.call("main_dir/test_object3.json", mock.ANY)
            ]
        )

        mock_hook.return_value.load_file.assert_called_once_with(
            mock.ANY, CONTAINER_NAME, BLOB_NAME
        )

        self.assertEqual(len(files), 1, "no matched at expected uploaded files")

    @mock.patch('airflow.providers.microsoft.azure.transfers.stfp_to_wasb.SFTPHook')
    def test_sftp_move(self, sftp_hook):
        sftp_mock = sftp_hook.return_value

        operator = STFPToWasbOperator(
            task_id=TASK_ID,
            sftp_source_path=SOURCE_OBJECT_NO_WILDCARD,
            sftp_conn_id=SFTP_CONN_ID,
            container_name=CONTAINER_NAME,
            blob_name=BLOB_NAME,
            wasb_conn_id=WASB_CONN_ID,
            move_object=True
        )

        sftp_file_paths = [SOURCE_OBJECT_NO_WILDCARD]
        operator.sftp_move(sftp_mock, sftp_file_paths)

        sftp_mock.delete_file.assert_has_calls(
            [
                mock.call(SOURCE_OBJECT_NO_WILDCARD)
            ]
        )

    @mock.patch('airflow.providers.microsoft.azure.transfers.stfp_to_wasb.SFTPHook')
    def test_no_sftp_move(self, sftp_hook):
        sftp_mock = sftp_hook.return_value

        operator = STFPToWasbOperator(
            task_id=TASK_ID,
            sftp_source_path=SOURCE_OBJECT_NO_WILDCARD,
            sftp_conn_id=SFTP_CONN_ID,
            container_name=CONTAINER_NAME,
            blob_name=BLOB_NAME,
            wasb_conn_id=WASB_CONN_ID,
            move_object=False
        )

        sftp_file_paths = [SOURCE_OBJECT_NO_WILDCARD]
        operator.sftp_move(sftp_mock, sftp_file_paths)

        sftp_mock.delete_file.assert_not_called()

    @mock.patch('airflow.providers.microsoft.azure.transfers.stfp_to_wasb.WasbHook')
    @mock.patch('airflow.providers.microsoft.azure.transfers.stfp_to_wasb.SFTPHook')
    def test_execute(self, sftp_hook, mock_hook):
        sftp_hook.return_value.get_tree_map.return_value = [
            [SOURCE_OBJECT_NO_WILDCARD],
            [],
            [],
        ]

        operator = STFPToWasbOperator(
            task_id=TASK_ID,
            sftp_source_path=WILDCARD_FILE_NAME,
            sftp_conn_id=SFTP_CONN_ID,
            container_name=CONTAINER_NAME,
            blob_name=BLOB_NAME,
            wasb_conn_id=WASB_CONN_ID,
            move_object=False
        )

        operator.execute(None)

        sftp_hook.return_value.get_tree_map.assert_called_with(
            "main_dir", prefix="main_dir/test_object", delimiter=".json"
        )

        sftp_hook.return_value.retrieve_file.assert_has_calls(
            [
                mock.call(SOURCE_OBJECT_NO_WILDCARD, mock.ANY)
            ]
        )

        mock_hook.return_value.load_file.assert_called_once_with(
            mock.ANY, CONTAINER_NAME, os.path.basename(SOURCE_OBJECT_NO_WILDCARD)
        )

        sftp_hook.delete_file.assert_not_called()
