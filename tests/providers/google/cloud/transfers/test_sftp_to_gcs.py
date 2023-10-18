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
from __future__ import annotations

import os
from unittest import mock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.transfers.sftp_to_gcs import SFTPToGCSOperator

TASK_ID = "test-gcs-to-sftp-operator"
GCP_CONN_ID = "GCP_CONN_ID"
SFTP_CONN_ID = "SFTP_CONN_ID"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]

DEFAULT_MIME_TYPE = "application/octet-stream"

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


class TestSFTPToGCSOperator:
    @mock.patch("airflow.providers.google.cloud.transfers.sftp_to_gcs.GCSHook")
    @mock.patch("airflow.providers.google.cloud.transfers.sftp_to_gcs.SFTPHook")
    def test_execute_copy_single_file(self, sftp_hook, gcs_hook):
        task = SFTPToGCSOperator(
            task_id=TASK_ID,
            source_path=SOURCE_OBJECT_NO_WILDCARD,
            destination_bucket=TEST_BUCKET,
            destination_path=DESTINATION_PATH_FILE,
            move_object=False,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        task.execute(None)
        gcs_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        sftp_hook.assert_called_once_with(SFTP_CONN_ID)

        sftp_hook.return_value.retrieve_file.assert_called_once_with(
            os.path.join(SOURCE_OBJECT_NO_WILDCARD), mock.ANY, prefetch=True
        )

        gcs_hook.return_value.upload.assert_called_once_with(
            bucket_name=TEST_BUCKET,
            object_name=DESTINATION_PATH_FILE,
            filename=mock.ANY,
            mime_type=DEFAULT_MIME_TYPE,
            gzip=False,
        )

        sftp_hook.return_value.delete_file.assert_not_called()

    @mock.patch("airflow.providers.google.cloud.transfers.sftp_to_gcs.GCSHook")
    @mock.patch("airflow.providers.google.cloud.transfers.sftp_to_gcs.SFTPHook")
    def test_execute_copy_single_file_with_compression(self, sftp_hook, gcs_hook):
        task = SFTPToGCSOperator(
            task_id=TASK_ID,
            source_path=SOURCE_OBJECT_NO_WILDCARD,
            destination_bucket=TEST_BUCKET,
            destination_path=DESTINATION_PATH_FILE,
            move_object=False,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            gzip=True,
            sftp_prefetch=False,
        )
        task.execute(None)
        gcs_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        sftp_hook.assert_called_once_with(SFTP_CONN_ID)

        sftp_hook.return_value.retrieve_file.assert_called_once_with(
            os.path.join(SOURCE_OBJECT_NO_WILDCARD), mock.ANY, prefetch=False
        )

        gcs_hook.return_value.upload.assert_called_once_with(
            bucket_name=TEST_BUCKET,
            object_name=DESTINATION_PATH_FILE,
            filename=mock.ANY,
            mime_type=DEFAULT_MIME_TYPE,
            gzip=True,
        )

        sftp_hook.return_value.delete_file.assert_not_called()

    @mock.patch("airflow.providers.google.cloud.transfers.sftp_to_gcs.GCSHook")
    @mock.patch("airflow.providers.google.cloud.transfers.sftp_to_gcs.SFTPHook")
    def test_execute_move_single_file(self, sftp_hook, gcs_hook):
        task = SFTPToGCSOperator(
            task_id=TASK_ID,
            source_path=SOURCE_OBJECT_NO_WILDCARD,
            destination_bucket=TEST_BUCKET,
            destination_path=DESTINATION_PATH_FILE,
            move_object=True,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            sftp_prefetch=True,
        )
        task.execute(None)
        gcs_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        sftp_hook.assert_called_once_with(SFTP_CONN_ID)

        sftp_hook.return_value.retrieve_file.assert_called_once_with(
            os.path.join(SOURCE_OBJECT_NO_WILDCARD), mock.ANY, prefetch=True
        )

        gcs_hook.return_value.upload.assert_called_once_with(
            bucket_name=TEST_BUCKET,
            object_name=DESTINATION_PATH_FILE,
            filename=mock.ANY,
            mime_type=DEFAULT_MIME_TYPE,
            gzip=False,
        )

        sftp_hook.return_value.delete_file.assert_called_once_with(SOURCE_OBJECT_NO_WILDCARD)

    @mock.patch("airflow.providers.google.cloud.transfers.sftp_to_gcs.GCSHook")
    @mock.patch("airflow.providers.google.cloud.transfers.sftp_to_gcs.SFTPHook")
    def test_execute_copy_with_wildcard(self, sftp_hook, gcs_hook):
        sftp_hook.return_value.get_tree_map.return_value = [
            ["main_dir/test_object3.json", "main_dir/sub_dir/test_object3.json"],
            [],
            [],
        ]

        task = SFTPToGCSOperator(
            task_id=TASK_ID,
            source_path=SOURCE_OBJECT_WILDCARD_FILENAME,
            destination_bucket=TEST_BUCKET,
            destination_path=DESTINATION_PATH_DIR,
            move_object=True,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
        )
        task.execute(None)

        sftp_hook.return_value.get_tree_map.assert_called_with(
            "main_dir", prefix="main_dir/test_object", delimiter=".json"
        )

        sftp_hook.return_value.retrieve_file.assert_has_calls(
            [
                mock.call("main_dir/test_object3.json", mock.ANY, prefetch=True),
                mock.call("main_dir/sub_dir/test_object3.json", mock.ANY, prefetch=True),
            ]
        )

        gcs_hook.return_value.upload.assert_has_calls(
            [
                mock.call(
                    bucket_name=TEST_BUCKET,
                    object_name="destination_dir/test_object3.json",
                    mime_type=DEFAULT_MIME_TYPE,
                    filename=mock.ANY,
                    gzip=False,
                ),
                mock.call(
                    bucket_name=TEST_BUCKET,
                    object_name="destination_dir/sub_dir/test_object3.json",
                    mime_type=DEFAULT_MIME_TYPE,
                    filename=mock.ANY,
                    gzip=False,
                ),
            ]
        )

    @mock.patch("airflow.providers.google.cloud.transfers.sftp_to_gcs.GCSHook")
    @mock.patch("airflow.providers.google.cloud.transfers.sftp_to_gcs.SFTPHook")
    def test_execute_move_with_wildcard(self, sftp_hook, gcs_hook):
        sftp_hook.return_value.get_tree_map.return_value = [
            ["main_dir/test_object3.json", "main_dir/sub_dir/test_object3.json"],
            [],
            [],
        ]

        gcs_hook.return_value.list.return_value = SOURCE_FILES_LIST[:2]
        task = SFTPToGCSOperator(
            task_id=TASK_ID,
            source_path=SOURCE_OBJECT_WILDCARD_FILENAME,
            destination_bucket=TEST_BUCKET,
            destination_path=DESTINATION_PATH_DIR,
            move_object=True,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
        )
        task.execute(None)

        sftp_hook.return_value.delete_file.assert_has_calls(
            [
                mock.call("main_dir/test_object3.json"),
                mock.call("main_dir/sub_dir/test_object3.json"),
            ]
        )

    @mock.patch("airflow.providers.google.cloud.transfers.sftp_to_gcs.GCSHook")
    @mock.patch("airflow.providers.google.cloud.transfers.sftp_to_gcs.SFTPHook")
    def test_execute_more_than_one_wildcard_exception(self, sftp_hook, gcs_hook):
        task = SFTPToGCSOperator(
            task_id=TASK_ID,
            source_path=SOURCE_OBJECT_MULTIPLE_WILDCARDS,
            destination_bucket=TEST_BUCKET,
            destination_path=DESTINATION_PATH_FILE,
            move_object=False,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
        )
        with pytest.raises(AirflowException) as ctx:
            task.execute(None)

        err = ctx.value
        assert "Only one wildcard '*' is allowed in source_path parameter" in str(err)

    @mock.patch("airflow.providers.google.cloud.transfers.sftp_to_gcs.GCSHook")
    @mock.patch("airflow.providers.google.cloud.transfers.sftp_to_gcs.SFTPHook")
    def test_execute_with_streaming(self, sftp_hook, gcs_hook):
        chunks = [b"chunk1", b"chunk2", b"chunk3", b""]
        sftp_file_mock = mock.MagicMock()
        sftp_file_mock.read.side_effect = chunks
        gcs_blob_mock = mock.MagicMock()

        sftp_hook.return_value.retrieve_file.return_value.__enter__.return_value = sftp_file_mock
        gcs_hook.return_value.get_bucket.return_value.blob.return_value = gcs_blob_mock

        task = SFTPToGCSOperator(
            task_id=TASK_ID,
            source_path=SOURCE_OBJECT_NO_WILDCARD,
            destination_bucket=TEST_BUCKET,
            destination_path=DESTINATION_PATH_FILE,
            move_object=False,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            use_stream=True
        )
        task.execute(None)
        assert sftp_file_mock.read.call_count == len(chunks)
        gcs_blob_mock.upload_from_file.assert_called_with(sftp_file_mock, size=len(b''.join(chunks)))

    @mock.patch("airflow.providers.google.cloud.transfers.sftp_to_gcs.GCSHook")
    @mock.patch("airflow.providers.google.cloud.transfers.sftp_to_gcs.SFTPHook")
    def test_execute_stream_single_file(self, sftp_hook, gcs_hook):
        sftp_file = sftp_hook.return_value.get_conn.return_value.file

        message = b"file"
        mock_sftp_buffer = BytesIO(message)
        sftp_file.return_value.__enter__.return_value = mock_sftp_buffer     

        gcs_client = MagicMock()
        gcs_blob = MagicMock()
        written_content = BytesIO()
        gcs_hook.return_value.get_conn.return_value = gcs_client
        gcs_client.bucket.return_value.blob = gcs_blob
        gcs_blob.return_value.open.return_value.__enter__.return_value = written_content

        task = SFTPToGCSOperator(
            task_id=TASK_ID,
            source_path=SOURCE_OBJECT_NO_WILDCARD,
            destination_bucket=TEST_BUCKET,
            destination_path=DESTINATION_PATH_FILE,
            move_object=False,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            use_stream=True
        )
        task.execute(None)
        gcs_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        sftp_hook.assert_called_once_with(SFTP_CONN_ID)
        sftp_file.assert_called_once_with(SOURCE_OBJECT_NO_WILDCARD, 'rb')
        gcs_client.bucket.assert_called_once_with(TEST_BUCKET)
        gcs_blob.assert_called_once_with(DESTINATION_PATH_FILE)

        written_content.seek(0)
        assert written_content.read() == message # decode bytes to string