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
from unittest.mock import patch

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
SOURCE_OBJECT_WILDCARD_TXT_FILENAME = "main_dir/test_object*.txt"
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
    def test_execute_copy_single_file_with_stream(self, sftp_hook, gcs_hook):
        task = SFTPToGCSOperator(
            task_id=TASK_ID,
            source_path=SOURCE_OBJECT_NO_WILDCARD,
            destination_bucket=TEST_BUCKET,
            destination_path=DESTINATION_PATH_FILE,
            move_object=False,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            use_stream=True,
        )

        task.execute(None)

        gcs_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        gcs_hook.return_value.get_bucket.assert_called_once_with(TEST_BUCKET)
        gcs_hook.return_value.get_bucket.return_value.blob.assert_called_once_with(DESTINATION_PATH_FILE)
        sftp_hook.assert_called_once_with(SFTP_CONN_ID)
        sftp_hook.return_value.retrieve_file.assert_called_once_with(
            os.path.join(SOURCE_OBJECT_NO_WILDCARD), mock.ANY, prefetch=True
        )
        gcs_hook.return_value.upload.assert_not_called()
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
    def test_execute_copy_with_wildcard_and_default_destination_path(self, sftp_hook, gcs_hook):
        sftp_hook.return_value.get_tree_map.return_value = [
            ["main_dir/test_object1.txt", "main_dir/test_object2.txt"],
            [],
            [],
        ]

        task = SFTPToGCSOperator(
            task_id=TASK_ID,
            source_path=SOURCE_OBJECT_WILDCARD_TXT_FILENAME,
            destination_bucket=TEST_BUCKET,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
        )
        task.execute(None)

        sftp_hook.return_value.get_tree_map.assert_called_with(
            "main_dir", prefix="main_dir/test_object", delimiter=".txt"
        )

        sftp_hook.return_value.retrieve_file.assert_has_calls(
            [
                mock.call("main_dir/test_object1.txt", mock.ANY, prefetch=True),
                mock.call("main_dir/test_object2.txt", mock.ANY, prefetch=True),
            ]
        )

        gcs_hook.return_value.upload.assert_has_calls(
            [
                mock.call(
                    bucket_name=TEST_BUCKET,
                    object_name="test_object1.txt",
                    mime_type=DEFAULT_MIME_TYPE,
                    filename=mock.ANY,
                    gzip=False,
                ),
                mock.call(
                    bucket_name=TEST_BUCKET,
                    object_name="test_object2.txt",
                    mime_type=DEFAULT_MIME_TYPE,
                    filename=mock.ANY,
                    gzip=False,
                ),
            ]
        )

    @pytest.mark.parametrize(
        ("source_object", "destination_path", "expected_source", "expected_destination"),
        [
            ("folder/test_object.txt", "dest/dir", "folder/test_object.txt", "dest"),
            ("folder/test_object.txt", "dest/dir/", "folder/test_object.txt", "dest/dir"),
            ("folder/test_object.txt", "/", "folder/test_object.txt", "/"),
            (
                "folder/test_object.txt",
                "dest/dir/dest_object.txt",
                "folder/test_object.txt",
                "dest/dir/dest_object.txt",
            ),
            ("folder/test_object*.txt", "dest/dir", "folder", "dest"),
            ("folder/test_object/*", "/", "folder/test_object", "/"),
            ("folder/test_object*", "/", "folder", "/"),
            ("folder/test_object/*", None, "folder/test_object", "/"),
            ("*", "/", "/", "/"),
            ("/*", "/", "/", "/"),
            ("/*", "dest/dir", "/", "dest"),
        ],
    )
    @mock.patch("airflow.providers.google.cloud.transfers.sftp_to_gcs.SFTPHook")
    def test_get_openlineage_facets(
        self, sftp_hook_mock, source_object, destination_path, expected_source, expected_destination
    ):
        sftp_hook_mock.return_value.remote_host = "11.222.33.44"
        sftp_hook_mock.return_value.port = 22
        operator = SFTPToGCSOperator(
            task_id=TASK_ID,
            source_path=source_object,
            destination_path=destination_path,
            destination_bucket=TEST_BUCKET,
            move_object=False,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
        )

        result = operator.get_openlineage_facets_on_start()
        assert not result.run_facets
        assert not result.job_facets
        assert len(result.inputs) == 1
        assert len(result.outputs) == 1
        assert result.inputs[0].namespace == "file://11.222.33.44:22"
        assert result.inputs[0].name == expected_source
        assert result.outputs[0].namespace == f"gs://{TEST_BUCKET}"
        assert result.outputs[0].name == expected_destination

    @pytest.mark.parametrize("fail_on_file_not_exist", [False, True])
    @mock.patch("airflow.providers.google.cloud.transfers.sftp_to_gcs.GCSHook")
    @mock.patch("airflow.providers.google.cloud.transfers.sftp_to_gcs.SFTPHook")
    def test_sftp_to_gcs_fail_on_file_not_exist(self, sftp_hook, gcs_hook, fail_on_file_not_exist):
        invalid_file_name = "main_dir/invalid-object.json"
        task = SFTPToGCSOperator(
            task_id=TASK_ID,
            source_path=invalid_file_name,
            destination_bucket=TEST_BUCKET,
            destination_path=DESTINATION_PATH_FILE,
            move_object=False,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            fail_on_file_not_exist=fail_on_file_not_exist,
        )
        with patch.object(sftp_hook.return_value, "retrieve_file", side_effect=FileNotFoundError):
            if fail_on_file_not_exist:
                with pytest.raises(FileNotFoundError):
                    task.execute(None)
            else:
                task.execute(None)
