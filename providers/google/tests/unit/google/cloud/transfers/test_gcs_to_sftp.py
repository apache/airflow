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

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud.transfers.gcs_to_sftp import GCSToSFTPOperator

TASK_ID = "test-gcs-to-sftp-operator"
GCP_CONN_ID = "GCP_CONN_ID"
SFTP_CONN_ID = "SFTP_CONN_ID"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]
TEST_BUCKET = "test-bucket"
DESTINATION_SFTP = "destination_path"


# TODO: After deprecating delimiter and wildcards in source objects,
#       implement reverted changes from the first commit of PR #31261
class TestGoogleCloudStorageToSFTPOperator:
    @pytest.mark.parametrize(
        ("source_object", "target_object", "keep_directory_structure"),
        [
            ("folder/test_object.txt", "folder/test_object.txt", True),
            ("folder/subfolder/test_object.txt", "folder/subfolder/test_object.txt", True),
            ("folder/test_object.txt", "test_object.txt", False),
            ("folder/subfolder/test_object.txt", "test_object.txt", False),
        ],
    )
    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_sftp.GCSHook")
    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_sftp.SFTPHook")
    def test_execute_copy_single_file(
        self, sftp_hook_mock, gcs_hook_mock, source_object, target_object, keep_directory_structure
    ):
        task = GCSToSFTPOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=source_object,
            destination_path=DESTINATION_SFTP,
            keep_directory_structure=keep_directory_structure,
            move_object=False,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        task.execute({})
        gcs_hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        sftp_hook_mock.assert_called_once_with(SFTP_CONN_ID)

        gcs_hook_mock.return_value.download.assert_called_with(
            bucket_name=TEST_BUCKET, object_name=source_object, filename=mock.ANY
        )

        sftp_hook_mock.return_value.store_file.assert_called_with(
            os.path.join(DESTINATION_SFTP, target_object), mock.ANY
        )

        gcs_hook_mock.return_value.delete.assert_not_called()

    @pytest.mark.parametrize(
        ("source_object", "target_object", "keep_directory_structure"),
        [
            ("folder/test_object.txt", "folder/test_object.txt", True),
            ("folder/subfolder/test_object.txt", "folder/subfolder/test_object.txt", True),
            ("folder/test_object.txt", "test_object.txt", False),
            ("folder/subfolder/test_object.txt", "test_object.txt", False),
        ],
    )
    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_sftp.GCSHook")
    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_sftp.SFTPHook")
    def test_execute_move_single_file(
        self, sftp_hook_mock, gcs_hook_mock, source_object, target_object, keep_directory_structure
    ):
        task = GCSToSFTPOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=source_object,
            destination_path=DESTINATION_SFTP,
            keep_directory_structure=keep_directory_structure,
            move_object=True,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        task.execute(None)
        gcs_hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        sftp_hook_mock.assert_called_once_with(SFTP_CONN_ID)

        gcs_hook_mock.return_value.download.assert_called_with(
            bucket_name=TEST_BUCKET, object_name=source_object, filename=mock.ANY
        )

        sftp_hook_mock.return_value.store_file.assert_called_with(
            os.path.join(DESTINATION_SFTP, target_object), mock.ANY
        )

        gcs_hook_mock.return_value.delete.assert_called_once_with(TEST_BUCKET, source_object)

    @pytest.mark.parametrize(
        (
            "source_object",
            "prefix",
            "delimiter",
            "gcs_files_list",
            "target_objects",
            "keep_directory_structure",
        ),
        [
            (
                "folder/test_object*.txt",
                "folder/test_object",
                ".txt",
                [
                    "folder/test_object/file1.txt",
                    "folder/test_object/file2.txt",
                ],
                ["test_object/file1.txt", "test_object/file2.txt"],
                False,
            ),
            (
                "folder/test_object/*",
                "folder/test_object/",
                "",
                [
                    "folder/test_object/file1.txt",
                    "folder/test_object/file2.txt",
                ],
                ["file1.txt", "file2.txt"],
                False,
            ),
            (
                "folder/test_object*.txt",
                "folder/test_object",
                ".txt",
                [
                    "folder/test_object/file1.txt",
                    "folder/test_object/file2.txt",
                ],
                ["folder/test_object/file1.txt", "folder/test_object/file2.txt"],
                True,
            ),
            (
                "folder/test_object/*",
                "folder/test_object/",
                "",
                [
                    "folder/test_object/file1.txt",
                    "folder/test_object/file2.txt",
                ],
                ["folder/test_object/file1.txt", "folder/test_object/file2.txt"],
                True,
            ),
        ],
    )
    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_sftp.GCSHook")
    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_sftp.SFTPHook")
    def test_execute_copy_with_wildcard(
        self,
        sftp_hook_mock,
        gcs_hook_mock,
        source_object,
        prefix,
        delimiter,
        gcs_files_list,
        target_objects,
        keep_directory_structure,
    ):
        gcs_hook_mock.return_value.list.return_value = gcs_files_list
        operator = GCSToSFTPOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=source_object,
            destination_path=DESTINATION_SFTP,
            keep_directory_structure=keep_directory_structure,
            move_object=False,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
        )
        operator.execute(None)

        gcs_hook_mock.return_value.list.assert_called_with(TEST_BUCKET, delimiter=delimiter, prefix=prefix)

        gcs_hook_mock.return_value.download.assert_has_calls(
            [
                mock.call(bucket_name=TEST_BUCKET, object_name=gcs_file, filename=mock.ANY)
                for gcs_file in gcs_files_list
            ]
        )
        sftp_hook_mock.return_value.store_file.assert_has_calls(
            [
                mock.call(os.path.join(DESTINATION_SFTP, target_object), mock.ANY)
                for target_object in target_objects
            ]
        )

        gcs_hook_mock.return_value.delete.assert_not_called()

    @pytest.mark.parametrize(
        (
            "source_object",
            "prefix",
            "delimiter",
            "gcs_files_list",
            "target_objects",
            "keep_directory_structure",
        ),
        [
            (
                "folder/test_object*.txt",
                "folder/test_object",
                ".txt",
                [
                    "folder/test_object/file1.txt",
                    "folder/test_object/file2.txt",
                ],
                ["test_object/file1.txt", "test_object/file2.txt"],
                False,
            ),
            (
                "folder/test_object/*",
                "folder/test_object/",
                "",
                [
                    "folder/test_object/file1.txt",
                    "folder/test_object/file2.txt",
                ],
                ["file1.txt", "file2.txt"],
                False,
            ),
            (
                "folder/test_object*.txt",
                "folder/test_object",
                ".txt",
                [
                    "folder/test_object/file1.txt",
                    "folder/test_object/file2.txt",
                ],
                ["folder/test_object/file1.txt", "folder/test_object/file2.txt"],
                True,
            ),
            (
                "folder/test_object/*",
                "folder/test_object/",
                "",
                [
                    "folder/test_object/file1.txt",
                    "folder/test_object/file2.txt",
                ],
                ["folder/test_object/file1.txt", "folder/test_object/file2.txt"],
                True,
            ),
        ],
    )
    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_sftp.GCSHook")
    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_sftp.SFTPHook")
    def test_execute_move_with_wildcard(
        self,
        sftp_hook_mock,
        gcs_hook_mock,
        source_object,
        prefix,
        delimiter,
        gcs_files_list,
        target_objects,
        keep_directory_structure,
    ):
        gcs_hook_mock.return_value.list.return_value = gcs_files_list
        operator = GCSToSFTPOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=source_object,
            destination_path=DESTINATION_SFTP,
            keep_directory_structure=keep_directory_structure,
            move_object=True,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
        )
        operator.execute(None)

        gcs_hook_mock.return_value.list.assert_called_with(TEST_BUCKET, delimiter=delimiter, prefix=prefix)

        gcs_hook_mock.return_value.download.assert_has_calls(
            [
                mock.call(bucket_name=TEST_BUCKET, object_name=gcs_file, filename=mock.ANY)
                for gcs_file in gcs_files_list
            ]
        )
        sftp_hook_mock.return_value.store_file.assert_has_calls(
            [
                mock.call(os.path.join(DESTINATION_SFTP, target_object), mock.ANY)
                for target_object in target_objects
            ]
        )

        gcs_hook_mock.return_value.delete.assert_has_calls(
            [mock.call(TEST_BUCKET, gcs_file) for gcs_file in gcs_files_list]
        )

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_sftp.GCSHook")
    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_sftp.SFTPHook")
    def test_execute_more_than_one_wildcard_exception(self, sftp_hook_mock, gcs_hook_mock):
        operator = GCSToSFTPOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object="csv/*/test_*.csv",
            destination_path=DESTINATION_SFTP,
            move_object=False,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
        )
        with pytest.raises(AirflowException):
            operator.execute(None)

    @pytest.mark.parametrize(
        (
            "source_object",
            "destination_path",
            "keep_directory_structure",
            "expected_source",
            "expected_destination",
        ),
        [
            (
                "folder/test_object.txt",
                "dest/dir",
                True,
                "folder/test_object.txt",
                "dest/dir/folder/test_object.txt",
            ),
            (
                "folder/test_object.txt",
                "dest/dir/",
                True,
                "folder/test_object.txt",
                "dest/dir/folder/test_object.txt",
            ),
            (
                "folder/test_object.txt",
                "dest/dir",
                False,
                "folder/test_object.txt",
                "dest/dir/test_object.txt",
            ),
            ("folder/test_object.txt", "/", False, "folder/test_object.txt", "/test_object.txt"),
            ("folder/test_object.txt", "/", True, "folder/test_object.txt", "/folder/test_object.txt"),
            (
                "folder/test_object.txt",
                "dest/dir/dest_object.txt",
                True,
                "folder/test_object.txt",
                "dest/dir/dest_object.txt/folder/test_object.txt",  # Dest path is always treated as "dir"
            ),
            (
                "folder/test_object.txt",
                "dest/dir/dest_object.txt",
                False,
                "folder/test_object.txt",
                "dest/dir/dest_object.txt/test_object.txt",  # Dest path is always treated as "dir"
            ),
            ("folder/test_object*.txt", "dest/dir", True, "folder", "dest/dir/folder"),
            ("folder/test_object*", "dest/dir", False, "folder", "dest/dir"),
            ("*", "/", True, "/", "/"),
            ("*", "/dest/dir", True, "/", "/dest/dir"),
            ("*", "/dest/dir", False, "/", "/dest/dir"),
        ],
    )
    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_sftp.SFTPHook")
    def test_get_openlineage_facets(
        self,
        sftp_hook_mock,
        source_object,
        destination_path,
        keep_directory_structure,
        expected_source,
        expected_destination,
    ):
        sftp_hook_mock.return_value.remote_host = "11.222.33.44"
        sftp_hook_mock.return_value.port = 22
        operator = GCSToSFTPOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=source_object,
            destination_path=destination_path,
            keep_directory_structure=keep_directory_structure,
            move_object=False,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
        )

        result = operator.get_openlineage_facets_on_start()
        assert not result.run_facets
        assert not result.job_facets
        assert len(result.inputs) == 1
        assert len(result.outputs) == 1
        assert result.inputs[0].namespace == f"gs://{TEST_BUCKET}"
        assert result.inputs[0].name == expected_source
        assert result.outputs[0].namespace == "file://11.222.33.44:22"
        assert result.outputs[0].name == expected_destination

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_sftp.GCSHook")
    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_sftp.SFTPHook")
    def test_create_intermediate_dirs_true(self, sftp_hook_mock, gcp_hook_mock):
        task = GCSToSFTPOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object="folder/test_object.txt",  # Hard-coding
            destination_path=DESTINATION_SFTP,
            keep_directory_structure=True,  # Hard-coding
            create_intermediate_dirs=True,
            move_object=False,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        assert task.create_intermediate_dirs

        task.execute(None)

        sftp_hook_mock.return_value.create_directory.assert_called_once_with(
            os.path.join(DESTINATION_SFTP, "folder")
        )

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_sftp.GCSHook")
    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_sftp.SFTPHook")
    def test_create_intermediate_dirs_false(self, sftp_hook_mock, gcp_hook_mock):
        task = GCSToSFTPOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object="folder/test_object.txt",  # Hard-coding
            destination_path=DESTINATION_SFTP,
            keep_directory_structure=True,  # Hard-coding
            create_intermediate_dirs=False,
            move_object=False,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        assert not task.create_intermediate_dirs

        task.execute(None)

        sftp_hook_mock.return_value.create_directory.assert_not_called()

    @pytest.mark.parametrize(
        "source_object",
        [
            pytest.param("incoming/../../../../etc/passwd", id="dotdot-segments"),
            pytest.param("/etc/passwd", id="absolute-path"),
        ],
    )
    def test_resolve_destination_path_rejects_escape(self, source_object):
        # ``_resolve_destination_path`` is the GCSToSFTPOperator method that
        # joins a GCS object name with the configured ``destination_path``.
        # When the joined path canonicalises outside the destination — either
        # via ``..`` segments or an absolute ``source_object`` that absorbs
        # the prefix — the method must refuse rather than hand the path to
        # the SFTP server, where the server would resolve it on its own host.
        task = GCSToSFTPOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object="incoming/*",
            destination_path="/srv/sftp/incoming",
            keep_directory_structure=True,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
        )
        with pytest.raises(ValueError, match="escapes configured destination_path"):
            task._resolve_destination_path(source_object)

    def test_resolve_destination_path_allows_benign_nested(self):
        # The new validation is post-join normalisation; benign nested paths
        # under the destination must still resolve cleanly.
        task = GCSToSFTPOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object="incoming/*",
            destination_path="/srv/sftp/incoming",
            keep_directory_structure=True,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
        )
        assert (
            task._resolve_destination_path("incoming/sub/dir/file.csv")
            == "/srv/sftp/incoming/incoming/sub/dir/file.csv"
        )

    @pytest.mark.parametrize(
        ("destination_path", "source_object", "expected"),
        [
            pytest.param(".", "file.txt", "file.txt", id="dot-base-benign"),
            pytest.param("", "file.txt", "file.txt", id="empty-base-benign"),
            pytest.param(".", "sub/dir/file.txt", "sub/dir/file.txt", id="dot-base-nested"),
        ],
    )
    def test_resolve_destination_path_allows_relative_base(self, destination_path, source_object, expected):
        # ``destination_path="."`` and ``destination_path=""`` are valid SFTP
        # destinations — they refer to the SFTP user's login / current
        # directory. The validation must allow these benign uploads while
        # still rejecting ``..`` escapes and absolute-path absorption (covered
        # by the next test).
        task = GCSToSFTPOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object="*",
            destination_path=destination_path,
            keep_directory_structure=True,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
        )
        assert task._resolve_destination_path(source_object) == expected

    @pytest.mark.parametrize(
        ("destination_path", "source_object"),
        [
            pytest.param(".", "../etc/passwd", id="dotdot-escape-from-dot-base"),
            pytest.param(".", "/etc/passwd", id="absolute-absorbs-dot-base"),
            # Non-trivial relative bases: the ``..`` segments cancel the base
            # itself, so ``normpath`` leaves no leading ``..`` and the
            # leading-``..`` check alone would let them through — containment
            # against the configured base must still reject them.
            pytest.param("incoming", "../.ssh/authorized_keys", id="dotdot-escape-from-nested-base"),
            pytest.param("uploads/in", "../../etc/passwd", id="dotdot-escape-from-deeper-base"),
        ],
    )
    def test_resolve_destination_path_rejects_escape_from_relative_base(
        self, destination_path, source_object
    ):
        # A relative ``destination_path`` must still contain the resolved path:
        # ``..`` segments that cancel the base and absolute ``source_object``
        # values that absorb it entirely are both refused.
        task = GCSToSFTPOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object="*",
            destination_path=destination_path,
            keep_directory_structure=True,
            gcp_conn_id=GCP_CONN_ID,
            sftp_conn_id=SFTP_CONN_ID,
        )
        with pytest.raises(ValueError, match="escapes configured destination_path"):
            task._resolve_destination_path(source_object)
