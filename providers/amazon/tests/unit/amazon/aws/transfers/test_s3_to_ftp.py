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

from unittest import mock

import pytest

from airflow.providers.amazon.aws.transfers.s3_to_ftp import S3ToFTPOperator

TASK_ID = "test_s3_to_ftp"
BUCKET = "test-s3-bucket"
S3_KEY = "test/test_1_file.csv"
FTP_PATH = "/tmp/remote_path.txt"
AWS_CONN_ID = "aws_default"
FTP_CONN_ID = "ftp_default"


class TestS3ToFTPOperator:
    @mock.patch("airflow.providers.ftp.hooks.ftp.FTPHook.store_file")
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_key")
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.check_for_key", return_value=True)
    @mock.patch("airflow.providers.amazon.aws.transfers.s3_to_ftp.NamedTemporaryFile")
    def test_execute(
        self, mock_local_tmp_file, mock_check_for_key, mock_s3_hook_get_key, mock_ftp_hook_store_file
    ):
        operator = S3ToFTPOperator(task_id=TASK_ID, s3_bucket=BUCKET, s3_key=S3_KEY, ftp_path=FTP_PATH)
        operator.execute(None)

        mock_s3_hook_get_key.assert_called_once_with(operator.s3_key, operator.s3_bucket)

        mock_local_tmp_file_value = mock_local_tmp_file.return_value.__enter__.return_value
        mock_s3_hook_get_key.return_value.download_fileobj.assert_called_once_with(mock_local_tmp_file_value)
        mock_ftp_hook_store_file.assert_called_once_with(operator.ftp_path, mock_local_tmp_file_value.name)


class TestS3ToFTPOperatorInit:
    """Unit tests for S3ToFTPOperator.__init__ that do not require an FTP server."""

    @pytest.mark.parametrize(
        ("s3_filenames", "ftp_filenames"),
        [
            (None, None),
            ("*", None),
            ("prefix_", "renamed_"),
            (["a.csv", "b.csv"], ["x.csv", "y.csv"]),
        ],
    )
    def test_multi_file_params(self, s3_filenames, ftp_filenames):
        """s3_filenames and ftp_filenames are stored correctly."""
        op = S3ToFTPOperator(
            task_id="test_multi",
            s3_bucket=BUCKET,
            s3_key=S3_KEY,
            ftp_path=FTP_PATH,
            s3_filenames=s3_filenames,
            ftp_filenames=ftp_filenames,
        )
        assert op.s3_filenames == s3_filenames
        assert op.ftp_filenames == ftp_filenames

    def test_fail_on_file_not_exist_default(self):
        """fail_on_file_not_exist defaults to True."""
        op = S3ToFTPOperator(task_id="test_fail_default", s3_bucket=BUCKET, s3_key=S3_KEY, ftp_path=FTP_PATH)
        assert op.fail_on_file_not_exist is True

    @pytest.mark.parametrize("fail_on_file_not_exist", [True, False])
    def test_fail_on_file_not_exist_skip(self, fail_on_file_not_exist):
        """When key is missing: raise FileNotFoundError if True, skip if False."""
        from unittest.mock import MagicMock, patch

        op = S3ToFTPOperator(
            task_id="test_skip",
            s3_bucket=BUCKET,
            s3_key=S3_KEY,
            ftp_path=FTP_PATH,
            fail_on_file_not_exist=fail_on_file_not_exist,
        )
        mock_s3_hook = MagicMock()
        mock_s3_hook.check_for_key.return_value = False

        if fail_on_file_not_exist:
            with pytest.raises(FileNotFoundError):
                op._download_from_s3(mock_s3_hook, MagicMock(), S3_KEY, FTP_PATH)
        else:
            with patch.object(op.log, "info") as mock_log:
                op._download_from_s3(mock_s3_hook, MagicMock(), S3_KEY, FTP_PATH)
            mock_log.assert_called_once()
