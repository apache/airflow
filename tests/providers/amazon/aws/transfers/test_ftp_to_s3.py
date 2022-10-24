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

import unittest
from unittest import mock

from airflow.providers.amazon.aws.transfers.ftp_to_s3 import FTPToS3Operator

TASK_ID = "test_ftp_to_s3"
BUCKET = "test-s3-bucket"
S3_KEY = "test/test_1_file.csv"
FTP_PATH = "/tmp/remote_path.txt"
AWS_CONN_ID = "aws_default"
FTP_CONN_ID = "ftp_default"
S3_KEY_MULTIPLE = "test/"
FTP_PATH_MULTIPLE = "/tmp/"


class TestFTPToS3Operator(unittest.TestCase):
    def assert_execute(
        self, mock_local_tmp_file, mock_s3_hook_load_file, mock_ftp_hook_retrieve_file, ftp_file, s3_file
    ):

        mock_local_tmp_file_value = mock_local_tmp_file.return_value.__enter__.return_value
        mock_ftp_hook_retrieve_file.assert_called_once_with(
            local_full_path_or_buffer=mock_local_tmp_file_value.name, remote_full_path=ftp_file
        )

        mock_s3_hook_load_file.assert_called_once_with(
            filename=mock_local_tmp_file_value.name,
            key=s3_file,
            bucket_name=BUCKET,
            acl_policy=None,
            encrypt=False,
            gzip=False,
            replace=False,
        )

    @mock.patch("airflow.providers.ftp.hooks.ftp.FTPHook.retrieve_file")
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.load_file")
    @mock.patch("airflow.providers.amazon.aws.transfers.ftp_to_s3.NamedTemporaryFile")
    def test_execute(self, mock_local_tmp_file, mock_s3_hook_load_file, mock_ftp_hook_retrieve_file):
        operator = FTPToS3Operator(task_id=TASK_ID, s3_bucket=BUCKET, s3_key=S3_KEY, ftp_path=FTP_PATH)
        operator.execute(None)

        self.assert_execute(
            mock_local_tmp_file,
            mock_s3_hook_load_file,
            mock_ftp_hook_retrieve_file,
            ftp_file=operator.ftp_path,
            s3_file=operator.s3_key,
        )

    @mock.patch("airflow.providers.ftp.hooks.ftp.FTPHook.retrieve_file")
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.load_file")
    @mock.patch("airflow.providers.amazon.aws.transfers.ftp_to_s3.NamedTemporaryFile")
    def test_execute_multiple_files_different_names(
        self, mock_local_tmp_file, mock_s3_hook_load_file, mock_ftp_hook_retrieve_file
    ):

        operator = FTPToS3Operator(
            task_id=TASK_ID,
            s3_bucket=BUCKET,
            s3_key=S3_KEY_MULTIPLE,
            ftp_path=FTP_PATH_MULTIPLE,
            ftp_filenames=["test1.txt"],
            s3_filenames=["test1_s3.txt"],
        )
        operator.execute(None)

        self.assert_execute(
            mock_local_tmp_file,
            mock_s3_hook_load_file,
            mock_ftp_hook_retrieve_file,
            ftp_file=operator.ftp_path + operator.ftp_filenames[0],
            s3_file=operator.s3_key + operator.s3_filenames[0],
        )

    @mock.patch("airflow.providers.ftp.hooks.ftp.FTPHook.retrieve_file")
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.load_file")
    @mock.patch("airflow.providers.amazon.aws.transfers.ftp_to_s3.NamedTemporaryFile")
    def test_execute_multiple_files_same_names(
        self, mock_local_tmp_file, mock_s3_hook_load_file, mock_ftp_hook_retrieve_file
    ):

        operator = FTPToS3Operator(
            task_id=TASK_ID,
            s3_bucket=BUCKET,
            s3_key=S3_KEY_MULTIPLE,
            ftp_path=FTP_PATH_MULTIPLE,
            ftp_filenames=["test1.txt"],
        )
        operator.execute(None)

        self.assert_execute(
            mock_local_tmp_file,
            mock_s3_hook_load_file,
            mock_ftp_hook_retrieve_file,
            ftp_file=operator.ftp_path + operator.ftp_filenames[0],
            s3_file=operator.s3_key + operator.ftp_filenames[0],
        )

    @mock.patch("airflow.providers.ftp.hooks.ftp.FTPHook.list_directory")
    def test_execute_multiple_files_prefix(
        self,
        mock_ftp_hook_list_directory,
    ):

        operator = FTPToS3Operator(
            task_id=TASK_ID,
            s3_bucket=BUCKET,
            s3_key=S3_KEY_MULTIPLE,
            ftp_path=FTP_PATH_MULTIPLE,
            ftp_filenames="test_prefix",
            s3_filenames="s3_prefix",
        )
        operator.execute(None)

        mock_ftp_hook_list_directory.assert_called_once_with(path=FTP_PATH_MULTIPLE)
