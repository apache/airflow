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
"""Transfers data from Exasol database into a S3 Bucket."""

from __future__ import annotations

import os
from unittest import mock

BASE_PATH = "airflow.providers.amazon.aws.transfers.exasol_to_s3.{}"
MOCK_QUERY = "QUERY-CODE"

from airflow.providers.amazon.aws.transfers.exasol_to_s3 import (
    ExasolExportOperator,
    S3UploadOperator,
    ExasolToS3Operator,
)

class TestExasolExportOperator:
    @mock.patch(BASE_PATH.format("NamedTemporaryFile"))
    @mock.patch(BASE_PATH.format("ExasolHook"))
    def test_execute(self, mock_exasol_hook, mock_named_tmp_file):
        mock_tmp_file = mock_named_tmp_file.return_value.__enter__.return_value
        mock_tmp_file.name = "dummy_tmp_file"
        op = ExasolExportOperator(
            task_id="export_task",
            query_or_table=MOCK_QUERY,
            export_params={"export_param": "value"},
            query_params={"query_param": "value"},
            exasol_conn_id="exasol_default",
        )
        ret = op.execute({})
        mock_exasol_hook.assert_called_once_with(exasol_conn_id="exasol_default")
        mock_exasol_hook.return_value.export_to_file.assert_called_once_with(
            filename="dummy_tmp_file",
            query_or_table=MOCK_QUERY,
            export_params={"export_param": "value"},
            query_params={"query_param": "value"},
        )
        mock_tmp_file.flush.assert_called_once_with()
        assert ret == "dummy_tmp_file"

    @mock.patch(BASE_PATH.format("os.remove"))
    @mock.patch(BASE_PATH.format("NamedTemporaryFile"))
    @mock.patch(BASE_PATH.format("ExasolHook"))
    @mock.patch(BASE_PATH.format("os.path.exists"), return_value=True)
    def test_execute_with_exception(
        self, _mock_os_path_exists, mock_exasol_hook, mock_named_tmp_file, mock_os_remove
    ):
        mock_tmp_file = mock_named_tmp_file.return_value.__enter__.return_value
        mock_tmp_file.name = "dummy_tmp_file"
        mock_exasol_hook.return_value.export_to_file.side_effect = Exception("export error")
        op = ExasolExportOperator(
            task_id="export_task",
            query_or_table=MOCK_QUERY,
            exasol_conn_id="exasol_default",
        )
        try:
            op.execute({})
        except Exception as e:
            assert str(e) == "export error"
        mock_os_remove.assert_called_once_with("dummy_tmp_file")


class TestS3UploadOperator:
    @mock.patch(BASE_PATH.format("os.remove"))
    @mock.patch(BASE_PATH.format("S3Hook"))
    @mock.patch(BASE_PATH.format("os.path.exists"), return_value=True)
    def test_execute(self, _mock_os_path_exists, mock_s3_hook, mock_os_remove):
        file_path = "dummy_file"
        op = S3UploadOperator(
            task_id="upload_task",
            file_path=file_path,
            key="dummy_key",
            bucket_name="dummy_bucket",
            replace=True,
            encrypt=True,
            gzip=False,
            acl_policy="dummy_policy",
            aws_conn_id="aws_default",
        )
        ret = op.execute({})
        mock_s3_hook.assert_called_once_with(aws_conn_id="aws_default")
        mock_s3_hook.return_value.load_file.assert_called_once_with(
            filename=file_path,
            key="dummy_key",
            bucket_name="dummy_bucket",
            replace=True,
            encrypt=True,
            gzip=False,
            acl_policy="dummy_policy",
        )
        mock_os_remove.assert_called_once_with(file_path)
        assert ret == "dummy_key"

    @mock.patch(BASE_PATH.format("os.remove"))
    @mock.patch(BASE_PATH.format("S3Hook"))
    @mock.patch(BASE_PATH.format("os.path.exists"), return_value=True)
    def test_execute_with_exception(self, _mock_os_path_exists, mock_s3_hook, mock_os_remove):
        mock_s3_hook.return_value.load_file.side_effect = Exception("upload error")
        file_path = "dummy_file"
        op = S3UploadOperator(
            task_id="upload_task",
            file_path=file_path,
            key="dummy_key",
            bucket_name="dummy_bucket",
            aws_conn_id="aws_default",
        )
        try:
            op.execute({})
        except Exception as e:
            assert str(e) == "upload error"
        mock_os_remove.assert_called_once_with(file_path)


class TestExasolToS3Operator:
    @mock.patch(BASE_PATH.format("S3UploadOperator.execute"))
    @mock.patch(BASE_PATH.format("ExasolExportOperator.execute"))
    def test_execute(self, mock_export_execute, mock_upload_execute):
        mock_export_execute.return_value = "dummy_file"
        mock_upload_execute.return_value = "dummy_key"
        op = ExasolToS3Operator(
            task_id="exasol_to_s3_task",
            query_or_table=MOCK_QUERY,
            key="dummy_key",
            bucket_name="dummy_bucket",
            export_params={"export_param": "value"},
            query_params={"query_param": "value"},
            exasol_conn_id="exasol_default",
            aws_conn_id="aws_default",
        )
        ret = op.execute({})
        assert ret == "dummy_key"
        mock_export_execute.assert_called_once()
        mock_upload_execute.assert_called_once()