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

from unittest import mock

from airflow.providers.amazon.aws.transfers.exasol_to_s3 import ExasolToS3Operator

BASE_PATH = "airflow.providers.amazon.aws.transfers.exasol_to_s3.{}"


class TestExasolToS3Operator:
    @mock.patch(BASE_PATH.format("NamedTemporaryFile"))
    @mock.patch(BASE_PATH.format("ExasolHook"))
    @mock.patch(BASE_PATH.format("S3Hook"))
    def test_execute(self, mock_s3_hook, mock_exasol_hook, mock_local_tmp_file):
        mock_fh = mock_local_tmp_file.return_value.__enter__.return_value

        op = ExasolToS3Operator(
            task_id="task_id",
            query_or_table="query_or_table",
            key="key",
            bucket_name="bucket_name",
            replace=False,
            encrypt=True,
            gzip=False,
            acl_policy="acl_policy",
            query_params={"query_params": "1"},
            export_params={"export_params": "2"},
            exasol_conn_id="exasol_conn_id",
            aws_conn_id="aws_conn_id",
        )
        op.execute({})

        mock_s3_hook.assert_called_once_with(aws_conn_id="aws_conn_id")
        mock_exasol_hook.assert_called_once_with(exasol_conn_id="exasol_conn_id")

        mock_exasol_hook.return_value.export_to_file.assert_called_once_with(
            filename=mock_fh.name,
            query_or_table="query_or_table",
            query_params={"query_params": "1"},
            export_params={"export_params": "2"},
        )

        mock_fh.flush.assert_called_once_with()

        mock_s3_hook.return_value.load_file(
            key="key",
            bucket_name="bucket_name",
            replace=False,
            encrypt=True,
            gzip=False,
            acl_policy="acl_policy",
        )
