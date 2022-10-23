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

from unittest import TestCase, mock

from airflow.providers.amazon.aws.operators.glacier import (
    GlacierCreateJobOperator,
    GlacierUploadArchiveOperator,
)

AWS_CONN_ID = "aws_default"
BUCKET_NAME = "airflow_bucket"
FILENAME = "path/to/file/"
GCP_CONN_ID = "google_cloud_default"
JOB_ID = "1a2b3c4d"
OBJECT_NAME = "file.csv"
TASK_ID = "glacier_job"
VAULT_NAME = "airflow"


class TestGlacierCreateJobOperator(TestCase):
    @mock.patch("airflow.providers.amazon.aws.operators.glacier.GlacierHook")
    def test_execute(self, hook_mock):
        op = GlacierCreateJobOperator(aws_conn_id=AWS_CONN_ID, vault_name=VAULT_NAME, task_id=TASK_ID)
        op.execute(mock.MagicMock())
        hook_mock.assert_called_once_with(aws_conn_id=AWS_CONN_ID)
        hook_mock.return_value.retrieve_inventory.assert_called_once_with(vault_name=VAULT_NAME)


class TestGlacierUploadArchiveOperator(TestCase):
    @mock.patch("airflow.providers.amazon.aws.operators.glacier.GlacierHook.get_conn")
    def test_execute(self, hook_mock):
        op = GlacierUploadArchiveOperator(
            aws_conn_id=AWS_CONN_ID, vault_name=VAULT_NAME, body=b'Test Data', task_id=TASK_ID
        )
        op.execute(mock.MagicMock())
        hook_mock.return_value.upload_archive.assert_called_once_with(
            accountId=None, vaultName=VAULT_NAME, archiveDescription=None, body=b'Test Data', checksum=None
        )
