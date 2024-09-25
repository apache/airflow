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

from airflow.providers.amazon.aws.transfers.glacier_to_gcs import GlacierToGCSOperator

AWS_CONN_ID = "aws_default"
BUCKET_NAME = "airflow_bucket"
FILENAME = "path/to/file/"
GCP_CONN_ID = "google_cloud_default"
JOB_ID = "1a2b3c4d"
OBJECT_NAME = "file.csv"
TASK_ID = "glacier_job"
VAULT_NAME = "airflow"


class TestGlacierToGCSOperator:
    @mock.patch("airflow.providers.amazon.aws.transfers.glacier_to_gcs.GlacierHook")
    @mock.patch("airflow.providers.amazon.aws.transfers.glacier_to_gcs.GCSHook")
    @mock.patch("airflow.providers.amazon.aws.transfers.glacier_to_gcs.tempfile")
    def test_execute(self, mock_temp, hook_gcs_mock, hook_aws_mock):
        op = GlacierToGCSOperator(
            aws_conn_id=AWS_CONN_ID,
            vault_name=VAULT_NAME,
            gcp_conn_id=GCP_CONN_ID,
            google_impersonation_chain=None,
            bucket_name=BUCKET_NAME,
            object_name=OBJECT_NAME,
            gzip=False,
            task_id=TASK_ID,
        )

        op.execute(context=None)
        hook_aws_mock.assert_called_once_with(aws_conn_id=AWS_CONN_ID)
        hook_aws_mock.return_value.retrieve_inventory.assert_called_once_with(vault_name=VAULT_NAME)
        hook_aws_mock.return_value.retrieve_inventory_results.assert_called_once_with(
            vault_name=VAULT_NAME, job_id=hook_aws_mock.return_value.retrieve_inventory.return_value[JOB_ID]
        )

        hook_gcs_mock.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=None)
        hook_gcs_mock.return_value.upload.assert_called_once_with(
            bucket_name=BUCKET_NAME,
            object_name=OBJECT_NAME,
            gzip=False,
            filename=mock_temp.NamedTemporaryFile.return_value.__enter__.return_value.name,
        )
