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
import unittest
from unittest import mock

from airflow.providers.trino.transfers.gcs_to_trino import GCSToTrinoOperator

BUCKET = "source_bucket"
PATH = "path/to/file.csv"
GCP_CONN_ID = "test_gcp"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]
TRINO_CONN_ID = "test_trino"
TRINO_TABLE = "test_table"
TASK_ID = "test_gcs_to_trino"
SCHEMA_FIELDS = ["colA", "colB", "colC"]
SCHEMA_JSON = "path/to/file.json"


class TestGCSToTrinoOperator(unittest.TestCase):
    @mock.patch('airflow.providers.trino.transfers.gcs_to_trino.TrinoHook')
    @mock.patch("airflow.providers.trino.transfers.gcs_to_trino.GCSHook")
    @mock.patch("airflow.providers.trino.transfers.gcs_to_trino.NamedTemporaryFile")
    def test_execute_without_schema(self, mock_tempfile, mock_gcs_hook, mock_trino_hook):
        filename = "file://97g23r"
        file_handle = mock.MagicMock()
        mock_tempfile.return_value.__enter__.return_value = file_handle
        mock_tempfile.return_value.__enter__.return_value.name = filename

        op = GCSToTrinoOperator(
            task_id=TASK_ID,
            source_bucket=BUCKET,
            source_object=PATH,
            trino_table=TRINO_TABLE,
            trino_conn_id=TRINO_CONN_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(None)

        mock_gcs_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=None,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_trino_hook.assert_called_once_with(trino_conn_id=TRINO_CONN_ID)

        mock_download = mock_gcs_hook.return_value.download

        mock_download.assert_called_once_with(bucket_name=BUCKET, object_name=PATH, filename=filename)

        mock_insert = mock_trino_hook.return_value.insert_rows

        mock_insert.assert_called_once()

    @mock.patch('airflow.providers.trino.transfers.gcs_to_trino.TrinoHook')
    @mock.patch("airflow.providers.trino.transfers.gcs_to_trino.GCSHook")
    @mock.patch("airflow.providers.trino.transfers.gcs_to_trino.NamedTemporaryFile")
    def test_execute_schema_fields(self, mock_tempfile, mock_gcs_hook, mock_trino_hook):
        filename = "file://97g23r"
        file_handle = mock.MagicMock()
        mock_tempfile.return_value.__enter__.return_value = file_handle
        mock_tempfile.return_value.__enter__.return_value.name = filename

        op = GCSToTrinoOperator(
            task_id=TASK_ID,
            source_bucket=BUCKET,
            source_object=PATH,
            trino_table=TRINO_TABLE,
            trino_conn_id=TRINO_CONN_ID,
            gcp_conn_id=GCP_CONN_ID,
            schema_fields=SCHEMA_FIELDS,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(None)

        mock_gcs_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=None,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_trino_hook.assert_called_once_with(trino_conn_id=TRINO_CONN_ID)

        mock_download = mock_gcs_hook.return_value.download

        mock_download.assert_called_once_with(bucket_name=BUCKET, object_name=PATH, filename=filename)

        mock_insert = mock_trino_hook.return_value.insert_rows

        mock_insert.assert_called_once()

    @mock.patch('airflow.providers.trino.transfers.gcs_to_trino.json.loads')
    @mock.patch('airflow.providers.trino.transfers.gcs_to_trino.TrinoHook')
    @mock.patch("airflow.providers.trino.transfers.gcs_to_trino.GCSHook")
    @mock.patch("airflow.providers.trino.transfers.gcs_to_trino.NamedTemporaryFile")
    def test_execute_schema_json(self, mock_tempfile, mock_gcs_hook, mock_trino_hook, mock_json_loader):
        filename = "file://97g23r"
        file_handle = mock.MagicMock()
        mock_tempfile.return_value.__enter__.return_value = file_handle
        mock_tempfile.return_value.__enter__.return_value.name = filename
        mock_json_loader.return_value = SCHEMA_FIELDS

        op = GCSToTrinoOperator(
            task_id=TASK_ID,
            source_bucket=BUCKET,
            source_object=PATH,
            trino_table=TRINO_TABLE,
            trino_conn_id=TRINO_CONN_ID,
            gcp_conn_id=GCP_CONN_ID,
            schema_object=SCHEMA_JSON,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(None)

        mock_gcs_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=None,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_trino_hook.assert_called_once_with(trino_conn_id=TRINO_CONN_ID)

        mock_download = mock_gcs_hook.return_value.download

        assert mock_download.call_count == 2

        mock_insert = mock_trino_hook.return_value.insert_rows

        mock_insert.assert_called_once()
