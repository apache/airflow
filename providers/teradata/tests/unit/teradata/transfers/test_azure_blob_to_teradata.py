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

from airflow.providers.teradata.transfers.azure_blob_to_teradata import AzureBlobStorageToTeradataOperator

AZURE_CONN_ID = "wasb_default"
TERADATA_CONN_ID = "teradata_default"
BLOB_SOURCE_KEY = "az/test"
TERADATA_TABLE = "test"
TASK_ID = "transfer_file"


class TestAzureBlobStorageToTeradataOperator:
    def test_init(self):
        operator = AzureBlobStorageToTeradataOperator(
            azure_conn_id=AZURE_CONN_ID,
            teradata_conn_id=TERADATA_CONN_ID,
            teradata_table=TERADATA_TABLE,
            blob_source_key=BLOB_SOURCE_KEY,
            task_id=TASK_ID,
        )

        assert operator.azure_conn_id == AZURE_CONN_ID
        assert operator.blob_source_key == BLOB_SOURCE_KEY
        assert operator.teradata_conn_id == TERADATA_CONN_ID
        assert operator.teradata_table == TERADATA_TABLE
        assert operator.task_id == TASK_ID

    @mock.patch("airflow.providers.teradata.transfers.azure_blob_to_teradata.TeradataHook")
    @mock.patch("airflow.providers.teradata.transfers.azure_blob_to_teradata.WasbHook")
    def test_execute(self, mock_hook_wasb, mock_hook_teradata):
        op = AzureBlobStorageToTeradataOperator(
            azure_conn_id=AZURE_CONN_ID,
            teradata_conn_id=TERADATA_CONN_ID,
            teradata_table=TERADATA_TABLE,
            blob_source_key=BLOB_SOURCE_KEY,
            task_id=TASK_ID,
        )
        op.execute(context=None)
        mock_hook_wasb.assert_called_once_with(wasb_conn_id=AZURE_CONN_ID)
        mock_hook_teradata.assert_called_once_with(teradata_conn_id=TERADATA_CONN_ID)
        sql = "SQL"
        mock_hook_teradata.run(sql)
