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

import json
from unittest import mock

from airflow.providers.microsoft.azure.operators.adls import ADLSCreateObjectOperator

TASK_ID = "test-adls-upload-operator"
FILE_SYSTEM_NAME = "Fabric"
REMOTE_PATH = "TEST-DIR"
DATA = json.dumps({"name": "David", "surname": "Blain", "gender": "M"}).encode("utf-8")


class TestADLSUploadOperator:
    @mock.patch(
        "airflow.providers.microsoft.azure.operators.adls.AzureDataLakeStorageV2Hook"
    )
    def test_execute_success_when_local_data(self, mock_hook):
        operator = ADLSCreateObjectOperator(
            task_id=TASK_ID,
            file_system_name=FILE_SYSTEM_NAME,
            file_name=REMOTE_PATH,
            data=DATA,
            replace=True,
        )
        operator.execute(None)
        data_lake_file_client_mock = mock_hook.return_value.create_file
        data_lake_file_client_mock.assert_called_once_with(
            file_system_name=FILE_SYSTEM_NAME, file_name=REMOTE_PATH
        )
        upload_data_mock = data_lake_file_client_mock.return_value.upload_data
        upload_data_mock.assert_called_once_with(data=DATA, length=None, overwrite=True)
