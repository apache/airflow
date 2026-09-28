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

import pytest

from airflow.providers.microsoft.azure.operators.adls import (
    DEFAULT_AZURE_DATA_LAKE_CONN_ID,
    ADLSCreateObjectOperator,
    ADLSDeleteOperator,
    ADLSListOperator,
)

TASK_ID = "test-adls-operator"
FILE_SYSTEM_NAME = "Fabric"
REMOTE_PATH = "TEST-DIR"
DATA = json.dumps({"name": "David", "surname": "Blain", "gender": "M"}).encode("utf-8")
TEST_PATH = "test/path"
MOCK_FILES = [
    "test/TEST1.csv",
    "test/TEST2.csv",
    "test/path/TEST3.csv",
    "test/path/PARQUET.parquet",
    "test/path/PIC.png",
]


class TestADLSCreateObjectOperator:
    @pytest.mark.parametrize("replace", [True, False])
    @mock.patch("airflow.providers.microsoft.azure.operators.adls.AzureDataLakeStorageV2Hook")
    def test_execute_uploads_data_and_forwards_replace_as_overwrite(self, mock_hook, replace):
        operator = ADLSCreateObjectOperator(
            task_id=TASK_ID,
            file_system_name=FILE_SYSTEM_NAME,
            file_name=REMOTE_PATH,
            data=DATA,
            replace=replace,
        )

        result = operator.execute(None)

        mock_hook.assert_called_once_with(adls_conn_id=DEFAULT_AZURE_DATA_LAKE_CONN_ID)
        data_lake_file_client_mock = mock_hook.return_value.create_file
        data_lake_file_client_mock.assert_called_once_with(
            file_system_name=FILE_SYSTEM_NAME, file_name=REMOTE_PATH
        )
        upload_data_mock = data_lake_file_client_mock.return_value.upload_data
        upload_data_mock.assert_called_once_with(data=DATA, length=None, overwrite=replace)
        assert result is upload_data_mock.return_value

    @mock.patch("airflow.providers.microsoft.azure.operators.adls.AzureDataLakeStorageV2Hook")
    def test_execute_uses_custom_connection_and_length(self, mock_hook):
        operator = ADLSCreateObjectOperator(
            task_id=TASK_ID,
            file_system_name=FILE_SYSTEM_NAME,
            file_name=REMOTE_PATH,
            data=DATA,
            length=42,
            azure_data_lake_conn_id="adls_custom",
        )

        operator.execute(None)

        mock_hook.assert_called_once_with(adls_conn_id="adls_custom")
        upload_data_mock = mock_hook.return_value.create_file.return_value.upload_data
        upload_data_mock.assert_called_once_with(data=DATA, length=42, overwrite=False)


class TestADLSDeleteOperator:
    @mock.patch("airflow.providers.microsoft.azure.operators.adls.AzureDataLakeHook")
    def test_execute(self, mock_hook):
        operator = ADLSDeleteOperator(task_id=TASK_ID, path=TEST_PATH)

        result = operator.execute(None)

        mock_hook.assert_called_once_with(azure_data_lake_conn_id=DEFAULT_AZURE_DATA_LAKE_CONN_ID)
        mock_hook.return_value.remove.assert_called_once_with(
            path=TEST_PATH, recursive=False, ignore_not_found=True
        )
        assert result is mock_hook.return_value.remove.return_value

    @mock.patch("airflow.providers.microsoft.azure.operators.adls.AzureDataLakeHook")
    def test_execute_forwards_non_default_arguments(self, mock_hook):
        operator = ADLSDeleteOperator(
            task_id=TASK_ID,
            path=TEST_PATH,
            recursive=True,
            ignore_not_found=False,
            azure_data_lake_conn_id="adls_custom",
        )

        operator.execute(None)

        mock_hook.assert_called_once_with(azure_data_lake_conn_id="adls_custom")
        mock_hook.return_value.remove.assert_called_once_with(
            path=TEST_PATH, recursive=True, ignore_not_found=False
        )


class TestADLSListOperator:
    @mock.patch("airflow.providers.microsoft.azure.operators.adls.AzureDataLakeStorageV2Hook")
    def test_execute(self, mock_hook):
        mock_hook.return_value.list_files_directory.return_value = MOCK_FILES

        operator = ADLSListOperator(task_id=TASK_ID, file_system_name=FILE_SYSTEM_NAME, path=TEST_PATH)

        files = operator.execute(None)

        mock_hook.assert_called_once_with(adls_conn_id=DEFAULT_AZURE_DATA_LAKE_CONN_ID)
        mock_hook.return_value.list_files_directory.assert_called_once_with(
            file_system_name=FILE_SYSTEM_NAME,
            directory_name=TEST_PATH,
        )
        assert sorted(files) == sorted(MOCK_FILES)


@pytest.mark.parametrize(
    ("operator_class", "expected_template_fields"),
    [
        pytest.param(
            ADLSCreateObjectOperator,
            ("file_system_name", "file_name", "data", "azure_data_lake_conn_id"),
            id="create",
        ),
        pytest.param(ADLSDeleteOperator, ("path", "azure_data_lake_conn_id"), id="delete"),
        pytest.param(ADLSListOperator, ("path", "azure_data_lake_conn_id"), id="list"),
    ],
)
def test_template_fields(operator_class, expected_template_fields):
    """The templated attributes are a rendering contract for existing Dags — removals break users."""
    assert tuple(operator_class.template_fields) == expected_template_fields
