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

from airflow.models import Connection
from airflow.providers.microsoft.azure.fs.adls import get_fs


@pytest.fixture
def mocked_blob_file_system():
    with mock.patch("adlfs.AzureBlobFileSystem") as m:
        yield m


@pytest.mark.parametrize(
    ("mocked_connection", "expected_options"),
    [
        (
            Connection(
                conn_id="testconn",
                conn_type="wasb",
                host="testaccountname.blob.core.windows.net",
            ),
            {
                "account_url": "https://testaccountname.blob.core.windows.net",
            },
        ),
        (
            Connection(
                conn_id="testconn",
                conn_type="wasb",
                login="testaccountname",
            ),
            {
                "account_url": "https://testaccountname.blob.core.windows.net/",
            },
        ),
        (
            Connection(
                conn_id="testconn",
                conn_type="wasb",
                login="testaccountname",
                password="p",
                host="testaccountID",
                extra={
                    "connection_string": "c",
                },
            ),
            {
                "connection_string": "c",
            },
        ),
        (
            Connection(
                conn_id="testconn",
                conn_type="wasb",
                login="testaccountname",
                password="p",
                host="testaccountID",
                extra={
                    "account_name": "n",
                    "account_key": "k",
                    "sas_token": "s",
                    "tenant_id": "t",
                    "managed_identity_client_id": "m",
                    "workload_identity_tenant_id": "w",
                    "anon": False,
                    "other_field_name": "other_field_value",
                },
            ),
            {
                "account_url": "https://testaccountname.blob.core.windows.net/",
                # "account_url": "https://testaccountid.blob.core.windows.net/",
                "client_id": "testaccountname",
                "client_secret": "p",
                "account_name": "n",
                "account_key": "k",
                "sas_token": "s",
                "tenant_id": "t",
                "managed_identity_client_id": "m",
                "workload_identity_tenant_id": "w",
                "anon": False,
            },
        ),
        (
            Connection(
                conn_id="testconn",
                conn_type="wasb",
                login="testaccountname",
                password="p",
                host="testaccountID",
                extra={},
            ),
            {
                "account_url": "https://testaccountname.blob.core.windows.net/",
                # "account_url": "https://testaccountid.blob.core.windows.net/",
                "account_key": "p",
            },
        ),
    ],
    indirect=["mocked_connection"],
)
def test_get_fs(mocked_connection, expected_options, mocked_blob_file_system):
    get_fs(mocked_connection.conn_id)
    mocked_blob_file_system.assert_called_once_with(**expected_options)
