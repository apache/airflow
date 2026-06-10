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

import os
from unittest import mock

import pytest

from airflow.models import Connection
from airflow.providers.microsoft.azure.fs.adls import get_fs

if os.environ.get("_AIRFLOW_SKIP_DB_TESTS") == "true":
    # Handle collection of the test by non-db case
    Connection = mock.MagicMock()  # type: ignore[misc]


pytestmark = pytest.mark.db_test


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
                "account_name": "testaccountname",
            },
        ),
        (
            Connection(
                conn_id="testconn",
                conn_type="wasb",
                login="testaccountname",
            ),
            {
                "account_name": "testaccountname",
            },
        ),
        (
            Connection(
                conn_id="testconn",
                conn_type="wasb",
                login="testaccountname",
                password="password",
                host="testaccountID",
                extra={
                    "connection_string": "connection_string",
                },
            ),
            {
                "connection_string": "connection_string",
            },
        ),
        (
            Connection(
                conn_id="testconn",
                conn_type="wasb",
                login="testaccountname",
                password="password",
                host="testaccountID",
                extra={
                    "account_name": "account_name",
                    "account_key": "account_key",
                    "sas_token": "sas_token",
                    "tenant_id": "tenant_id",
                    "managed_identity_client_id": "managed_identity",
                    "workload_identity_tenant_id": "workload_identity",
                    "anon": False,
                    "other_field_name": "other_field_value",
                },
            ),
            {
                "account_name": "account_name",
                "client_id": "testaccountname",
                "client_secret": "password",
                "account_key": "account_key",
                "sas_token": "sas_token",
                "tenant_id": "tenant_id",
                "managed_identity_client_id": "managed_identity",
                "workload_identity_tenant_id": "workload_identity",
                "anon": False,
            },
        ),
        (
            Connection(
                conn_id="testconn",
                conn_type="wasb",
                login="testaccountname",
                password="password",
                host="testaccountID",
                extra={},
            ),
            {
                "account_name": "testaccountname",
                "account_key": "password",
            },
        ),
        (
            Connection(
                conn_id="testconn",
                conn_type="wasb",
                login="testaccountname",
                password="password",
                host="testaccountID",
                extra={
                    "account_host": "mystorageaccount.blob.core.mydomain.io",
                },
            ),
            {
                "account_name": "testaccountname",
                "account_host": "mystorageaccount.blob.core.mydomain.io",
                "account_key": "password",
            },
        ),
        (
            Connection(
                conn_id="testconn",
                conn_type="adls",
                host="testaccountname",
                login="client_id",
                password="client_secret",
                extra={
                    "tenant_id": "tenant_id",
                },
            ),
            {
                "account_name": "testaccountname",
                "client_id": "client_id",
                "client_secret": "client_secret",
                "tenant_id": "tenant_id",
            },
        ),
        (
            Connection(
                conn_id="testconn",
                conn_type="adls",
                host="testaccountname.dfs.core.windows.net",
                login="client_id",
                password="client_secret",
                extra={
                    "tenant_id": "tenant_id",
                },
            ),
            {
                "account_name": "testaccountname",
                "client_id": "client_id",
                "client_secret": "client_secret",
                "tenant_id": "tenant_id",
            },
        ),
        (
            Connection(
                conn_id="testconn",
                conn_type="adls",
                login="client_id",
                password="client_secret",
                extra={
                    "tenant_id": "tenant_id",
                },
            ),
            {
                "client_id": "client_id",
                "client_secret": "client_secret",
                "tenant_id": "tenant_id",
            },
        ),
        (
            Connection(
                conn_id="testconn",
                conn_type="adls",
                login="client_id",
                password="client_secret",
                extra={
                    "account_name": "testaccountname",
                    "tenant_id": "tenant_id",
                },
            ),
            {
                "account_name": "testaccountname",
                "client_id": "client_id",
                "client_secret": "client_secret",
                "tenant_id": "tenant_id",
            },
        ),
    ],
    indirect=["mocked_connection"],
)
def test_get_fs(mocked_connection, expected_options, mocked_blob_file_system):
    get_fs(mocked_connection.conn_id)
    mocked_blob_file_system.assert_called_once_with(**expected_options)
