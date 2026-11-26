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
from airflow.providers.microsoft.azure.fs.adls import _bare_account_name_from_host, get_fs

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
                "client_id": "testaccountname",
                "client_secret": "password",
                "account_name": "account_name",
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
            # Service principal without a host: the login is a client id, so
            # no account_name or account_host may be derived from it; the
            # account comes from extras alone.
            Connection(
                conn_id="testconn",
                conn_type="wasb",
                login="testclientid",
                password="testclientsecret",
                extra={
                    "account_name": "actualstorage",
                    "tenant_id": "tenant_id",
                },
            ),
            {
                "client_id": "testclientid",
                "client_secret": "testclientsecret",
                "account_name": "actualstorage",
                "tenant_id": "tenant_id",
            },
        ),
        (
            # Same, but with a bare identifier in host: it is not an endpoint,
            # so no account_host may be derived from the login-built URL.
            Connection(
                conn_id="testconn",
                conn_type="wasb",
                login="testclientid",
                password="testclientsecret",
                host="testaccountID",
                extra={
                    "account_name": "actualstorage",
                    "tenant_id": "tenant_id",
                },
            ),
            {
                "client_id": "testclientid",
                "client_secret": "testclientsecret",
                "account_name": "actualstorage",
                "tenant_id": "tenant_id",
            },
        ),
        (
            # Service principal with a bare identifier host and no account in
            # extras: nothing may be derived from the client id login, so
            # adlfs can fall back to AZURE_STORAGE_ACCOUNT_NAME.
            Connection(
                conn_id="testconn",
                conn_type="wasb",
                login="testclientid",
                password="testclientsecret",
                host="testaccountID",
                extra={
                    "tenant_id": "tenant_id",
                },
            ),
            {
                "client_id": "testclientid",
                "client_secret": "testclientsecret",
                "tenant_id": "tenant_id",
            },
        ),
        (
            # A Data Lake (dfs) endpoint is an endpoint host, but adlfs is a Blob
            # client: the derived account_host is mapped back to the blob form,
            # which is the default host here, so no account_host is set.
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
            # Same dfs host, with the account in extras: that account decides the
            # URL, as on main, so no account_host may be derived from the host.
            Connection(
                conn_id="testconn",
                conn_type="adls",
                host="testaccountname.dfs.core.windows.net",
                login="client_id",
                password="client_secret",
                extra={
                    "account_name": "otheraccount",
                    "tenant_id": "tenant_id",
                },
            ),
            {
                "account_name": "otheraccount",
                "client_id": "client_id",
                "client_secret": "client_secret",
                "tenant_id": "tenant_id",
            },
        ),
        (
            # Hostnames are case-insensitive: an uppercase dfs host maps and
            # compares like the lowercase one, so the account in extras still
            # decides the URL and no account_host is set.
            Connection(
                conn_id="testconn",
                conn_type="adls",
                host="TestAccountName.DFS.core.windows.net",
                login="client_id",
                password="client_secret",
                extra={
                    "account_name": "otheraccount",
                    "tenant_id": "tenant_id",
                },
            ),
            {
                "account_name": "otheraccount",
                "client_id": "client_id",
                "client_secret": "client_secret",
                "tenant_id": "tenant_id",
            },
        ),
        (
            # A custom domain with a dfs label is not an Azure endpoint, so it
            # is passed on unchanged instead of being mapped to a blob label.
            Connection(
                conn_id="testconn",
                conn_type="wasb",
                host="testaccountname.dfs.example.com",
                login="testaccountname",
                password="account_key",
            ),
            {
                "account_name": "testaccountname",
                "account_host": "testaccountname.dfs.example.com",
                "account_key": "account_key",
            },
        ),
        (
            # The dfs mapping replaces the ".dfs.core." labels wherever they
            # appear and does not depend on the cloud suffix, so a host on a
            # cloud outside the well-known set maps to its blob form too.
            Connection(
                conn_id="testconn",
                conn_type="wasb",
                host="testaccountname.dfs.core.example.com",
                login="testaccountname",
                password="account_key",
            ),
            {
                "account_name": "testaccountname",
                "account_host": "testaccountname.blob.core.example.com",
                "account_key": "account_key",
            },
        ),
        (
            # An explicit port does not stop the mapping: a dfs endpoint with a
            # port maps to the blob endpoint with the same port, which is then
            # not the default host and is passed on.
            Connection(
                conn_id="testconn",
                conn_type="wasb",
                host="https://testaccountname.dfs.core.windows.net:443",
                login="testaccountname",
                password="account_key",
            ),
            {
                "account_name": "testaccountname",
                "account_host": "testaccountname.blob.core.windows.net:443",
                "account_key": "account_key",
            },
        ),
        (
            # A dfs endpoint on a sovereign cloud maps to the blob endpoint of
            # the same cloud, which is not the default host and is passed on.
            Connection(
                conn_id="testconn",
                conn_type="wasb",
                host="testaccountname.dfs.core.chinacloudapi.cn",
                login="testaccountname",
                password="account_key",
            ),
            {
                "account_name": "testaccountname",
                "account_host": "testaccountname.blob.core.chinacloudapi.cn",
                "account_key": "account_key",
            },
        ),
        (
            # A host with a port and no scheme, on a connection without a
            # login: the account comes from the host itself, not from the
            # account URL parse_blob_account_url builds from the login.
            Connection(
                conn_id="testconn",
                conn_type="wasb",
                host="testaccountname.blob.core.windows.net:443",
                password="account_key",
            ),
            {
                "account_name": "testaccountname",
                "account_host": "testaccountname.blob.core.windows.net:443",
                "account_key": "account_key",
            },
        ),
        (
            # On a private-link name the dfs label is not the second label,
            # but the endpoint still maps back to its blob form.
            Connection(
                conn_id="testconn",
                conn_type="adls",
                host="testaccountname.privatelink.dfs.core.windows.net",
                login="client_id",
                password="client_secret",
                extra={
                    "tenant_id": "tenant_id",
                },
            ),
            {
                "account_name": "testaccountname",
                "account_host": "testaccountname.privatelink.blob.core.windows.net",
                "client_id": "client_id",
                "client_secret": "client_secret",
                "tenant_id": "tenant_id",
            },
        ),
        (
            # A dot in the path does not make the host an endpoint: the hostname
            # has no dot, so with a tenant id nothing is derived from the
            # client id login.
            Connection(
                conn_id="testconn",
                conn_type="wasb",
                host="testaccountID/some.path",
                login="testclientid",
                password="testclientsecret",
                extra={
                    "tenant_id": "tenant_id",
                },
            ),
            {
                "client_id": "testclientid",
                "client_secret": "testclientsecret",
                "tenant_id": "tenant_id",
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
            # A mixed-case bare host is still an account name; urlparse lowercases it.
            Connection(
                conn_id="testconn",
                conn_type="adls",
                host="MyAccount",
                login="client_id",
                password="client_secret",
                extra={
                    "tenant_id": "tenant_id",
                },
            ),
            {
                "account_name": "myaccount",
                "client_id": "client_id",
                "client_secret": "client_secret",
                "tenant_id": "tenant_id",
            },
        ),
        (
            Connection(
                conn_id="testconn",
                conn_type="adls",
                host="testaccountname",
                password="account_key",
                extra={},
            ),
            {
                "account_name": "testaccountname",
                "account_key": "account_key",
            },
        ),
        (
            # Custom domain / private endpoint: adls connections set account_host (a real
            # adlfs parameter, carried through the extras overlay) rather than a full URL in
            # host, which adlfs would ignore. This is the path the filesystem docs point at.
            Connection(
                conn_id="testconn",
                conn_type="adls",
                host="testaccountname",
                login="client_id",
                password="client_secret",
                extra={
                    "tenant_id": "tenant_id",
                    "account_host": "custom.blob.core.mydomain.io",
                },
            ),
            {
                "account_name": "testaccountname",
                "account_host": "custom.blob.core.mydomain.io",
                "client_id": "client_id",
                "client_secret": "client_secret",
                "tenant_id": "tenant_id",
            },
        ),
        (
            # No host and no account_name in Extra: get_fs sets no account, so adlfs needs
            # AZURE_STORAGE_ACCOUNT_NAME to connect. This pins the options passed, not a
            # configuration that works on its own; the next case supplies the account.
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


@pytest.mark.parametrize(
    ("host", "expected"),
    [
        ("testaccountname", "testaccountname"),
        # Full endpoints (dotted host, scheme, port or path) are not bare account names, so
        # they never reach the account_name path.
        ("testaccountname.dfs.core.windows.net", None),
        ("https://testaccountname.blob.core.windows.net", None),
        ("testaccountname:443", None),
        ("testaccountname/container", None),
        ("", None),
        (None, None),
    ],
)
def test_bare_account_name_from_host(host, expected):
    assert _bare_account_name_from_host(host) == expected


@pytest.mark.parametrize(
    "mocked_connection",
    [
        # An invalid port is a configuration error: it must raise instead of
        # being dropped silently and connecting to the wrong endpoint.
        Connection(
            conn_id="testconn",
            conn_type="wasb",
            host="testaccountname.blob.core.windows.net:99999",
            password="account_key",
        ),
        Connection(
            conn_id="testconn",
            conn_type="wasb",
            host="testaccountname.blob.core.windows.net:notaport",
            password="account_key",
        ),
    ],
    indirect=["mocked_connection"],
)
def test_get_fs_invalid_port_raises(mocked_connection, mocked_blob_file_system):
    with pytest.raises(ValueError, match="Port"):
        get_fs(mocked_connection.conn_id)


@pytest.mark.parametrize(
    "mocked_connection",
    [
        Connection(
            conn_id="testconn",
            conn_type="wasb",
            login="testaccountname",
            password="testaccountkey",
        ),
        Connection(
            conn_id="testconn",
            conn_type="wasb",
            host="https://testaccountname.blob.core.windows.net",
            password="testaccountkey",
        ),
        # Service principal with a Data Lake endpoint in host: it must build
        # against the blob endpoint, not the dfs one. The tenant id is a GUID
        # because the real credential validates its format.
        Connection(
            conn_id="testconn",
            conn_type="adls",
            host="testaccountname.dfs.core.windows.net",
            login="client_id",
            password="client_secret",
            extra={
                "tenant_id": "11111111-1111-1111-1111-111111111111",
            },
        ),
    ],
    indirect=["mocked_connection"],
)
def test_get_fs_with_real_filesystem(mocked_connection, monkeypatch):
    # The mocked test above cannot catch a keyword adlfs does not accept, so
    # build the real filesystem: with only the account name or URL on the
    # connection it must construct instead of raising
    # "Must provide either a connection_string or account_name".
    monkeypatch.delenv("AZURE_STORAGE_ACCOUNT_NAME", raising=False)
    fs = get_fs(mocked_connection.conn_id)
    assert fs.account_name == "testaccountname"
    assert fs.account_url == "https://testaccountname.blob.core.windows.net"
