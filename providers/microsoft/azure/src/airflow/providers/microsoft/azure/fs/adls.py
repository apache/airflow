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

from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from azure.identity import ClientSecretCredential

from airflow.providers.common.compat.sdk import BaseHook
from airflow.providers.microsoft.azure.utils import get_field, parse_blob_account_url

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem

schemes = ["abfs", "abfss", "adl"]


def _hostname_from_host(host: str | None) -> str | None:
    if not host:
        return None
    return urlparse(host if "://" in host else f"//{host}").hostname


def _account_host_from_host(host: str | None) -> str | None:
    if not host:
        return None
    parsed = urlparse(host if "://" in host else f"//{host}")
    if not parsed.hostname:
        return None
    # adlfs is a Blob client, so a Data Lake (dfs) endpoint is mapped back to
    # its blob form wherever the ".dfs.core." labels appear in the hostname.
    hostname = parsed.hostname.replace(".dfs.core.", ".blob.core.")
    try:
        port = parsed.port
    except ValueError:
        port = None
    return f"{hostname}:{port}" if port is not None else hostname


def _bare_account_name_from_host(host: str | None) -> str | None:
    """
    Return the storage account name when the host is a bare account name, else ``None``.

    A bare host such as ``mystorageaccount`` is the ADLS account name itself. A full host
    like ``mystorageaccount.dfs.core.windows.net`` (or anything with a scheme, port or path)
    is an endpoint instead and goes through the account URL derivation in ``get_fs``.
    """
    # urlparse().hostname is already lowercased, so compare against a lowercased host to keep
    # the check case-insensitive (Azure account names are lowercase anyway). The dot guard
    # keeps full endpoints like "acct.dfs.core.windows.net" out of the account_name path.
    hostname = _hostname_from_host(host)
    if hostname and "." not in hostname and host is not None and hostname == host.lower():
        return hostname
    return None


def get_fs(conn_id: str | None, storage_options: dict[str, Any] | None = None) -> AbstractFileSystem:
    from adlfs import AzureBlobFileSystem

    if conn_id is None:
        return AzureBlobFileSystem()

    conn = BaseHook.get_connection(conn_id)
    extras = conn.extra_dejson
    conn_type = conn.conn_type or "azure_data_lake"

    # connection string always overrides everything else
    connection_string = get_field(
        conn_id=conn_id, conn_type=conn_type, extras=extras, field_name="connection_string"
    )

    if connection_string:
        return AzureBlobFileSystem(connection_string=connection_string)

    # mirror handling of custom field "client_secret_auth_config" from extras. Ignore if missing as AzureBlobFileSystem can handle.
    tenant_id = get_field(conn_id=conn_id, conn_type=conn_type, extras=extras, field_name="tenant_id")
    options: dict[str, Any] = {}
    account_name = _bare_account_name_from_host(conn.host)
    if conn_type == "adls" and account_name:
        # An ADLS Active Directory connection uses ``login`` as the client_id, so a bare host
        # is the storage account name rather than part of an account URL.
        options["account_name"] = account_name
    elif conn_type != "adls" or conn.host or not tenant_id:
        # adlfs has no account_url parameter and would silently drop it; it reads
        # account_name and builds the URL from account_host, so derive both from
        # the host. Only an endpoint host (a hostname with a dot, the same
        # parsing _bare_account_name_from_host uses) supplies an account_host,
        # and only when it is not the default blob host, which adlfs already
        # defaults to. The name and host come from the parsed hostname, not the
        # rebuilt account URL: parse_blob_account_url misreads a host with a
        # port and no scheme as a scheme, and would build the URL from the
        # login instead. A bare identifier is not an endpoint, and for those
        # parse_blob_account_url builds the URL from the login. A bare login is
        # the account name (wasb shared key), but with a tenant id the login is a
        # service principal client id, so without an endpoint host nothing is
        # derived and adlfs falls back to account_name from extras or the
        # AZURE_STORAGE_ACCOUNT_NAME env var.
        conn_hostname = _hostname_from_host(conn.host)
        host_is_endpoint = conn_hostname is not None and "." in conn_hostname
        if host_is_endpoint:
            derived_account_name = conn_hostname.split(".")[0]
            options["account_name"] = derived_account_name
            account_host = _account_host_from_host(conn.host)
            if account_host and account_host != f"{derived_account_name}.blob.core.windows.net":
                options["account_host"] = account_host
        elif conn.login and not tenant_id:
            account_url = urlparse(parse_blob_account_url(conn.host, conn.login))
            if account_url.hostname:
                options["account_name"] = account_url.hostname.split(".")[0]
    # Otherwise (adls + no host + a tenant_id) no account is set here: adlfs needs it from
    # ``account_name`` in Extra or the AZURE_STORAGE_ACCOUNT_NAME environment variable.

    login = conn.login or ""
    password = conn.password or ""
    # assumption (from WasbHook) that if tenant_id is set, we want service principal connection
    if tenant_id:
        client_secret_auth_config = get_field(
            conn_id=conn_id, conn_type=conn_type, extras=extras, field_name="client_secret_auth_config"
        )
        if login:
            options["client_id"] = login
        if password:
            options["client_secret"] = password
        if client_secret_auth_config and login and password:
            options["credential"] = ClientSecretCredential(
                tenant_id=tenant_id, client_id=login, client_secret=password, **client_secret_auth_config
            )

    # if not service principal, then password is taken to be account admin key
    if tenant_id is None and password:
        options["account_key"] = password

    # Now take any fields from extras and overlay them on top of existing options.
    # Add empty field to remove defaults.
    # 'account_host' is included to allow overriding the default Azure Blob endpoint domain
    # (e.g., to use a private endpoint or custom domain instead of core.windows.net).

    fields = [
        "account_name",
        "account_key",
        "sas_token",
        "tenant_id",
        "managed_identity_client_id",
        "workload_identity_client_id",
        "workload_identity_tenant_id",
        "anon",
        "account_host",
    ]
    for field in fields:
        value = get_field(conn_id=conn_id, conn_type=conn_type, extras=extras, field_name=field)
        if value is not None:
            if value == "":
                options.pop(field, "")
            else:
                options[field] = value

    options.update(storage_options or {})

    return AzureBlobFileSystem(**options)
