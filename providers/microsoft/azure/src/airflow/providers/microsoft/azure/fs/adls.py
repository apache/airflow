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


def _account_name_from_host(host: str | None) -> str | None:
    if not host:
        return None

    parsed_url = urlparse(host if "://" in host else f"//{host}")
    hostname = parsed_url.hostname
    if not hostname:
        return None

    return hostname.split(".", 1)[0]


def _storage_account_name_from_host(host: str | None) -> str | None:
    account_name = _account_name_from_host(host)
    if account_name and account_name == host:
        return account_name
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
    account_name = _storage_account_name_from_host(conn.host)
    # Preserve account_url for existing WASB, full URL, and custom endpoint connections.
    # ADLS Active Directory connections use login as client_id, so bare hosts are account names.
    if conn_type == "adls" and account_name:
        options["account_name"] = account_name
    elif conn_type != "adls" or conn.host or not tenant_id:
        options["account_url"] = parse_blob_account_url(conn.host, conn.login)

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
