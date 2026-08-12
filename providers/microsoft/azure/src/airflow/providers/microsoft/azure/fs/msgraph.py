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

from airflow.providers.common.compat.sdk import BaseHook
from airflow.providers.microsoft.azure.utils import get_field

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem

schemes = ["msgraph", "sharepoint", "onedrive", "msgd"]
DEFAULT_SCOPE = "https://graph.microsoft.com/.default"


def _get_token_endpoint(tenant_id: str) -> str:
    return f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"


def _get_scopes(options: dict[str, Any]) -> list[str]:
    scopes = options.get("scope") or options.get("scopes") or DEFAULT_SCOPE
    if isinstance(scopes, str):
        return scopes.split()
    return scopes


def _get_certificate_token(options: dict[str, Any]) -> dict[str, Any]:
    from azure.identity import CertificateCredential

    credential = CertificateCredential(
        tenant_id=options["tenant_id"],
        client_id=options["client_id"],
        password=options.get("password") or options.get("client_secret"),
        certificate_path=options.get("certificate_path"),
        certificate_data=options["certificate_data"].encode() if options.get("certificate_data") else None,
        authority=options.get("authority"),
        disable_instance_discovery=options.get("disable_instance_discovery", False),
    )
    try:
        access_token = credential.get_token(*_get_scopes(options))
    finally:
        credential.close()

    return {
        "access_token": access_token.token,
        "token_type": "Bearer",
        "expires_at": access_token.expires_on,
    }


def get_fs(conn_id: str | None, storage_options: dict[str, Any] | None = None) -> AbstractFileSystem:
    from msgraphfs import MSGDriveFS

    if conn_id is None:
        return MSGDriveFS({})

    conn = BaseHook.get_connection(conn_id)
    extras = conn.extra_dejson
    conn_type = conn.conn_type or "msgraph"

    options: dict[str, Any] = {}

    # Get authentication parameters with fallback handling
    client_id = conn.login or get_field(
        conn_id=conn_id, conn_type=conn_type, extras=extras, field_name="client_id"
    )
    client_secret = conn.password or get_field(
        conn_id=conn_id, conn_type=conn_type, extras=extras, field_name="client_secret"
    )
    tenant_id = conn.host or get_field(
        conn_id=conn_id, conn_type=conn_type, extras=extras, field_name="tenant_id"
    )

    if client_id:
        options["client_id"] = client_id
    if client_secret:
        options["client_secret"] = client_secret
    if tenant_id:
        options["tenant_id"] = tenant_id

    # Process additional fields from extras
    fields = [
        "drive_id",
        "scope",
        "scopes",
        "token_endpoint",
        "redirect_uri",
        "token_endpoint_auth_method",
        "code_challenge_method",
        "update_token",
        "username",
        "password",
        "certificate_path",
        "certificate_data",
        "authority",
        "disable_instance_discovery",
    ]
    for field in fields:
        value = get_field(conn_id=conn_id, conn_type=conn_type, extras=extras, field_name=field)
        if value is not None:
            if value == "":
                options.pop(field, "")
            else:
                options[field] = value

    # Update with storage options
    options.update(storage_options or {})

    # Create oauth2 client parameters if authentication is provided
    oauth2_client_params = {}
    if (
        options.get("client_id")
        and options.get("tenant_id")
        and (options.get("certificate_path") or options.get("certificate_data"))
    ):
        token_endpoint = options.get("token_endpoint") or _get_token_endpoint(options["tenant_id"])
        oauth2_client_params = {
            "client_id": options["client_id"],
            "token": _get_certificate_token(options),
            "token_endpoint": token_endpoint,
            "scope": " ".join(_get_scopes(options)),
        }
    elif options.get("client_id") and options.get("client_secret") and options.get("tenant_id"):
        oauth2_client_params = {
            "client_id": options["client_id"],
            "client_secret": options["client_secret"],
            "tenant_id": options["tenant_id"],
        }

        # Add additional oauth2 parameters supported by authlib
        oauth2_params = [
            "scope",
            "token_endpoint",
            "redirect_uri",
            "token_endpoint_auth_method",
            "code_challenge_method",
            "update_token",
            "username",
            "password",
        ]
        for param in oauth2_params:
            if param in options:
                oauth2_client_params[param] = options[param]

        if "scopes" in options and "scope" not in oauth2_client_params:
            oauth2_client_params["scope"] = " ".join(_get_scopes(options))

        # Construct default token_endpoint from tenant_id if not explicitly provided
        if "token_endpoint" not in oauth2_client_params:
            oauth2_client_params["token_endpoint"] = _get_token_endpoint(options["tenant_id"])

    # Determine which filesystem to return based on drive_id
    drive_id = options.get("drive_id")

    return MSGDriveFS(drive_id=drive_id, oauth2_client_params=oauth2_client_params)
