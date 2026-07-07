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
from airflow.exceptions import AirflowException
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import BaseHook
from airflow.providers.microsoft.azure.utils import get_field

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem

schemes = ["msgraph", "sharepoint", "onedrive", "msgd"]

#: Default scope requested when fetching a certificate-based access token.
#: Matches ``KiotaRequestAdapterHook.DEFAULT_SCOPE``.
DEFAULT_SCOPE = "https://graph.microsoft.com/.default"


def _get_certificate_token(
    *,
    tenant_id: str,
    client_id: str,
    certificate_path: str | None,
    certificate_data: str | None,
    certificate_password: str | None,
    scope: str,
) -> dict[str, Any]:
    """
    Fetch a single access token using certificate-based auth (azure-identity).

    ``msgraphfs`` builds its HTTP client on top of authlib's ``AsyncOAuth2Client``,
    which only understands client-secret based OAuth2 grants, not azure-identity
    credential objects. To support certificate auth, we fetch one access token
    up front using ``azure.identity.CertificateCredential`` (the same credential
    class already used for certificate auth in
    :class:`~airflow.providers.microsoft.azure.hooks.msgraph.KiotaRequestAdapterHook`)
    and hand it to authlib as a static bearer token.

    .. note::
        Because authlib is not driving the token acquisition, it cannot silently
        refresh this token using the certificate once it expires (Azure AD access
        tokens are typically valid ~60-90 minutes). Tasks that run longer than the
        token's lifetime will need to re-instantiate the filesystem (e.g. by
        re-resolving the ``ObjectStoragePath``) to obtain a fresh token.
    """
    from azure.identity import CertificateCredential

    credential = CertificateCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        certificate_path=certificate_path,
        certificate_data=certificate_data.encode() if certificate_data else None,
        password=certificate_password,
    )
    access_token = credential.get_token(scope)
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
    # Certificate-based auth, mirroring KiotaRequestAdapterHook's field names so the
    # same connection can be reused across the msgraph hook/operator/sensor and this
    # filesystem. When set, these take precedence over client_secret.
    certificate_path = get_field(
        conn_id=conn_id, conn_type=conn_type, extras=extras, field_name="certificate_path"
    )
    certificate_data = get_field(
        conn_id=conn_id, conn_type=conn_type, extras=extras, field_name="certificate_data"
    )

    if client_id:
        options["client_id"] = client_id
    if client_secret:
        options["client_secret"] = client_secret
    if tenant_id:
        options["tenant_id"] = tenant_id
    if certificate_path:
        options["certificate_path"] = certificate_path
    if certificate_data:
        options["certificate_data"] = certificate_data

    # Process additional fields from extras
    fields = [
        "drive_id",
        "scope",
        "token_endpoint",
        "redirect_uri",
        "token_endpoint_auth_method",
        "code_challenge_method",
        "update_token",
        "username",
        "password",
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
    use_certificate = options.get("certificate_path") or options.get("certificate_data")

    if use_certificate and options.get("client_id") and options.get("tenant_id"):
        # Certificate auth: fetch a single access token via azure-identity and hand
        # it to authlib as a static bearer token (see _get_certificate_token above).
        scope = options.get("scope", DEFAULT_SCOPE)
        try:
            token = _get_certificate_token(
                tenant_id=options["tenant_id"],
                client_id=options["client_id"],
                certificate_path=options.get("certificate_path"),
                certificate_data=options.get("certificate_data"),
                # The certificate's private key password reuses the same connection
                # field as client_secret (conn.password), matching the semantics of
                # `connection.password` in KiotaRequestAdapterHook.get_credentials.
                certificate_password=client_secret,
                scope=scope,
            )
        except Exception as e:
            raise AirflowException(
                f"Failed to acquire a Microsoft Graph access token using the configured "
                f"certificate for conn_id {conn_id!r}: {e}"
            ) from e

        oauth2_client_params = {
            "client_id": options["client_id"],
            "token_endpoint": options.get(
                "token_endpoint",
                f"https://login.microsoftonline.com/{options['tenant_id']}/oauth2/v2.0/token",
            ),
            "token": token,
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

    # Determine which filesystem to return based on drive_id
    drive_id = options.get("drive_id")

    return MSGDriveFS(drive_id=drive_id, oauth2_client_params=oauth2_client_params)
