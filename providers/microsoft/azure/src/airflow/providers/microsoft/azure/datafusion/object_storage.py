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
import re
from typing import Any
from urllib.parse import parse_qsl, urlsplit

from datafusion.object_store import MicrosoftAzure

from airflow.providers.common.compat.sdk import BaseHook
from airflow.providers.common.sql.config import ConnectionConfig, StorageType
from airflow.providers.common.sql.datafusion.base import ObjectStorageProvider
from airflow.providers.common.sql.datafusion.exceptions import ObjectStoreCreationException
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

_ABFS_URI_RE = re.compile(
    r"^abfss?://(?P<container>[^@/]+)@(?P<account>[^./]+)\.(?P<host>[^/]+)(?P<path>/.*)?$"
)
# MicrosoftAzure has no endpoint kwarg, so a non-public host only reaches the right cloud when
# AZURE_STORAGE_ENDPOINT/AZURE_ENDPOINT redirects the binding; same rule as _resolve_wasb_account.
_AZURE_PUBLIC_HOST_SUFFIX = ".core.windows.net"
_AZURE_BLOB_PUBLIC_SUFFIX = ".blob.core.windows.net"
_ABFS_SCHEMES = ("abfs://", "abfss://")

_UNSUPPORTED_CONNECTION_FIELDS = (
    "connection_string",
    "managed_identity_client_id",
    "workload_identity_tenant_id",
)

# object_store's build() precedence, high to low: bearer token > access key > workload
# identity (client_id+tenant_id+federated_token_file) > client secret
# (client_id+client_secret+tenant_id) > SAS. Used by _find_conflicting_azure_env_vars.
_AZURE_ENV_BEARER_VARS = ("AZURE_STORAGE_TOKEN",)
_AZURE_ENV_ACCESS_KEY_VARS = (
    "AZURE_STORAGE_ACCOUNT_KEY",
    "AZURE_STORAGE_ACCESS_KEY",
    "AZURE_STORAGE_MASTER_KEY",
)
_AZURE_ENV_CLIENT_ID_VARS = ("AZURE_STORAGE_CLIENT_ID", "AZURE_CLIENT_ID")
_AZURE_ENV_CLIENT_SECRET_VARS = ("AZURE_STORAGE_CLIENT_SECRET", "AZURE_CLIENT_SECRET")
_AZURE_ENV_TENANT_ID_VARS = (
    "AZURE_STORAGE_TENANT_ID",
    "AZURE_STORAGE_AUTHORITY_ID",
    "AZURE_TENANT_ID",
    "AZURE_AUTHORITY_ID",
)
_AZURE_ENV_FEDERATED_TOKEN_FILE_VAR = "AZURE_FEDERATED_TOKEN_FILE"


def _find_conflicting_azure_env_vars(credential_tier: str) -> list[str]:
    """
    Return worker env vars that would silently outrank the connection's own credential.

    The binding always calls `from_env()` with no way to skip it, so this can only be
    caught here, not avoided. The connection's fields overwrite the same fields from
    `from_env()`, so only a tier above the connection's own, or an unoccupied tier
    (workload identity or client secret, for SAS) with *all* its fields present, can
    actually take over.
    """

    def env_set(*var_groups: tuple[str, ...]) -> list[str]:
        return [var for group in var_groups for var in group if os.environ.get(var)]

    conflicting = env_set(_AZURE_ENV_BEARER_VARS)
    if credential_tier == "access_key":
        return conflicting
    conflicting += env_set(_AZURE_ENV_ACCESS_KEY_VARS)
    if credential_tier == "client_secret":
        # client_id and tenant_id are already the connection's own; only the federated
        # token file is left for env to complete the workload-identity triple with.
        if os.environ.get(_AZURE_ENV_FEDERATED_TOKEN_FILE_VAR):
            conflicting.append(_AZURE_ENV_FEDERATED_TOKEN_FILE_VAR)
        return conflicting
    # SAS occupies none of these fields, so each whole triple must come from env.
    if (
        any(os.environ.get(var) for var in _AZURE_ENV_CLIENT_ID_VARS)
        and any(os.environ.get(var) for var in _AZURE_ENV_TENANT_ID_VARS)
        and os.environ.get(_AZURE_ENV_FEDERATED_TOKEN_FILE_VAR)
    ):
        conflicting += env_set(_AZURE_ENV_CLIENT_ID_VARS, _AZURE_ENV_TENANT_ID_VARS)
        conflicting.append(_AZURE_ENV_FEDERATED_TOKEN_FILE_VAR)
    if (
        any(os.environ.get(var) for var in _AZURE_ENV_CLIENT_ID_VARS)
        and any(os.environ.get(var) for var in _AZURE_ENV_CLIENT_SECRET_VARS)
        and any(os.environ.get(var) for var in _AZURE_ENV_TENANT_ID_VARS)
    ):
        conflicting += env_set(
            _AZURE_ENV_CLIENT_ID_VARS,
            _AZURE_ENV_CLIENT_SECRET_VARS,
            _AZURE_ENV_TENANT_ID_VARS,
        )
    return list(dict.fromkeys(conflicting))


def _resolve_wasb_account(host: str | None, login: str | None) -> str | None:
    """
    Return the storage account name the way WasbHook resolves it.

    From ``host`` (its netloc's first label) when set, falling back to ``login`` only when
    ``host`` is empty -- login holds the service-principal client_id otherwise, not the
    account name (the caller drops the login-derived account for service principals).
    Returns ``None`` when neither is set, so the binding falls back to
    ``AZURE_STORAGE_ACCOUNT_NAME`` rather than the literal string ``"None"``. Not built on
    :func:`~airflow.providers.microsoft.azure.utils.parse_blob_account_url`, which reads a
    ``host:port`` netloc as a URL scheme and so cannot reject emulator addresses.

    Only the public ``*.blob.core.windows.net`` cloud is supported, unless
    ``AZURE_STORAGE_ENDPOINT``/``AZURE_ENDPOINT`` is set for a real sovereign-cloud hostname
    (DataFusion's binding otherwise has no endpoint override). A ``host:port`` netloc (the
    Azurite emulator's shape) is never a real hostname, so it always raises instead --
    checked before the dotless-host fallback below, which would otherwise mask it.
    """
    if not host and not login:
        return None
    netloc = urlsplit(host if host else f"https://{login}{_AZURE_BLOB_PUBLIC_SUFFIX}/").netloc
    if not netloc:
        # No scheme was given (e.g. a bare DNS name); urlsplit put it all in the path instead.
        netloc = urlsplit(f"https://{host}").netloc
    if host and ":" in netloc:
        raise ValueError(
            f"Connection host {host!r} looks like an emulator address (host:port), which "
            "DataFusion's Azure Blob Storage binding cannot resolve an account name from -- "
            "even with AZURE_STORAGE_ENDPOINT set. Put the account name in `login` with "
            "`host` empty instead, alongside AZURE_STORAGE_ENDPOINT and AZURE_ALLOW_HTTP=true."
        )
    if "." not in netloc:
        if not login:
            raise ValueError(
                f"Connection host {host!r} is not a full URL or DNS name, and no `login` was "
                "given to resolve it as an Active Directory ID instead."
            )
        # Only an Active Directory ID was given, not a full URL or DNS name.
        netloc = f"{login}{_AZURE_BLOB_PUBLIC_SUFFIX}"
    if not netloc.endswith(_AZURE_BLOB_PUBLIC_SUFFIX) and not (
        os.environ.get("AZURE_STORAGE_ENDPOINT") or os.environ.get("AZURE_ENDPOINT")
    ):
        raise ValueError(
            f"Connection host {host!r} does not resolve to the public {_AZURE_BLOB_PUBLIC_SUFFIX} "
            "cloud, which is the only one DataFusion's Azure Blob Storage binding can target (it "
            "has no endpoint override). A sovereign cloud is supported once the "
            "AZURE_STORAGE_ENDPOINT environment variable is set."
        )
    # Azure storage account names are capped at 24 characters.
    return netloc.split(".", 1)[0][:24]


class AzureObjectStorageProvider(ObjectStorageProvider):
    """
    Azure Object Storage Provider using DataFusion's MicrosoftAzure.

    ``ObjectStoragePath`` reaches ``abfs://`` through :mod:`airflow.providers.microsoft.azure.fs.adls`
    instead, which hands ``WasbHook``-style credentials to adlfs. DataFusion's ``MicrosoftAzure``
    supports a narrower set, so this class reads service principal (``tenant_id`` + login + password),
    ``shared_access_key``, ``sas_token``, account key and ambient credentials, and rejects
    ``connection_string``, ``managed_identity_client_id`` and ``workload_identity_tenant_id``.
    Keep the two in mind together when changing how a ``wasb`` connection is read.
    """

    @property
    def get_storage_type(self) -> StorageType:
        """Return the storage type."""
        return StorageType.AZURE

    def get_bucket(self, path: str) -> str | None:
        """Extract the container name, from either the ``az://`` or ``abfs(s)://`` URI shape."""
        if match := _ABFS_URI_RE.match(path):
            return match.group("container")
        if path.startswith(_ABFS_SCHEMES):
            raise ValueError(
                f"{path!r} does not match the required abfs(s)://<container>@<account>.<host>/<path> shape"
            )
        return super().get_bucket(path)

    def _validate_uri_host(self, path: str) -> None:
        match = _ABFS_URI_RE.match(path)
        if not match:
            return
        host = match.group("host").lower()
        if host == _AZURE_PUBLIC_HOST_SUFFIX[1:] or host.endswith(_AZURE_PUBLIC_HOST_SUFFIX):
            return
        if os.environ.get("AZURE_STORAGE_ENDPOINT") or os.environ.get("AZURE_ENDPOINT"):
            return
        raise ValueError(
            f"URI {path!r} host {match.group('host')!r} is not a public *{_AZURE_PUBLIC_HOST_SUFFIX} "
            "endpoint, and DataFusion's Azure Blob Storage binding has no endpoint override, so the "
            "request would go to the public cloud. A sovereign cloud is supported once the "
            "AZURE_STORAGE_ENDPOINT environment variable is set."
        )

    def _get_uri_account(self, path: str) -> str | None:
        match = _ABFS_URI_RE.match(path)
        return match.group("account") if match else None

    def normalize_uri(self, path: str) -> str:
        """
        Rewrite ``abfs(s)://<container>@<account>.<host>/<path>`` to ``az://<account>.<container>/<path>``.

        DataFusion's registry keys on (schema, host) alone, so container-only would collide for
        two different accounts sharing a container name. Account and container names never
        contain a dot, so joining on one is unambiguous. ``az://`` URIs pass through unchanged.
        """
        if match := _ABFS_URI_RE.match(path):
            return f"az://{match.group('account')}.{match.group('container')}{match.group('path') or ''}"
        return path

    def _credentials_from_connection(self, conn_id: str) -> dict[str, Any]:
        """Derive Azure credentials from an Airflow connection; only ``wasb`` connections are supported."""
        conn = BaseHook.get_connection(conn_id)
        if conn.conn_type != "wasb":
            raise ValueError(f"Unknown connection type {conn.conn_type}")

        hook = WasbHook(wasb_conn_id=conn_id)
        extra_dejson = conn.extra_dejson

        def get_field(field_name: str) -> Any:
            return hook._get_field(extra_dejson, field_name)

        for unsupported_field in _UNSUPPORTED_CONNECTION_FIELDS:
            if get_field(unsupported_field):
                raise ValueError(
                    f"Connection field {unsupported_field!r} is not supported for DataFusion "
                    "Azure Blob Storage access; only tenant_id+login+password (service "
                    "principal), sas_token, shared_access_key/account_key/password, or ambient "
                    "credentials (AZURE_* environment variables, managed identity, workload "
                    "identity, or az login) are used."
                )
        credentials: dict[str, Any] = {"account": _resolve_wasb_account(conn.host, conn.login)}
        credential_tier: str | None = None
        if tenant_id := get_field("tenant_id"):
            if not conn.login or not conn.password:
                # Falling through here would silently switch identity (ambient auth, or
                # the client secret sent as a shared key) instead of failing clearly.
                missing = "login (client_id)" if not conn.login else "password (client_secret)"
                raise ValueError(
                    f"Connection extra 'tenant_id' is set for DataFusion Azure Blob Storage "
                    f"service-principal auth, but {missing} is not."
                )
            credentials.update(
                {"client_id": conn.login, "client_secret": conn.password, "tenant_id": tenant_id}
            )
            if not conn.host:
                # login holds the client_id here, so it cannot name the storage account.
                credentials["account"] = None
            credential_tier = "client_secret"
        elif shared_access_key := get_field("shared_access_key"):
            # Checked ahead of sas_token to match WasbHook.get_conn's precedence.
            credentials["access_key"] = shared_access_key
            credential_tier = "access_key"
        elif sas_token := get_field("sas_token"):
            if sas_token.startswith("http"):
                raise ValueError(
                    "A URL-form `sas_token` is not supported for DataFusion Azure Blob Storage "
                    "access; provide the SAS token as a query string instead."
                )
            credentials["sas_query_pairs"] = parse_qsl(sas_token.lstrip("?"))
            credential_tier = "sas"
        elif access_key := conn.password or get_field("account_key"):
            credentials["access_key"] = access_key
            credential_tier = "access_key"

        if credential_tier is not None:
            if conflicting_env_vars := _find_conflicting_azure_env_vars(credential_tier):
                raise ValueError(
                    f"Worker environment variable(s) {', '.join(conflicting_env_vars)} would "
                    "silently take precedence over this connection's explicit credential in "
                    "DataFusion's Azure Blob Storage binding. Unset them on the worker, or "
                    "remove the explicit credential from this connection to rely on the "
                    "environment instead."
                )
        return {k: v for k, v in credentials.items() if v is not None}

    def create_object_store(self, path: str, connection_config: ConnectionConfig | None = None):
        """
        Create an Azure object store using DataFusion's MicrosoftAzure.

        Credentials supplied on ``connection_config`` take precedence. Only when
        ``connection_config.credentials`` is empty are they resolved from the ``wasb``
        Airflow connection named by ``conn_id``.
        """
        if connection_config is None:
            raise ValueError(f"connection_config must be provided for {self.get_storage_type.value}")

        credentials = connection_config.credentials
        if not credentials:
            credentials = self._credentials_from_connection(connection_config.conn_id)

        try:
            container = self.get_bucket(path)

            self._validate_uri_host(path)
            uri_account = self._get_uri_account(path)
            resolved_account = credentials.get("account")
            if uri_account and resolved_account and uri_account.lower() != resolved_account.lower():
                raise ValueError(
                    f"URI {path!r} names storage account {uri_account!r}, but connection "
                    f"{connection_config.conn_id!r} resolves to account {resolved_account!r}. Point "
                    "the URI and the connection at the same account, or omit the account from one of "
                    "them."
                )
            if uri_account and not resolved_account:
                # Pins the URI's account; MicrosoftAzure otherwise falls back to AZURE_STORAGE_ACCOUNT_NAME,
                # which can name a different account.
                credentials = {**credentials, "account": uri_account}

            azure_store = MicrosoftAzure(container_name=container, **credentials)
            self.log.info("Created Azure object store for container %s", container)

            return azure_store

        except BaseException as e:
            # A bad credential combination panics as pyo3_runtime.PanicException, not a plain Exception.
            raise ObjectStoreCreationException(f"Failed to create Azure object store: {e}")

    def get_scheme(self) -> str:
        """Return the scheme for Azure; ``normalize_uri`` rewrites ``abfs(s)://`` URIs onto it."""
        return "az://"
