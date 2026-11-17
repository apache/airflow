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
"""Hooks for the Apache Polaris Management API and Polaris-only catalog APIs."""

from __future__ import annotations

import base64
import json
import random
import threading
import time
from dataclasses import dataclass, field
from functools import cached_property
from typing import TYPE_CHECKING, Any, Literal, TypeVar, cast, get_args
from urllib.parse import urlsplit, urlunsplit

import requests
import urllib3
from apache_polaris.sdk.catalog import (
    ApiClient as CatalogApiClient,
    Configuration as CatalogConfiguration,
    IcebergCatalogAPI,
    IcebergConfigurationAPI,
)
from apache_polaris.sdk.catalog.api.catalog_api import CatalogAPI
from apache_polaris.sdk.catalog.api.policy_api import PolicyAPI
from apache_polaris.sdk.catalog.exceptions import ApiException as CatalogApiException
from apache_polaris.sdk.catalog.models.assert_table_uuid import AssertTableUUID
from apache_polaris.sdk.catalog.models.attach_policy_request import AttachPolicyRequest
from apache_polaris.sdk.catalog.models.commit_table_request import CommitTableRequest
from apache_polaris.sdk.catalog.models.create_namespace_request import CreateNamespaceRequest
from apache_polaris.sdk.catalog.models.create_policy_request import CreatePolicyRequest
from apache_polaris.sdk.catalog.models.create_table_request import CreateTableRequest
from apache_polaris.sdk.catalog.models.detach_policy_request import DetachPolicyRequest
from apache_polaris.sdk.catalog.models.notification_request import NotificationRequest
from apache_polaris.sdk.catalog.models.policy_attachment_target import PolicyAttachmentTarget
from apache_polaris.sdk.catalog.models.register_table_request import RegisterTableRequest
from apache_polaris.sdk.catalog.models.remove_properties_update import RemovePropertiesUpdate
from apache_polaris.sdk.catalog.models.rename_table_request import RenameTableRequest
from apache_polaris.sdk.catalog.models.set_properties_update import SetPropertiesUpdate
from apache_polaris.sdk.catalog.models.table_identifier import TableIdentifier
from apache_polaris.sdk.catalog.models.table_update import TableUpdate
from apache_polaris.sdk.catalog.models.update_namespace_properties_request import (
    UpdateNamespacePropertiesRequest,
)
from apache_polaris.sdk.catalog.models.update_policy_request import UpdatePolicyRequest
from apache_polaris.sdk.management import (
    AddGrantRequest,
    ApiClient as ManagementApiClient,
    Catalog,
    CatalogRole,
    Configuration as ManagementConfiguration,
    CreateCatalogRequest,
    CreateCatalogRoleRequest,
    CreatePrincipalRequest,
    CreatePrincipalRoleRequest,
    GrantCatalogRoleRequest,
    GrantPrincipalRoleRequest,
    GrantResource,
    PolarisDefaultApi,
    Principal,
    PrincipalRole,
    RevokeGrantRequest,
    StorageConfigInfo,
    UpdateCatalogRequest,
)
from apache_polaris.sdk.management.exceptions import ApiException as ManagementApiException
from pydantic import PrivateAttr

from airflow.providers.apache.polaris.exceptions import (
    PolarisApiError,
    PolarisAuthenticationError,
    PolarisConflictError,
    PolarisConnectionError,
    PolarisForbiddenError,
    PolarisInvalidArgumentError,
    PolarisNotFoundError,
    PolarisRequestError,
    is_retryable_status,
)
from airflow.providers.common.compat.sdk import (
    AirflowOptionalProviderFeatureException,
    BaseHook,
    mask_secret,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from apache_polaris.sdk.catalog.models.applicable_policy import ApplicablePolicy
    from apache_polaris.sdk.catalog.models.load_policy_response import LoadPolicyResponse
    from apache_polaris.sdk.catalog.models.policy import Policy
    from apache_polaris.sdk.catalog.models.policy_identifier import PolicyIdentifier
    from apache_polaris.sdk.management import PrincipalWithCredentials
    from pyiceberg.catalog.rest import RestCatalog

    from airflow.providers.common.compat.sdk import Connection

AuthType = Literal["internal", "oidc", "token"]
ClientAuthMethod = Literal["client_secret_basic", "client_secret_post"]
IfExists = Literal["fail", "skip"]
IfMissing = Literal["fail", "skip"]
IfPolicyExists = Literal["fail", "skip", "update"]

DEFAULT_REALM_HEADER = "Polaris-Realm"
DEFAULT_SCOPE = "PRINCIPAL_ROLE:ALL"
PRINCIPAL_ROLE_PREFIX = "PRINCIPAL_ROLE:"
DEFAULT_CATALOG_PATH = "/api/catalog"
DEFAULT_MANAGEMENT_PATH = "/api/management/v1"
INTERNAL_TOKEN_PATH = "/v1/oauth/tokens"
DEFAULT_TIMEOUT = 30.0

#: Refresh a cached token this many seconds before it expires.
TOKEN_REFRESH_MARGIN = 60
#: Cache lifetime for a token whose response has neither ``expires_in`` nor a JWT ``exp`` claim.
FALLBACK_TOKEN_TTL = 300

#: Multi-level namespaces travel in URL paths joined with the ASCII unit separator.
NAMESPACE_SEPARATOR = "\x1f"
REALM_ERROR_TYPE = "MissingOrInvalidRealm"
#: Polaris answers HTTP 400 with this type when revoking a role assignment or grant that is absent.
GRANT_NOT_FOUND_TYPE = "GRANT_NOT_FOUND"
#: How often an update is attempted when Polaris keeps rejecting it as based on a stale version.
UPDATE_ATTEMPTS = 3
#: Range, in seconds, of the random pause before such a retry (see _pause_before_retry).
UPDATE_RETRY_PAUSE = (0.1, 1.0)
#: How often a create with ``if_exists`` is attempted when the entity Polaris reports as existing
#: cannot be read (see PolarisHook._create_or_get).
CREATE_ATTEMPTS = 2
#: Token request failures caused by the connection's URL, which a retry cannot fix.
_INVALID_URL_ERRORS = (
    requests.exceptions.InvalidURL,
    requests.exceptions.InvalidSchema,
    requests.exceptions.MissingSchema,
)

#: Iceberg Java catalog property -> Trino ``iceberg.rest-catalog.*`` property.
_TRINO_KEYS = {
    "uri": "uri",
    "warehouse": "warehouse",
    "token": "oauth2.token",
    "oauth2-server-uri": "oauth2.server-uri",
    "scope": "oauth2.scope",
}

_SDK_API_EXCEPTIONS = (CatalogApiException, ManagementApiException)
_T = TypeVar("_T")
_M = TypeVar("_M")
_C = TypeVar("_C", CatalogApiClient, ManagementApiClient)


def _as_bool_or_path(value: Any) -> bool | str:
    """Parse ``verify``: a bool, a ``"true"``/``"false"`` string (URI-form connections), or a CA path."""
    if value is None or value == "":
        return True
    if isinstance(value, bool):
        return value
    lowered = str(value).strip().lower()
    if lowered in ("true", "1", "yes"):
        return True
    if lowered in ("false", "0", "no"):
        return False
    return str(value)


def namespace_parts(namespace: str | Sequence[str]) -> list[str]:
    """Split ``"a.b"`` into ``["a", "b"]``; pass a list to use names that contain dots."""
    parts = namespace.split(".") if isinstance(namespace, str) else list(namespace)
    if not parts or any(not part for part in parts):
        raise PolarisInvalidArgumentError(f"Invalid namespace: {namespace!r}")
    return parts


def encode_namespace(namespace: str | Sequence[str]) -> str:
    """Encode a namespace for a Polaris URL path parameter (levels joined by the unit separator)."""
    return NAMESPACE_SEPARATOR.join(namespace_parts(namespace))


def _jwt_expiry(token: str) -> float | None:
    """
    Return the ``exp`` claim of a JWT as epoch seconds, or ``None``.

    The signature is not verified: the value is only used to schedule a refresh, never to trust
    the token.
    """
    try:
        payload = token.split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
        exp = claims.get("exp")
        return float(exp) if exp is not None else None
    except (IndexError, ValueError, TypeError, AttributeError):
        return None


def _model(model_cls: type[_M], value: _M | dict[str, Any]) -> _M:
    """
    Return ``value`` as an SDK model, building it from Polaris's JSON shape if it is a dict.

    A value the SDK cannot build the model from raises :class:`PolarisInvalidArgumentError`, so
    operators fail without retrying instead of retrying a pydantic or ``KeyError``.
    """
    if isinstance(value, model_cls):
        return value
    name = model_cls.__name__
    if not isinstance(value, dict):
        raise PolarisInvalidArgumentError(
            f"Expected a {name} or a dict, got {type(value).__name__}.{_template_hint(value)}"
        )
    try:
        return cast("_M", model_cls.from_dict(value))  # type: ignore[attr-defined]
    except (KeyError, TypeError, ValueError) as e:  # pydantic's ValidationError is a ValueError
        detail = f"missing key {e}" if isinstance(e, KeyError) else str(e).splitlines()[0]
        raise PolarisInvalidArgumentError(f"Invalid {name}: {detail}.") from e


def _json_equal(new: str | None, current: str | None) -> bool:
    """Compare policy contents as JSON; ``new`` must be valid JSON."""
    if new is None or current is None:
        return new == current
    try:
        new_value = json.loads(new)
    except ValueError:
        raise PolarisInvalidArgumentError(f"Policy content is not valid JSON: {new[:200]!r}") from None
    try:
        return new_value == json.loads(current)
    except ValueError:
        return False


def _pause_before_retry() -> None:
    """
    Wait a short, random time before retrying an update that lost a race.

    Writers that collided on one version would otherwise all re-read and write again at once and
    collide again; a random pause spreads them out, so one after the other succeeds.
    """
    time.sleep(random.uniform(*UPDATE_RETRY_PAUSE))


def _template_hint(value: Any) -> str:
    """Return a hint for a value that should be a dict or list but arrived as text (a template)."""
    if isinstance(value, str):
        return " If it comes from a template, set render_template_as_native_obj=True on the Dag."
    return ""


def _json_object(name: str, value: Any) -> dict[str, Any]:
    """Check that ``value`` is a JSON object (dict), with a hint for templates rendered as text."""
    if isinstance(value, dict):
        return value
    raise PolarisInvalidArgumentError(
        f"{name} must be a dict, got {type(value).__name__}.{_template_hint(value)}"
    )


def _string_map(name: str, value: Any) -> dict[str, str]:
    """Check that ``value`` maps strings to strings, as Iceberg and Polaris properties do."""
    value = _json_object(name, value)
    invalid = sorted(str(key) for key, item in value.items() if not isinstance(item, str))
    if invalid:
        raise PolarisInvalidArgumentError(
            f"{name} values must be strings (Iceberg properties are text); not for: {', '.join(invalid)}."
        )
    return value


def _optional_string_map(value: Any) -> dict[str, str] | None:
    """Check entity ``properties``, which may be omitted."""
    return None if value is None else _string_map("properties", value)


def _string_list(name: str, value: Any) -> list[str]:
    """Check that ``value`` is a list of strings; a bare string is refused, not split into letters."""
    if isinstance(value, (list, tuple)) and all(isinstance(item, str) for item in value):
        return list(value)
    raise PolarisInvalidArgumentError(
        f"{name} must be a list of strings, got {value!r}.{_template_hint(value)}"
    )


def _property_changes(updates: Any, removals: Any) -> tuple[dict[str, str], list[str]]:
    """Validate property updates and removals; a key cannot be in both (the Iceberg REST spec's 422)."""
    updates = _string_map("updates", updates) if updates is not None else {}
    removals = _string_list("removals", removals) if removals is not None else []
    if overlap := sorted(set(updates) & set(removals)):
        raise PolarisInvalidArgumentError(
            f"Properties cannot be both updated and removed: {', '.join(overlap)}."
        )
    return updates, removals


class _Verbatim:
    """
    Mixin for an SDK request model whose body is sent exactly as given, bypassing the SDK's models.

    The generated models drop or reject valid fields: ``spec-id`` and ``order-id`` (read-only in
    the Iceberg REST OpenAPI spec, but required by Polaris) and Iceberg v3 field defaults in table
    definitions, and the connection details of federated catalogs (``remoteCatalogName``,
    authentication parameters). Polaris validates the body itself.
    """

    _body: dict[str, Any] = PrivateAttr(default_factory=dict)

    @classmethod
    def of(cls, body: dict[str, Any]) -> Any:
        request = cls.model_construct()  # type: ignore[attr-defined]
        request._body = body
        return request

    def to_dict(self) -> dict[str, Any]:
        return self._body


class _VerbatimCreateTableRequest(_Verbatim, CreateTableRequest):
    """A ``CreateTableRequest`` sent as given."""


class _VerbatimCreateCatalogRequest(_Verbatim, CreateCatalogRequest):
    """A ``CreateCatalogRequest`` sent as given."""


class _VerbatimUpdateCatalogRequest(_Verbatim, UpdateCatalogRequest):
    """An ``UpdateCatalogRequest`` sent as given."""


def _policy(response: LoadPolicyResponse) -> Policy:
    if response.policy is None:
        raise PolarisApiError(200, "Polaris returned a policy response without a policy")
    return response.policy


def _error_details(body: str | bytes | None) -> tuple[str | None, str | None]:
    """Extract ``(message, type)`` from a Polaris/Iceberg error body or an OAuth error body."""
    if not body:
        return None, None
    try:
        data = json.loads(body)
    except (TypeError, ValueError):
        text = body.decode() if isinstance(body, bytes) else body
        return text[:500], None
    if not isinstance(data, dict):
        return None, None
    error = data.get("error")
    if isinstance(error, dict):  # Iceberg ErrorModel: {"error": {"message", "type", "code"}}
        return error.get("message"), error.get("type")
    if isinstance(error, str):  # OAuth error: {"error": "invalid_scope", "error_description": ...}
        return data.get("error_description") or error, error
    return data.get("message"), data.get("type")


@dataclass(frozen=True)
class PolarisConnectionConfig:
    """
    A validated, normalized view of a ``polaris`` connection.

    The client secret and token are excluded from ``repr`` so the config is safe to log.

    Built by :meth:`from_connection`, which performs every check that needs no network call, so a
    misconfigured connection fails before anything is sent.
    """

    auth_type: AuthType
    client_id: str | None
    client_secret: str | None = field(repr=False)
    token: str | None = field(repr=False)
    realm: str | None
    realm_header: str
    scope: str | None
    token_url: str | None
    audience: str | None
    resource: str | None
    client_auth_method: ClientAuthMethod
    default_catalog: str | None
    verify: bool | str
    catalog_url: str
    management_url: str
    timeout: float

    @staticmethod
    def _base_url(conn: Connection) -> str:
        host = (conn.host or "").strip()
        if not host:
            raise PolarisConnectionError(f"Connection {conn.conn_id!r}: Host (the Polaris URL) is empty.")
        if "://" not in host:
            host = f"https://{host}"
        parts = urlsplit(host)
        netloc = parts.netloc
        if conn.port and parts.port is None:
            netloc = f"{netloc}:{conn.port}"
        path = parts.path.rstrip("/")
        if conn.schema:  # the path of a URI-form connection, e.g. polaris://id:secret@host/prefix
            path += "/" + conn.schema.strip("/")
        return urlunsplit((parts.scheme, netloc, path, "", ""))

    @classmethod
    def from_connection(cls, conn: Connection) -> PolarisConnectionConfig:
        extra = conn.extra_dejson or {}

        def opt(key: str) -> str | None:
            value = extra.get(key)
            return str(value).strip() or None if value is not None else None

        conn_id = conn.conn_id
        auth_type = (opt("auth_type") or "internal").lower()
        if auth_type not in get_args(AuthType):
            raise PolarisConnectionError(
                f"Connection {conn_id!r}: unknown auth_type {auth_type!r}; "
                f"expected one of {get_args(AuthType)}."
            )
        client_auth_method = (opt("client_auth_method") or "client_secret_basic").lower()
        if client_auth_method not in get_args(ClientAuthMethod):
            raise PolarisConnectionError(
                f"Connection {conn_id!r}: unknown client_auth_method {client_auth_method!r}; "
                f"expected one of {get_args(ClientAuthMethod)}."
            )

        base_url = cls._base_url(conn)
        catalog_url = base_url + "/" + (opt("catalog_path") or DEFAULT_CATALOG_PATH).strip("/")
        management_url = base_url + "/" + (opt("management_path") or DEFAULT_MANAGEMENT_PATH).strip("/")
        login, password = conn.login or None, conn.password or None
        scope, token_url = opt("scope"), opt("oauth2_server_uri")

        if auth_type == "token":
            if not password:
                raise PolarisConnectionError(
                    f"Connection {conn_id!r}: auth_type 'token' needs the bearer token in Password."
                )
            client_id = client_secret = None
            token, scope, token_url = password, None, None
        else:
            if not (login and password):
                missing = "Client ID (Login)" if not login else "Client Secret (Password)"
                raise PolarisConnectionError(
                    f"Connection {conn_id!r}: auth_type {auth_type!r} needs both Client ID (Login) and "
                    f"Client Secret (Password); {missing} is empty."
                )
            client_id, client_secret, token = login, password, None
            if auth_type == "internal":
                scope = scope or DEFAULT_SCOPE
                bad = [
                    s
                    for s in scope.split()
                    if not s.startswith(PRINCIPAL_ROLE_PREFIX) or s == "PRINCIPAL_ROLE:"
                ]
                if bad:
                    raise PolarisConnectionError(
                        f"Connection {conn_id!r}: scope entries must look like 'PRINCIPAL_ROLE:<name>' "
                        f"(or PRINCIPAL_ROLE:ALL), separated by spaces; got {bad}."
                    )
                token_url = token_url or catalog_url + INTERNAL_TOKEN_PATH
            elif not token_url:
                raise PolarisConnectionError(
                    f"Connection {conn_id!r}: auth_type 'oidc' needs oauth2_server_uri, the identity "
                    "provider's token endpoint."
                )

        timeout_value = extra.get("timeout")
        try:
            timeout = float(timeout_value) if timeout_value not in (None, "") else DEFAULT_TIMEOUT
        except (TypeError, ValueError):
            raise PolarisConnectionError(
                f"Connection {conn_id!r}: timeout must be a number of seconds."
            ) from None

        return cls(
            auth_type=cast("AuthType", auth_type),
            client_id=client_id,
            client_secret=client_secret,
            token=token,
            realm=opt("realm"),
            realm_header=opt("realm_header") or DEFAULT_REALM_HEADER,
            scope=scope,
            token_url=token_url,
            audience=opt("audience") if auth_type == "oidc" else None,
            resource=opt("resource") if auth_type == "oidc" else None,
            client_auth_method=cast("ClientAuthMethod", client_auth_method),
            default_catalog=opt("default_catalog"),
            verify=_as_bool_or_path(extra.get("verify")),
            catalog_url=catalog_url,
            management_url=management_url,
            timeout=timeout,
        )


class PolarisHook(BaseHook):
    """
    Interact with `Apache Polaris <https://polaris.apache.org/>`__.

    Owns authentication for the ``polaris`` connection: it gets a bearer token (from the Polaris
    token endpoint, an external OIDC identity provider, or the connection itself), caches it per
    hook instance, refreshes it shortly before it expires, and retries a call once with a fresh
    token when Polaris answers 401. Every request carries the realm header when a realm is set.

    API calls go through the clients generated in the official ``apache-polaris`` SDK.
    :class:`PolarisManagementHook` and :class:`PolarisCatalogHook` add typed helpers on top.

    .. seealso::
        :ref:`howto/connection:polaris` for the connection fields and the three auth types.

    :param polaris_conn_id: :ref:`Polaris connection id <howto/connection:polaris>`.
    """

    conn_name_attr = "polaris_conn_id"
    default_conn_name = "polaris_default"
    conn_type = "polaris"
    hook_name = "Apache Polaris"

    def __init__(self, polaris_conn_id: str = default_conn_name, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.polaris_conn_id = polaris_conn_id
        self._token: str | None = None
        self._token_expires_at = 0.0  # time.monotonic() deadline, refresh margin already applied
        self._token_lock = threading.Lock()

    # ---- Connection ---------------------------------------------------------------------------

    @cached_property
    def config(self) -> PolarisConnectionConfig:
        """The validated connection; raises :class:`PolarisConnectionError` if it is misconfigured."""
        conn = self.get_connection(self.polaris_conn_id)
        config = PolarisConnectionConfig.from_connection(conn)
        if config.token:
            # A static token (auth_type=token) is the connection's Password, which Airflow masks
            # in tasks; mask it here too, for hooks used elsewhere and for values derived from it.
            mask_secret(config.token)
        if config.verify is False:
            self.log.warning(
                "Connection %r disables TLS certificate verification (verify=false).", self.polaris_conn_id
            )
        return config

    def resolve_catalog(self, catalog: str | None) -> str:
        """Return ``catalog``, or the connection's ``default_catalog`` when it is not given."""
        resolved = catalog or self.config.default_catalog
        if not resolved:
            raise PolarisConnectionError(
                "No catalog given, and connection "
                f"{self.polaris_conn_id!r} has no default_catalog in its extras."
            )
        return resolved

    # ---- Tokens -------------------------------------------------------------------------------

    def get_token(self, force_refresh: bool = False) -> str:
        """
        Return a bearer token for Polaris, fetching or refreshing it when needed.

        For ``auth_type=token`` this is the token stored on the connection, which is never
        refreshed. Otherwise the token is cached on this hook instance until shortly before it
        expires. Tokens are masked in task logs and never written anywhere else.

        :param force_refresh: Fetch a new token even if the cached one has not expired.
        """
        config = self.config
        if config.auth_type == "token":
            return cast("str", config.token)  # validated by PolarisConnectionConfig
        with self._token_lock:
            if force_refresh or self._token is None or time.monotonic() >= self._token_expires_at:
                self._token, self._token_expires_at = self._fetch_token()
            return self._token

    def _fetch_token(self) -> tuple[str, float]:
        """Run the OAuth2 client-credentials grant; return ``(token, monotonic refresh deadline)``."""
        config = self.config
        # PolarisConnectionConfig guarantees these for auth types other than "token".
        token_url, client_id, client_secret = cast(
            "tuple[str, str, str]", (config.token_url, config.client_id, config.client_secret)
        )
        data: dict[str, str] = {"grant_type": "client_credentials"}
        for key, value in (
            ("scope", config.scope),
            ("audience", config.audience),
            ("resource", config.resource),
        ):
            if value:
                data[key] = value
        auth: tuple[str, str] | None = None
        if config.client_auth_method == "client_secret_basic":
            auth = (client_id, client_secret)
        else:
            data["client_id"], data["client_secret"] = client_id, client_secret
        headers = {"Accept": "application/json"}
        if config.auth_type == "internal" and config.realm:
            # Polaris tokens are realm-bound; the identity provider never sees Polaris headers.
            headers[config.realm_header] = config.realm

        try:
            response = requests.post(
                token_url,
                data=data,
                headers=headers,
                auth=auth,
                timeout=config.timeout,
                verify=config.verify,
            )
        except requests.RequestException as e:
            raise PolarisAuthenticationError(
                f"Token request to {config.token_url} failed: {e}",
                retryable=not isinstance(e, _INVALID_URL_ERRORS),
            ) from e

        if response.status_code != 200:
            raise PolarisAuthenticationError(
                self._token_error_message(response), retryable=is_retryable_status(response.status_code)
            )
        try:
            payload = response.json()
            token = payload["access_token"]
        except (ValueError, KeyError, TypeError):
            raise PolarisAuthenticationError(
                f"Token endpoint {config.token_url} returned HTTP 200 without an access_token."
            ) from None
        mask_secret(token)

        try:
            lifetime = float(payload["expires_in"])
        except (KeyError, TypeError, ValueError):  # missing or not a number
            exp = _jwt_expiry(token)
            lifetime = exp - time.time() if exp is not None else FALLBACK_TOKEN_TTL
        margin = min(TOKEN_REFRESH_MARGIN, lifetime / 2)
        return token, time.monotonic() + max(lifetime - margin, 0)

    def _token_error_message(self, response: requests.Response) -> str:
        config = self.config
        message, error_type = _error_details(response.text)
        status = response.status_code
        prefix = f"Token request to {config.token_url} failed with HTTP {status}"
        detail = f" ({error_type}: {message})" if error_type and message != error_type else f" ({message})"
        prefix += detail if message else ""
        if error_type == "invalid_scope":
            hint = (
                "Check the scope extra: space-separated PRINCIPAL_ROLE:<name> entries "
                "(or PRINCIPAL_ROLE:ALL)."
            )
        elif error_type in ("invalid_client", "unauthorized_client"):
            hint = (
                "Check Client ID (Login) and Client Secret (Password); a reset invalidates the old secret "
                "immediately. For an identity provider, also check client_auth_method."
            )
        elif status == 501:
            hint = (
                "This Polaris realm uses external authentication, so its token endpoint is disabled. "
                "Use auth_type 'oidc' with your identity provider's token endpoint."
            )
        elif status == 404 and error_type == REALM_ERROR_TYPE:
            hint = self._realm_hint()
        else:
            hint = ""
        return f"{prefix}. {hint}".strip()

    def _realm_hint(self) -> str:
        realm = self.config.realm
        sent = f"realm {realm!r} in header {self.config.realm_header!r}" if realm else "no realm header"
        return (
            f"Polaris rejected the realm: the connection sent {sent}. Set the realm extra to a realm the "
            "server knows, and realm_header if the server uses a non-default header name."
        )

    # ---- SDK clients --------------------------------------------------------------------------

    def _api_client(self, client_cls: type[_C], configuration_cls: type[Any], host: str) -> _C:
        """Build an SDK API client with this connection's TLS settings and realm header."""
        config = self.config
        configuration = configuration_cls(host=host)
        configuration.verify_ssl = config.verify is not False
        if isinstance(config.verify, str):
            configuration.ssl_ca_cert = config.verify
        realm = {"header_name": config.realm_header, "header_value": config.realm} if config.realm else {}
        return client_cls(configuration, **realm)

    @cached_property
    def _management_client(self) -> ManagementApiClient:
        return self._api_client(ManagementApiClient, ManagementConfiguration, self.config.management_url)

    @cached_property
    def _catalog_client(self) -> CatalogApiClient:
        return self._api_client(CatalogApiClient, CatalogConfiguration, self.config.catalog_url)

    @cached_property
    def management_api(self) -> PolarisDefaultApi:
        """
        The SDK's Management API client. Call it through :meth:`call` to get token handling.

        Typed helpers for common operations live on :class:`PolarisManagementHook`.
        """
        return PolarisDefaultApi(self._management_client)

    @cached_property
    def policy_api(self) -> PolicyAPI:
        """The SDK's Polaris policy API client. Call it through :meth:`call`."""
        return PolicyAPI(self._catalog_client)

    @cached_property
    def iceberg_catalog_api(self) -> IcebergCatalogAPI:
        """The SDK's Iceberg REST catalog client (namespaces, tables). Call it through :meth:`call`."""
        return IcebergCatalogAPI(self._catalog_client)

    @cached_property
    def iceberg_configuration_api(self) -> IcebergConfigurationAPI:
        """The SDK's Iceberg REST config endpoint client. Call it through :meth:`call`."""
        return IcebergConfigurationAPI(self._catalog_client)

    @cached_property
    def polaris_catalog_api(self) -> CatalogAPI:
        """The SDK's Polaris catalog client (adds notifications). Call it through :meth:`call`."""
        return CatalogAPI(self._catalog_client)

    def get_conn(self) -> PolarisDefaultApi:
        """Return the Management API client (same object as :attr:`management_api`)."""
        return self.management_api

    def call(self, method: Callable[..., _T], *args: Any, **kwargs: Any) -> _T:
        """
        Call an SDK API method with a valid token, the connection timeout and error translation.

        Example: ``hook.call(hook.management_api.get_catalog, "analytics")``.

        On HTTP 401 the token is refreshed and the call retried once (except for
        ``auth_type=token``, which has nothing to refresh). SDK and network errors are translated
        into :class:`~airflow.providers.apache.polaris.exceptions.PolarisError` subclasses.
        """
        kwargs.setdefault("_request_timeout", self.config.timeout)
        refreshed = False
        while True:
            token = self.get_token(force_refresh=refreshed)
            self._management_client.configuration.access_token = token
            self._catalog_client.configuration.access_token = token
            try:
                return method(*args, **kwargs)
            except _SDK_API_EXCEPTIONS as e:
                if e.status == 401 and not refreshed and self.config.auth_type != "token":
                    self.log.info("Polaris returned 401; refreshing the token and retrying once.")
                    refreshed = True
                    continue
                raise self._translate(e) from e
            except urllib3.exceptions.HTTPError as e:  # connection refused, timeout, broken connection
                raise PolarisRequestError(f"Request to Polaris failed: {e}") from e

    def _translate(self, e: CatalogApiException | ManagementApiException) -> Exception:
        status = e.status or 0
        message, error_type = _error_details(e.body)
        message = message or e.reason or "no error message"
        if status == 401:
            if self.config.auth_type == "token":
                hint = (
                    "The bearer token in Password is invalid or has expired; "
                    "auth_type 'token' cannot refresh it."
                )
            else:
                hint = (
                    "A fresh token was rejected. Check that the principal exists in this realm and holds "
                    "every "
                    f"role in scope ({self.config.scope or 'from the identity provider'})."
                )
            return PolarisAuthenticationError(f"Polaris returned HTTP 401: {message}. {hint}")
        if status == 404 and error_type == REALM_ERROR_TYPE:
            return PolarisAuthenticationError(f"Polaris returned HTTP 404: {message}. {self._realm_hint()}")
        if status == 403 and "rotate" in message.lower():
            return PolarisAuthenticationError(
                f"Polaris returned HTTP 403: {message}. This principal was created with "
                "credentialRotationRequired: rotate its credentials, then update the connection's Password."
            )
        error_cls = {403: PolarisForbiddenError, 404: PolarisNotFoundError, 409: PolarisConflictError}.get(
            status, PolarisApiError
        )
        return error_cls(status, message, error_type)

    # ---- Test connection ----------------------------------------------------------------------

    def test_connection(self) -> tuple[bool, str]:
        """
        Check configuration, authentication and role activation with as few calls as possible.

        A token alone proves little: Polaris checks the requested principal roles only when the
        token is used. So after getting a token this makes one API call. With ``default_catalog``
        it reads that catalog's config; otherwise it lists catalogs, where an HTTP 403 still
        counts as success because it means authentication worked.
        """
        try:
            config = self.config
            self.get_token()
            if config.default_catalog:
                self.call(self.iceberg_configuration_api.get_config, warehouse=config.default_catalog)
                return True, f"Authenticated and read the config of catalog {config.default_catalog!r}."
            try:
                self.call(self.management_api.list_catalogs)
            except PolarisForbiddenError:
                return True, (
                    "Authenticated. The principal lacks the privilege to list catalogs (HTTP 403), "
                    "which is fine for catalog-scoped connections; set default_catalog to test catalog "
                    "access."
                )
            return True, "Authenticated and listed catalogs."
        except Exception as e:  # report any failure in the UI instead of raising
            return False, str(e)

    # ---- Engine configuration -----------------------------------------------------------------

    def get_iceberg_rest_properties(
        self, catalog: str | None = None, engine: Literal["spark", "trino"] = "spark"
    ) -> dict[str, str]:
        """
        Iceberg REST catalog properties that point an engine at Polaris with this connection's auth.

        ``engine="spark"`` returns Iceberg Java client properties (``uri``, ``warehouse``,
        ``header.<realm header>``, ...). Prefix each key with ``spark.sql.catalog.<name>.`` and add
        ``type=rest`` / the catalog implementation as your Spark setup requires.

        ``engine="trino"`` returns the same settings under Trino's ``iceberg.rest-catalog.*``
        names, for whoever manages Trino's catalog configuration (Trino does not take them per
        query).

        Authentication: the engine gets a short-lived **token**, never the client secret. With
        ``auth_type=internal`` the Spark properties include the token endpoint and scope, so the
        Iceberg Java client keeps the token fresh through Polaris token exchange and long jobs keep
        working. Otherwise the token cannot be refreshed, and the properties turn the engine's
        token refresh off, so jobs must finish before it expires (one hour by default for Polaris
        tokens): with ``auth_type=oidc`` or ``token`` Polaris cannot exchange it, and Trino accepts
        no scope next to a token. For a long-lived Trino catalog, configure
        ``iceberg.rest-catalog.oauth2.credential`` instead.

        :param catalog: Polaris catalog (the warehouse). Defaults to the connection's
            ``default_catalog``.
        :param engine: ``"spark"`` or ``"trino"``.
        """
        if engine not in ("spark", "trino"):
            raise PolarisInvalidArgumentError(f"engine must be 'spark' or 'trino', got {engine!r}")
        config = self.config
        props = {
            "uri": config.catalog_url,
            "warehouse": self.resolve_catalog(catalog),
            "token": self.get_token(),
        }
        if engine == "spark":
            # What Iceberg Java infers from a token anyway; naming it avoids the inference warning.
            props["rest.auth.type"] = "oauth2"
            if config.auth_type == "internal":
                # Iceberg Java refreshes the token by token exchange, asking for this scope; its
                # default ("catalog") is rejected by Polaris.
                props["oauth2-server-uri"] = config.token_url or ""
                props["scope"] = config.scope or DEFAULT_SCOPE
            else:
                # Iceberg Java would refresh the token by token exchange asking for scope "catalog",
                # which Polaris rejects (and Polaris cannot exchange an identity provider's token),
                # so the attempts could only fail. Naming an endpoint stops Iceberg warning that it
                # falls back to Polaris's; with refresh off, nothing is sent to it.
                props["oauth2-server-uri"] = config.token_url or config.catalog_url + INTERNAL_TOKEN_PATH
                props["token-refresh-enabled"] = "false"
            if config.realm:
                props[f"header.{config.realm_header}"] = config.realm
            return props
        trino = {f"iceberg.rest-catalog.{_TRINO_KEYS[key]}": value for key, value in props.items()}
        trino["iceberg.rest-catalog.security"] = "OAUTH2"
        # Trino would refresh the token by the same token exchange, without a Polaris scope.
        trino["iceberg.rest-catalog.oauth2.token-refresh-enabled"] = "false"
        if config.realm:
            trino["iceberg.rest-catalog.http-headers"] = f"{config.realm_header}: {config.realm}"
        return trino

    # ---- Helpers ------------------------------------------------------------------------------

    def _paginate(self, method: Callable[..., Any], *args: Any, items: str, **kwargs: Any) -> list[Any]:
        """Call a paginated SDK list method until ``next-page-token`` runs out; return every item."""
        result: list[Any] = []
        page_token: str | None = None
        while True:
            response = self.call(method, *args, page_token=page_token, **kwargs)
            result.extend(getattr(response, items) or [])
            page_token = response.next_page_token
            if not page_token:
                return result

    def _create_or_get(
        self,
        if_exists: IfExists | IfPolicyExists,
        what: str,
        create: Callable[[], _T],
        get: Callable[[], _T],
    ) -> _T:
        """
        Run ``create``; if the entity already exists and ``if_exists`` is not ``"fail"``, return ``get()``.

        Only a 409 ``AlreadyExistsException`` counts as existing; other 409s are raised. ``get``
        reads the existing entity (and may update it). When it finds nothing (HTTP 404), another
        client may have dropped the entity since the create, so ``create`` runs again, up to
        ``CREATE_ATTEMPTS`` times. If every attempt finds the name taken but nothing to read, the
        name belongs to another kind of entity (a table's name taken by a view, for example), and
        the conflict is raised.
        """
        for _ in range(CREATE_ATTEMPTS):
            try:
                return create()
            except PolarisConflictError as e:
                if if_exists == "fail" or not e.already_exists:
                    raise
                conflict = e
            try:
                existing = get()
            except PolarisNotFoundError:
                self.log.info("%s was reported as existing but could not be read.", what)
                continue
            if if_exists == "skip":
                self.log.info("%s already exists; skipping.", what)
            return existing
        raise conflict

    def _skip_missing(
        self, if_missing: IfMissing, method: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> bool:
        """
        Call a delete/revoke/detach method; ``False`` if the entity was missing and skipped.

        Missing means HTTP 404, or HTTP 400 ``GRANT_NOT_FOUND``, which Polaris returns when the
        role assignment or grant to revoke does not exist.
        """
        try:
            self.call(method, *args, **kwargs)
        except PolarisApiError as e:
            missing = isinstance(e, PolarisNotFoundError) or (
                e.status == 400 and e.error_type == GRANT_NOT_FOUND_TYPE
            )
            if missing and if_missing == "skip":
                self.log.info("Nothing to %s: not found.", method.__name__.replace("_", " "))
                return False
            raise
        return True

    def _retry_stale_version(self, what: str, update: Callable[[], _T]) -> _T:
        """
        Call ``update`` up to ``UPDATE_ATTEMPTS`` times while Polaris rejects it as stale.

        ``update`` must read the entity's latest version on every call, as Polaris asks. Each
        retry waits a short random time first (:func:`_pause_before_retry`).
        """
        for _ in range(UPDATE_ATTEMPTS - 1):
            try:
                return update()
            except PolarisConflictError as e:
                if not e.stale_version:
                    raise
                self.log.info(
                    "%s changed while it was being updated; retrying with its latest version.", what
                )
                _pause_before_retry()
        return update()


class PolarisManagementHook(PolarisHook):
    """
    Typed helpers for the Polaris Management API (``/api/management/v1``).

    Create methods take ``if_exists`` and delete/revoke methods take ``if_missing``
    (``"fail"`` or ``"skip"``), so callers can make steps idempotent and safe to retry.
    Entities may be passed as SDK models or as plain dicts in Polaris's JSON shape.

    :param polaris_conn_id: :ref:`Polaris connection id <howto/connection:polaris>`.
    """

    # ---- Catalogs -----------------------------------------------------------------------------

    def list_catalogs(self) -> list[Catalog]:
        """Return all catalogs visible to the principal."""
        return self.call(self.management_api.list_catalogs).catalogs

    def get_catalog(self, catalog_name: str) -> Catalog:
        """Return one catalog."""
        return self.call(self.management_api.get_catalog, catalog_name)

    def create_catalog(self, catalog: Catalog | dict[str, Any], if_exists: IfExists = "fail") -> Catalog:
        """
        Create a catalog.

        :param catalog: The catalog, e.g. ``{"type": "INTERNAL", "name": "analytics", "properties":
            {"default-base-location": "s3://bucket/analytics"}, "storageConfigInfo": {"storageType":
            "S3", "roleArn": "...", "allowedLocations": ["s3://bucket/"]}}``.
            A dict is sent as given, so every field Polaris knows reaches it (for example the
            ``connectionConfigInfo`` of a federated catalog, which the SDK's models would drop).
        :param if_exists: ``"skip"`` returns the existing catalog instead of failing. The existing
            catalog is not compared with or updated to ``catalog``.
        """
        body = catalog.to_dict() if isinstance(catalog, Catalog) else _json_object("catalog", catalog)
        name = body.get("name")
        if not isinstance(name, str) or not name:
            raise PolarisInvalidArgumentError("catalog needs a name.")
        request = _VerbatimCreateCatalogRequest.of({"catalog": body})
        return self._create_or_get(
            if_exists,
            f"Catalog {name!r}",
            lambda: self.call(self.management_api.create_catalog, request),
            lambda: self.get_catalog(name),
        )

    def update_catalog(
        self,
        catalog_name: str,
        properties: dict[str, str] | None = None,
        storage_config_info: StorageConfigInfo | dict[str, Any] | None = None,
        replace_properties: bool = False,
    ) -> Catalog:
        """
        Update a catalog's properties and/or storage configuration.

        Reads the current entity version first, as Polaris requires for optimistic concurrency, and
        starts again from the latest version if the catalog changes in the meantime.

        :param catalog_name: Catalog to update.
        :param properties: Properties to set. Merged into the existing ones unless
            ``replace_properties`` is true.
        :param storage_config_info: New storage configuration, if it should change.
        :param replace_properties: Replace all properties instead of merging.
        :return: The updated catalog.
        """
        new_properties = _optional_string_map(properties)
        storage = None
        if storage_config_info:
            storage = (
                storage_config_info.to_dict()
                if isinstance(storage_config_info, StorageConfigInfo)
                else _json_object("storage_config_info", storage_config_info)
            )

        def update() -> Catalog:
            current = self.get_catalog(catalog_name)
            body: dict[str, Any] = {"currentEntityVersion": current.entity_version}
            if new_properties is not None:
                existing = (
                    {} if replace_properties or not current.properties else current.properties.to_dict()
                )
                body["properties"] = {**existing, **new_properties}
            if storage:
                body["storageConfigInfo"] = storage
            request = _VerbatimUpdateCatalogRequest.of(body)
            return self.call(self.management_api.update_catalog, catalog_name, request)

        return self._retry_stale_version(f"Catalog {catalog_name!r}", update)

    def delete_catalog(self, catalog_name: str, if_missing: IfMissing = "fail") -> bool:
        """
        Delete a catalog.

        Polaris refuses to delete a catalog that still contains namespaces or catalog roles other
        than ``catalog_admin``.

        :return: ``True`` if deleted, ``False`` if it was missing and ``if_missing="skip"``.
        """
        return self._skip_missing(if_missing, self.management_api.delete_catalog, catalog_name)

    # ---- Principals ---------------------------------------------------------------------------

    def list_principals(self) -> list[Principal]:
        """Return all principals."""
        return self.call(self.management_api.list_principals).principals

    def get_principal(self, principal_name: str) -> Principal:
        """Return one principal (never includes credentials)."""
        return self.call(self.management_api.get_principal, principal_name)

    def create_principal(
        self,
        principal_name: str,
        properties: dict[str, str] | None = None,
        credential_rotation_required: bool = False,
        if_exists: IfExists = "fail",
    ) -> Principal:
        """
        Create a principal and return it **without** its credentials.

        Polaris returns the new principal's client secret exactly once, in this response. The
        hook discards it: this provider never hands secrets to task code, XCom or logs. Use this
        for principals that authenticate through an external identity provider, or issue
        credentials for it out of band (for example with the Polaris CLI's ``principals reset``).

        :param principal_name: Name of the principal.
        :param properties: Optional principal properties; values are strings.
        :param credential_rotation_required: Require the principal to rotate its credentials before
            it can use them.
        :param if_exists: ``"skip"`` returns the existing principal instead of failing.
        """
        request = CreatePrincipalRequest(
            principal=Principal(name=principal_name, properties=_optional_string_map(properties)),
            credential_rotation_required=credential_rotation_required,
        )

        def create() -> Principal:
            created: PrincipalWithCredentials = self.call(self.management_api.create_principal, request)
            if created.credentials is not None and created.credentials.client_secret is not None:
                mask_secret(created.credentials.client_secret.get_secret_value())
            return created.principal

        return self._create_or_get(
            if_exists, f"Principal {principal_name!r}", create, lambda: self.get_principal(principal_name)
        )

    def delete_principal(self, principal_name: str, if_missing: IfMissing = "fail") -> bool:
        """Delete a principal. Returns ``False`` if it was missing and ``if_missing="skip"``."""
        return self._skip_missing(if_missing, self.management_api.delete_principal, principal_name)

    # ---- Principal roles ----------------------------------------------------------------------

    def list_principal_roles(self) -> list[PrincipalRole]:
        """Return all principal roles."""
        return self.call(self.management_api.list_principal_roles).roles

    def create_principal_role(
        self, role_name: str, properties: dict[str, str] | None = None, if_exists: IfExists = "fail"
    ) -> PrincipalRole:
        """Create a principal role. ``if_exists="skip"`` returns the existing one."""
        request = CreatePrincipalRoleRequest(
            principal_role=PrincipalRole(name=role_name, properties=_optional_string_map(properties))
        )
        return self._create_or_get(
            if_exists,
            f"Principal role {role_name!r}",
            lambda: self.call(self.management_api.create_principal_role, request),
            lambda: self.call(self.management_api.get_principal_role, role_name),
        )

    def delete_principal_role(self, role_name: str, if_missing: IfMissing = "fail") -> bool:
        """Delete a principal role. Returns ``False`` if it was missing and ``if_missing="skip"``."""
        return self._skip_missing(if_missing, self.management_api.delete_principal_role, role_name)

    def assign_principal_role(self, principal_name: str, role_name: str) -> None:
        """Assign a principal role to a principal. Assigning an already-assigned role is a no-op."""
        request = GrantPrincipalRoleRequest(principal_role=PrincipalRole(name=role_name))
        self.call(self.management_api.assign_principal_role, principal_name, request)

    def revoke_principal_role(
        self, principal_name: str, role_name: str, if_missing: IfMissing = "fail"
    ) -> bool:
        """
        Revoke a principal role from a principal.

        :return: ``True`` if revoked, ``False`` if it was not assigned and ``if_missing="skip"``.
        """
        return self._skip_missing(
            if_missing, self.management_api.revoke_principal_role, principal_name, role_name
        )

    # ---- Catalog roles ------------------------------------------------------------------------

    def list_catalog_roles(self, catalog_name: str) -> list[CatalogRole]:
        """Return the catalog roles of a catalog."""
        return self.call(self.management_api.list_catalog_roles, catalog_name).roles

    def create_catalog_role(
        self,
        catalog_name: str,
        role_name: str,
        properties: dict[str, str] | None = None,
        if_exists: IfExists = "fail",
    ) -> CatalogRole:
        """Create a catalog role in a catalog. ``if_exists="skip"`` returns the existing one."""
        request = CreateCatalogRoleRequest(
            catalog_role=CatalogRole(name=role_name, properties=_optional_string_map(properties))
        )
        return self._create_or_get(
            if_exists,
            f"Catalog role {role_name!r} in catalog {catalog_name!r}",
            lambda: self.call(self.management_api.create_catalog_role, catalog_name, request),
            lambda: self.call(self.management_api.get_catalog_role, catalog_name, role_name),
        )

    def delete_catalog_role(self, catalog_name: str, role_name: str, if_missing: IfMissing = "fail") -> bool:
        """Delete a catalog role. Returns ``False`` if it was missing and ``if_missing="skip"``."""
        return self._skip_missing(
            if_missing, self.management_api.delete_catalog_role, catalog_name, role_name
        )

    def assign_catalog_role(
        self, principal_role_name: str, catalog_name: str, catalog_role_name: str
    ) -> None:
        """Assign a catalog role to a principal role. Assigning an already-assigned role is a no-op."""
        request = GrantCatalogRoleRequest(catalog_role=CatalogRole(name=catalog_role_name))
        self.call(
            self.management_api.assign_catalog_role_to_principal_role,
            principal_role_name,
            catalog_name,
            request,
        )

    def revoke_catalog_role(
        self,
        principal_role_name: str,
        catalog_name: str,
        catalog_role_name: str,
        if_missing: IfMissing = "fail",
    ) -> bool:
        """
        Revoke a catalog role from a principal role.

        :return: ``True`` if revoked, ``False`` if it was not assigned and ``if_missing="skip"``.
        """
        return self._skip_missing(
            if_missing,
            self.management_api.revoke_catalog_role_from_principal_role,
            principal_role_name,
            catalog_name,
            catalog_role_name,
        )

    # ---- Grants -------------------------------------------------------------------------------

    def list_grants(self, catalog_name: str, catalog_role_name: str) -> list[GrantResource]:
        """Return the privileges granted to a catalog role."""
        return self.call(
            self.management_api.list_grants_for_catalog_role, catalog_name, catalog_role_name
        ).grants

    def add_grant(
        self, catalog_name: str, catalog_role_name: str, grant: GrantResource | dict[str, Any]
    ) -> None:
        """
        Grant a privilege to a catalog role. Granting an existing privilege is a no-op in Polaris.

        :param grant: e.g. ``{"type": "catalog", "privilege": "TABLE_READ_DATA"}`` or
            ``{"type": "namespace", "namespace": ["sales"], "privilege": "TABLE_WRITE_DATA"}``.
            Types: ``catalog``, ``namespace``, ``table`` (adds ``tableName``), ``view``
            (``viewName``), ``policy`` (``policyName``).
        """
        request = AddGrantRequest(grant=_model(GrantResource, grant))
        self.call(self.management_api.add_grant_to_catalog_role, catalog_name, catalog_role_name, request)

    def revoke_grant(
        self,
        catalog_name: str,
        catalog_role_name: str,
        grant: GrantResource | dict[str, Any],
        cascade: bool = False,
        if_missing: IfMissing = "fail",
    ) -> bool:
        """
        Revoke a privilege from a catalog role; ``cascade`` also revokes it on sub-resources.

        :return: ``True`` if revoked, ``False`` if it was not granted and ``if_missing="skip"``.
        """
        request = RevokeGrantRequest(grant=_model(GrantResource, grant))
        return self._skip_missing(
            if_missing,
            self.management_api.revoke_grant_from_catalog_role,
            catalog_name,
            catalog_role_name,
            cascade=cascade,
            revoke_grant_request=request,
        )


class PolarisCatalogHook(PolarisHook):
    """
    Manage what a Polaris catalog defines: namespaces, tables, policies and notifications.

    Table methods manage table definitions (create, drop, register, rename, properties). Reading
    and writing table data is left to pyiceberg and the engines: :meth:`get_iceberg_catalog`
    returns a pyiceberg catalog configured with this connection's auth, realm and TLS settings.

    Namespaces are given as ``"a.b"`` or ``["a", "b"]`` (use the list form for names with dots).
    Table methods return the Iceberg REST JSON (dicts) rather than SDK models; namespace, policy
    and listing methods return SDK models.

    :param polaris_conn_id: :ref:`Polaris connection id <howto/connection:polaris>`.
    """

    def _scope(self, namespace: str | Sequence[str], catalog: str | None) -> tuple[str, str]:
        """Return the ``(prefix, namespace)`` path parameters for a namespace in a catalog."""
        return self.resolve_catalog(catalog), encode_namespace(namespace)

    def _call_json(self, method: Callable[..., Any], *args: Any, **kwargs: Any) -> dict[str, Any]:
        """
        Like :meth:`call`, for an SDK ``*_without_preload_content`` method; return the JSON body.

        Table responses are not parsed into the SDK's models: those reject valid tables (Iceberg v3
        field defaults, for example), and the Iceberg REST JSON is what callers need anyway.
        """

        def request(**request_kwargs: Any) -> dict[str, Any]:
            response = method(*args, **request_kwargs)
            body = response.data.decode("utf-8")  # reading the whole body returns the connection
            if not 200 <= response.status <= 299:
                raise CatalogApiException(status=response.status, reason=response.reason, body=body)
            return json.loads(body) if body else {}

        return self.call(request, **kwargs)

    # ---- Namespaces and tables ----------------------------------------------------------------

    def list_namespaces(
        self, catalog: str | None = None, parent: str | Sequence[str] | None = None
    ) -> list[list[str]]:
        """Return the namespaces directly under ``parent`` (or the top level)."""
        return self._paginate(
            self.iceberg_catalog_api.list_namespaces,
            self.resolve_catalog(catalog),
            items="namespaces",
            parent=encode_namespace(parent) if parent else None,
        )

    def list_tables(
        self, namespace: str | Sequence[str], catalog: str | None = None
    ) -> list[TableIdentifier]:
        """Return the tables in a namespace (follows pagination)."""
        return self._paginate(
            self.iceberg_catalog_api.list_tables, *self._scope(namespace, catalog), items="identifiers"
        )

    def _exists(self, head: Callable[[], Any], get: Callable[[], Any]) -> bool:
        """
        Check existence with a HEAD request, confirming a 404 with ``get``.

        A HEAD response has no body, so a rejected realm (also HTTP 404) would look like a missing
        entity; the GET raises :class:`PolarisAuthenticationError` for it instead.
        """
        try:
            head()
        except PolarisNotFoundError:
            try:
                get()
            except PolarisNotFoundError:
                return False
        return True

    def namespace_exists(self, namespace: str | Sequence[str], catalog: str | None = None) -> bool:
        """Return whether a namespace exists."""
        return self._exists(
            lambda: self.call(self.iceberg_catalog_api.namespace_exists, *self._scope(namespace, catalog)),
            lambda: self.get_namespace_properties(namespace, catalog),
        )

    def get_namespace_properties(
        self, namespace: str | Sequence[str], catalog: str | None = None
    ) -> dict[str, str]:
        """Return a namespace's properties (Polaris adds ``location``)."""
        response = self.call(
            self.iceberg_catalog_api.load_namespace_metadata, *self._scope(namespace, catalog)
        )
        return dict(response.properties or {})

    def create_namespace(
        self,
        namespace: str | Sequence[str],
        properties: dict[str, str] | None = None,
        catalog: str | None = None,
        if_exists: IfExists = "fail",
    ) -> dict[str, str]:
        """
        Create a namespace and return its properties.

        :param namespace: ``"a.b"`` or ``["a", "b"]``; the parent must exist.
        :param if_exists: ``"skip"`` returns the existing namespace's properties instead of
            failing. They are not compared with or updated to ``properties``.
        """
        parts, prefix = namespace_parts(namespace), self.resolve_catalog(catalog)
        if properties is not None:
            properties = _string_map("properties", properties)
        request = CreateNamespaceRequest(namespace=parts, properties=properties)
        return self._create_or_get(
            if_exists,
            f"Namespace {'.'.join(parts)!r}",
            lambda: dict(
                self.call(self.iceberg_catalog_api.create_namespace, prefix, request).properties or {}
            ),
            lambda: self.get_namespace_properties(parts, catalog),
        )

    def update_namespace_properties(
        self,
        namespace: str | Sequence[str],
        updates: dict[str, str] | None = None,
        removals: list[str] | None = None,
        catalog: str | None = None,
    ) -> dict[str, list[str]]:
        """
        Set and/or remove namespace properties; return ``{"updated", "removed", "missing"}`` keys.

        Removing a key that does not exist is not an error (it is reported as missing). A key
        cannot be both updated and removed.
        """
        updates, removals = _property_changes(updates, removals)
        prefix, encoded = self._scope(namespace, catalog)
        result: dict[str, list[str]] = {"updated": [], "removed": [], "missing": []}
        # Polaris (1.8 and 1.9) answers 409 "Concurrent modification" when one request both sets
        # and removes properties, after applying only the updates; separate requests behave correctly.
        requests_ = [UpdateNamespacePropertiesRequest(updates=updates)] if updates else []
        if removals:
            requests_.append(UpdateNamespacePropertiesRequest(removals=removals))
        for request in requests_:
            response = self.call(self.iceberg_catalog_api.update_properties, prefix, encoded, request)
            for key in result:
                result[key] += getattr(response, key) or []
        return result

    def drop_namespace(
        self, namespace: str | Sequence[str], catalog: str | None = None, if_missing: IfMissing = "fail"
    ) -> bool:
        """
        Drop an empty namespace.

        Polaris refuses to drop a namespace that still contains tables, views, namespaces or
        policies (a non-retryable :class:`PolarisConflictError`).

        :return: ``True`` if dropped, ``False`` if it was missing and ``if_missing="skip"``.
        """
        return self._skip_missing(
            if_missing, self.iceberg_catalog_api.drop_namespace, *self._scope(namespace, catalog)
        )

    def table_exists(self, namespace: str | Sequence[str], table: str, catalog: str | None = None) -> bool:
        """Return whether a table exists."""
        return self._exists(
            lambda: self.call(self.iceberg_catalog_api.table_exists, *self._scope(namespace, catalog), table),
            lambda: self.load_table(namespace, table, catalog),
        )

    def load_table(
        self, namespace: str | Sequence[str], table: str, catalog: str | None = None
    ) -> dict[str, Any]:
        """
        Return a table as the Iceberg REST ``LoadTableResult`` JSON, without vending credentials.

        The result has ``metadata-location`` and ``metadata`` (with ``table-uuid``, ``location``,
        ``properties``, ...). Only snapshots referenced by branches and tags are loaded, to keep
        the response small.
        """
        return self._call_json(
            self.iceberg_catalog_api.load_table_without_preload_content,
            *self._scope(namespace, catalog),
            table,
            snapshots="refs",
        )

    def get_table_location(
        self, namespace: str | Sequence[str], table: str, catalog: str | None = None
    ) -> str | None:
        """Return a table's base location, without vending storage credentials."""
        return self.load_table(namespace, table, catalog).get("metadata", {}).get("location")

    def create_table(
        self,
        namespace: str | Sequence[str],
        table: str,
        schema: dict[str, Any],
        partition_spec: dict[str, Any] | None = None,
        sort_order: dict[str, Any] | None = None,
        location: str | None = None,
        properties: dict[str, str] | None = None,
        catalog: str | None = None,
        if_exists: IfExists = "fail",
    ) -> dict[str, Any]:
        """
        Create an Iceberg table and return it as in :meth:`load_table`.

        The definition uses the Iceberg REST spec's JSON shapes and is sent as given; Polaris
        validates it. Storage credentials are not requested: creating a table needs none.

        :param schema: e.g. ``{"type": "struct", "fields": [{"id": 1, "name": "id", "type": "long",
            "required": False}]}``.
        :param partition_spec: e.g. ``{"fields": [{"source-id": 1, "field-id": 1000, "name":
            "id_bucket", "transform": "bucket[16]"}]}``. Omit for an unpartitioned table.
            ``spec-id`` defaults to 0.
        :param sort_order: The table's write order, e.g. ``{"fields": [{"source-id": 1,
            "transform": "identity", "direction": "asc", "null-order": "nulls-first"}]}``.
            ``order-id`` defaults to 1 (0 for an order without fields, i.e. unsorted).
        :param location: Base location; defaults to one derived from the namespace's.
        :param properties: Table properties, e.g. ``{"format-version": "3"}``; values are strings.
        :param if_exists: ``"skip"`` returns the existing table instead of failing. Its definition
            is not compared with or updated to the one given.
        """
        body: dict[str, Any] = {"name": table, "schema": _json_object("schema", schema)}
        if partition_spec is not None:
            body["partition-spec"] = {"spec-id": 0, **_json_object("partition_spec", partition_spec)}
        if sort_order is not None:
            sort_order = _json_object("sort_order", sort_order)
            body["write-order"] = {"order-id": 1 if sort_order.get("fields") else 0, **sort_order}
        if location is not None:
            body["location"] = location
        if properties is not None:
            body["properties"] = _string_map("properties", properties)
        request = _VerbatimCreateTableRequest.of(body)
        prefix, encoded = self._scope(namespace, catalog)

        def create() -> dict[str, Any]:
            try:
                return self._call_json(
                    self.iceberg_catalog_api.create_table_without_preload_content, prefix, encoded, request
                )
            except PolarisApiError as e:
                # Polaris answers a bare "HTTP 400 Bad Request" when it cannot parse the definition.
                if e.status == 400 and e.error_type == "WebApplicationException":
                    raise PolarisApiError(
                        e.status,
                        f"{e.message}. Polaris could not parse the table definition: check the schema, "
                        "partition spec and sort order against the Iceberg REST specification",
                        e.error_type,
                    ) from e
                raise

        return self._create_or_get(
            if_exists, f"Table {table!r}", create, lambda: self.load_table(namespace, table, catalog)
        )

    def drop_table(
        self,
        namespace: str | Sequence[str],
        table: str,
        purge: bool = False,
        catalog: str | None = None,
        if_missing: IfMissing = "fail",
    ) -> bool:
        """
        Drop a table.

        :param purge: Also delete the table's data and metadata files. Irreversible; Polaris
            refuses it unless the catalog property ``polaris.config.drop-with-purge.enabled`` is
            ``"true"`` or the server enables the ``DROP_WITH_PURGE_ENABLED`` feature.
        :return: ``True`` if dropped, ``False`` if it was missing and ``if_missing="skip"``.
        """
        return self._skip_missing(
            if_missing,
            self.iceberg_catalog_api.drop_table,
            *self._scope(namespace, catalog),
            table,
            purge_requested=purge,
        )

    def update_table_properties(
        self,
        namespace: str | Sequence[str],
        table: str,
        updates: dict[str, str] | None = None,
        removals: list[str] | None = None,
        catalog: str | None = None,
    ) -> dict[str, Any]:
        """
        Set and/or remove table properties, committing only what would change.

        The commit requires the table's UUID to be unchanged, as pyiceberg's commits do, so a
        table dropped and re-created in between is not modified. A key cannot be both updated and
        removed.

        :return: ``{"updated": [...], "removed": [...], "properties": {...}}``: the keys changed by
            this call and the table's properties afterwards. Re-running is a no-op.
        """
        updates, removals = _property_changes(updates, removals)
        metadata = self.load_table(namespace, table, catalog)["metadata"]
        current = dict(metadata.get("properties") or {})
        changed = {key: value for key, value in updates.items() if current.get(key) != value}
        removed = [key for key in removals if key in current]
        if not changed and not removed:
            self.log.info("Table %r properties are already up to date.", table)
            return {"updated": [], "removed": [], "properties": current}
        changes = []
        if changed:
            changes.append(TableUpdate(SetPropertiesUpdate(action="set-properties", updates=changed)))
        if removed:
            changes.append(TableUpdate(RemovePropertiesUpdate(action="remove-properties", removals=removed)))
        request = CommitTableRequest(
            identifier=TableIdentifier(namespace=namespace_parts(namespace), name=table),
            requirements=[AssertTableUUID(type="assert-table-uuid", uuid=metadata["table-uuid"])],
            updates=changes,
        )
        response = self._call_json(
            self.iceberg_catalog_api.update_table_without_preload_content,
            *self._scope(namespace, catalog),
            table,
            request,
        )
        return {
            "updated": sorted(changed),
            "removed": sorted(removed),
            "properties": dict(response["metadata"].get("properties") or {}),
        }

    def register_table(
        self,
        namespace: str | Sequence[str],
        table: str,
        metadata_location: str,
        catalog: str | None = None,
        if_exists: IfExists = "fail",
    ) -> dict[str, Any]:
        """
        Register an existing Iceberg table from its metadata file; return it as in :meth:`load_table`.

        :param metadata_location: The table's current ``metadata.json``, e.g. when moving a table
            from another catalog. It must lie within the catalog's allowed locations.
        :param if_exists: ``"skip"`` returns the existing table instead of failing, without
            comparing its metadata location.
        """
        request = RegisterTableRequest(name=table, metadata_location=metadata_location)
        prefix, encoded = self._scope(namespace, catalog)
        return self._create_or_get(
            if_exists,
            f"Table {table!r}",
            lambda: self._call_json(
                self.iceberg_catalog_api.register_table_without_preload_content, prefix, encoded, request
            ),
            lambda: self.load_table(namespace, table, catalog),
        )

    def rename_table(
        self,
        namespace: str | Sequence[str],
        table: str,
        new_table: str,
        new_namespace: str | Sequence[str] | None = None,
        catalog: str | None = None,
        if_missing: IfMissing = "fail",
    ) -> bool:
        """
        Rename a table, optionally moving it to another namespace of the same catalog.

        Since Polaris 1.9.0 a move is refused (HTTP 403) unless the table's files lie within the
        new namespace's location or the catalog enables ``ALLOW_UNSTRUCTURED_TABLE_LOCATION``.

        :param if_missing: With ``"skip"``, a missing source table whose new name already exists
            counts as already renamed (a re-run). A missing source without that is always an error.
        :return: ``True`` if renamed, ``False`` if it had already been renamed.
        """
        destination_namespace = new_namespace if new_namespace is not None else namespace
        request = RenameTableRequest(
            source=TableIdentifier(namespace=namespace_parts(namespace), name=table),
            destination=TableIdentifier(namespace=namespace_parts(destination_namespace), name=new_table),
        )
        try:
            self.call(self.iceberg_catalog_api.rename_table, self.resolve_catalog(catalog), request)
        except PolarisNotFoundError:
            if if_missing == "skip" and self.table_exists(destination_namespace, new_table, catalog):
                self.log.info("Table %r is already renamed to %r; skipping.", table, new_table)
                return False
            raise
        return True

    # ---- Policies -----------------------------------------------------------------------------

    @staticmethod
    def _content(content: str | dict[str, Any] | None) -> str | None:
        return json.dumps(content) if isinstance(content, dict) else content

    def list_policies(
        self, namespace: str | Sequence[str], catalog: str | None = None, policy_type: str | None = None
    ) -> list[PolicyIdentifier]:
        """Return the identifiers of the policies defined in a namespace (follows pagination)."""
        return self._paginate(
            self.policy_api.list_policies,
            *self._scope(namespace, catalog),
            items="identifiers",
            policy_type=policy_type,
        )

    def load_policy(
        self, namespace: str | Sequence[str], policy_name: str, catalog: str | None = None
    ) -> Policy:
        """Return one policy."""
        return _policy(self.call(self.policy_api.load_policy, *self._scope(namespace, catalog), policy_name))

    def create_policy(
        self,
        namespace: str | Sequence[str],
        policy_name: str,
        policy_type: str,
        content: str | dict[str, Any] | None = None,
        description: str | None = None,
        catalog: str | None = None,
        if_exists: IfPolicyExists = "fail",
    ) -> Policy:
        """
        Create a policy in a namespace.

        :param policy_type: e.g. ``system.snapshot-expiry`` or ``system.data-compaction``.
        :param content: Policy content as a dict or JSON string; Polaris validates it against the
            type's schema.
        :param if_exists: ``"skip"`` returns the existing policy unchanged; ``"update"`` updates
            its content and description when they differ. Either way, an existing policy of a
            different type raises :class:`PolarisInvalidArgumentError`.
        """
        request = CreatePolicyRequest(
            name=policy_name, type=policy_type, description=description, content=self._content(content)
        )
        scope = self._scope(namespace, catalog)

        def get_existing() -> Policy:
            policy = self.load_policy(namespace, policy_name, catalog)
            if policy.policy_type != policy_type:
                raise PolarisInvalidArgumentError(
                    f"Policy {policy_name!r} already exists with type {policy.policy_type!r}, "
                    f"not {policy_type!r}; drop it first or use another name."
                )
            if if_exists == "update":
                return self.update_policy(namespace, policy_name, content, description, catalog)
            return policy

        return self._create_or_get(
            if_exists,
            f"Policy {policy_name!r}",
            lambda: _policy(self.call(self.policy_api.create_policy, *scope, request)),
            get_existing,
        )

    def update_policy(
        self,
        namespace: str | Sequence[str],
        policy_name: str,
        content: str | dict[str, Any] | None = None,
        description: str | None = None,
        catalog: str | None = None,
    ) -> Policy:
        """
        Update a policy's content and/or description; a no-op when nothing changes.

        Polaris replaces both fields on every update, so a field that is not given keeps its
        current value. The update starts from the latest policy version, again if the policy
        changes in the meantime.
        """
        given_content = self._content(content) if content is not None else None

        def update() -> Policy:
            current = self.load_policy(namespace, policy_name, catalog)
            new_content = given_content if given_content is not None else current.content
            new_description = description if description is not None else current.description
            if _json_equal(new_content, current.content) and new_description == current.description:
                self.log.info("Policy %r is already up to date.", policy_name)
                return current
            request = UpdatePolicyRequest(
                description=new_description, content=new_content, current_policy_version=current.version
            )
            scope = self._scope(namespace, catalog)
            response = self.call(self.policy_api.update_policy, *scope, policy_name, request)
            return _policy(response)

        return self._retry_stale_version(f"Policy {policy_name!r}", update)

    def drop_policy(
        self,
        namespace: str | Sequence[str],
        policy_name: str,
        catalog: str | None = None,
        detach_all: bool = False,
        if_missing: IfMissing = "fail",
    ) -> bool:
        """
        Drop a policy; ``detach_all`` detaches it from all targets first.

        :return: ``True`` if dropped, ``False`` if it was missing and ``if_missing="skip"``.
        """
        return self._skip_missing(
            if_missing,
            self.policy_api.drop_policy,
            *self._scope(namespace, catalog),
            policy_name,
            detach_all=detach_all,
        )

    def attach_policy(
        self,
        namespace: str | Sequence[str],
        policy_name: str,
        target: dict[str, Any] | PolicyAttachmentTarget,
        parameters: dict[str, str] | None = None,
        catalog: str | None = None,
    ) -> None:
        """
        Attach a policy to a catalog, namespace or table. Re-attaching the same policy is a no-op.

        A target holds at most one inheritable policy of each type: if a *different* policy of the
        same type is already attached, Polaris refuses with HTTP 409, raised as a non-retryable
        :class:`PolarisConflictError`. Detach the other policy first.

        :param target: ``{"type": "catalog"}``, ``{"type": "namespace", "path": ["sales"]}`` or
            ``{"type": "table-like", "path": ["sales", "orders"]}``.
        """
        request = AttachPolicyRequest(target=_model(PolicyAttachmentTarget, target), parameters=parameters)
        self.call(self.policy_api.attach_policy, *self._scope(namespace, catalog), policy_name, request)

    def detach_policy(
        self,
        namespace: str | Sequence[str],
        policy_name: str,
        target: dict[str, Any] | PolicyAttachmentTarget,
        catalog: str | None = None,
        if_missing: IfMissing = "fail",
    ) -> bool:
        """
        Detach a policy from a target.

        :return: ``True`` if detached, ``False`` if it was not attached and ``if_missing="skip"``.
        """
        request = DetachPolicyRequest(target=_model(PolicyAttachmentTarget, target))
        return self._skip_missing(
            if_missing, self.policy_api.detach_policy, *self._scope(namespace, catalog), policy_name, request
        )

    def get_applicable_policies(
        self,
        catalog: str | None = None,
        namespace: str | Sequence[str] | None = None,
        table: str | None = None,
        policy_type: str | None = None,
    ) -> list[ApplicablePolicy]:
        """
        Return the policies in effect on a catalog, namespace or table, inheritance resolved.

        :param namespace: Omit for the catalog itself.
        :param table: Table name within ``namespace``; omit for the namespace itself.
        :param policy_type: Only return policies of this type.
        """
        if table and not namespace:
            raise PolarisInvalidArgumentError("table needs a namespace")
        return self._paginate(
            self.policy_api.get_applicable_policies,
            self.resolve_catalog(catalog),
            items="applicable_policies",
            namespace=encode_namespace(namespace) if namespace else None,
            target_name=table,
            policy_type=policy_type,
        )

    # ---- Notifications ------------------------------------------------------------------------

    def send_notification(
        self,
        namespace: str | Sequence[str],
        table: str,
        notification: NotificationRequest | dict[str, Any],
        catalog: str | None = None,
    ) -> None:
        """
        Notify Polaris of a change to a table managed outside Polaris (external catalogs).

        :param notification: e.g. ``{"notification-type": "UPDATE", "payload": {"table-name": ...,
            "timestamp": ..., "table-uuid": ..., "metadata-location": ...}}``.
        """
        request = _model(NotificationRequest, notification)
        self.call(
            self.polaris_catalog_api.send_notification, *self._scope(namespace, catalog), table, request
        )

    # ---- pyiceberg ----------------------------------------------------------------------------

    def get_iceberg_catalog_properties(self, catalog: str | None = None) -> dict[str, Any]:
        """
        Return pyiceberg ``RestCatalog`` properties for this connection (see :meth:`get_iceberg_catalog`).

        The returned dict contains the client secret for ``internal``/``oidc`` connections and the
        bearer token for ``token`` connections; pass it straight to pyiceberg and do not log it or
        return it from a task.
        """
        config = self.config
        props: dict[str, Any] = {
            "type": "rest",
            "uri": config.catalog_url,
            "warehouse": self.resolve_catalog(catalog),
        }
        if config.realm:
            props[f"header.{config.realm_header}"] = config.realm
        if config.verify is not True:
            props["ssl"] = {"cabundle": config.verify}

        if config.auth_type == "token":
            props["token"] = config.token
        elif (
            config.auth_type == "oidc"
            and config.client_auth_method == "client_secret_basic"
            and not (config.audience or config.resource)
            and config.verify is True
        ):
            # pyiceberg's refreshing OAuth2 manager: Basic client auth only, and it posts without the
            # session headers, which is fine for an identity provider (no realm header wanted there),
            # but also without the session's TLS settings, so only with default verification.
            oauth2: dict[str, Any] = {
                "client_id": config.client_id,
                "client_secret": config.client_secret,
                "token_url": config.token_url,
                # pyiceberg refuses a token response without expires_in unless given a default.
                "expires_in": FALLBACK_TOKEN_TTL,
            }
            if config.scope:
                oauth2["scope"] = config.scope
            props["auth"] = {"type": "oauth2", "oauth2": oauth2}
        else:
            # Legacy credential flow: posts through the catalog session, so the realm header reaches
            # Polaris's token endpoint (tokens are realm-bound) and TLS settings apply. Always pass
            # an explicit scope, since pyiceberg's default ("catalog") is rejected by Polaris.
            if config.auth_type == "oidc" and config.client_auth_method == "client_secret_basic":
                self.log.warning(
                    "pyiceberg sends the client secret in the token request body (client_secret_post) "
                    "for connection %r, which sets audience, resource or custom TLS verification.",
                    self.polaris_conn_id,
                )
            props["credential"] = f"{config.client_id}:{config.client_secret}"
            props["oauth2-server-uri"] = config.token_url
            # Without a scope pyiceberg sends "catalog", which identity providers may reject; set
            # the scope extra for oidc connections that take this path.
            if config.scope:
                props["scope"] = config.scope
            if config.audience:
                props["audience"] = config.audience
            if config.resource:
                props["resource"] = config.resource
        return props

    def get_iceberg_catalog(self, catalog: str | None = None, **properties: Any) -> RestCatalog:
        """
        Return a pyiceberg ``RestCatalog`` for a Polaris catalog, authenticated like this hook.

        Requires the ``iceberg`` extra (``pip install apache-airflow-providers-apache-polaris[iceberg]``).

        :param catalog: Polaris catalog (the warehouse). Defaults to the connection's
            ``default_catalog``.
        :param properties: Extra pyiceberg catalog properties (e.g. ``s3.region``), applied last.
        """
        try:
            from pyiceberg.catalog.rest import RestCatalog
        except ImportError as e:
            raise AirflowOptionalProviderFeatureException(
                "get_iceberg_catalog() needs pyiceberg: "
                "pip install 'apache-airflow-providers-apache-polaris[iceberg]'"
            ) from e
        props = {**self.get_iceberg_catalog_properties(catalog), **properties}
        return RestCatalog(f"polaris-{self.polaris_conn_id}", **props)
