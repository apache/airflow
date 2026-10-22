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
"""Resolve inbound webhooks from their ``webhook_<name>`` connections, with a per-process cache."""

from __future__ import annotations

import enum
import json
import math
import threading
from collections.abc import Mapping
from dataclasses import dataclass, field
from time import monotonic
from typing import TYPE_CHECKING

import structlog

from airflow._shared.secrets_backend.base import call_secrets_backend_method
from airflow._shared.secrets_masker import mask_secret
from airflow.configuration import ensure_secrets_loaded
from airflow.models.connection import AirflowSecretsBackendAccessDenied
from airflow.providers_manager import ProvidersManager
from airflow.webhooks.base import WebhookSource

if TYPE_CHECKING:
    from airflow.models.connection import Connection
    from airflow.webhooks.registry import WebhookRegistry

log = structlog.get_logger(__name__)

CONN_ID_PREFIX = "webhook_"
CONN_TYPE = "inbound_webhook"


@dataclass(frozen=True, kw_only=True)
class ConfiguredWebhook:
    """
    A webhook as its ``webhook_<name>`` connection configures it.

    :param name: The webhook's name, as in ``POST /webhooks/<name>``.
    :param source_type: The source type that verifies and parses its deliveries.
    :param source: The ``WebhookSource`` instance built from the connection's extra.
    :param secret: The shared secret, which is the connection's password.
    :param max_body_bytes: The largest body it accepts: ``max_body_bytes`` from the connection's extra,
        or else ``[webhooks] max_body_bytes``.
    """

    name: str
    source_type: str
    source: WebhookSource
    secret: str = field(repr=False)
    max_body_bytes: int


class ResolutionFailure(enum.Enum):
    """The kinds of failure that keep a webhook from being served."""

    #: The name is not listed, or no secrets backend has its connection.
    NOT_FOUND = "not_found"
    #: A secrets backend failed, so the connection may exist. This is never cached.
    UNAVAILABLE = "unavailable"
    #: The connection does not configure a webhook that can be served.
    MISCONFIGURED = "misconfigured"


@dataclass(frozen=True, kw_only=True)
class UnresolvedWebhook:
    """
    A webhook that cannot be served, and why.

    :param failure: What kind of failure it is.
    :param reason: What is wrong, for the log and the Deployment Manager; never for the sender.
    """

    failure: ResolutionFailure
    reason: str


def _configure_webhook(
    name: str, conn: Connection, *, source_type_owners: Mapping[str, str], default_max_body_bytes: int
) -> ConfiguredWebhook | UnresolvedWebhook:
    try:
        if conn.conn_type != CONN_TYPE:
            raise ValueError(f"its type is {conn.conn_type!r}, not {CONN_TYPE!r}")
        extra_json = conn.extra
        extra = json.loads(extra_json) if extra_json else {}
        mask_secret(extra)
        # The connection form saves fields left empty as null or "", which mean "not set".
        extra = {key: value for key, value in extra.items() if value not in (None, "")}
        if not conn.password:
            raise ValueError("its password, which holds the shared secret, is empty")
        source_type = extra.get("source_type", name)
        if not isinstance(source_type, str):
            raise ValueError("source_type must be a string")
        owner = source_type_owners.get(source_type)
        providers_manager = ProvidersManager()
        source_class = providers_manager.get_webhook_source_class(source_type, package_name=owner)
        if source_class is None:
            declarations = providers_manager.webhook_source_conflicts.get(source_type)
            if owner is None and declarations:
                packages = ", ".join(info.package_name for info in declarations)
                return UnresolvedWebhook(
                    failure=ResolutionFailure.MISCONFIGURED,
                    reason=f"source type {source_type!r} is declared by {packages}; set [webhooks] "
                    "source_type_owners to the package that serves it",
                )
            named = f" ([webhooks] source_type_owners names {owner})" if owner else ""
            raise ValueError(f"no installed provider serves source type {source_type!r}{named}")
        if not (isinstance(source_class, type) and issubclass(source_class, WebhookSource)):
            raise ValueError(f"source type {source_type!r} is not a subclass of WebhookSource")
        # A connection given as a URI has string values in its extra.
        max_body_bytes = str(extra.get("max_body_bytes", default_max_body_bytes))
        if not max_body_bytes.isdecimal() or int(max_body_bytes) < 1:
            raise ValueError("max_body_bytes must be a positive integer")
        return ConfiguredWebhook(
            name=name,
            source_type=source_type,
            source=source_class.from_extra(extra),
            secret=conn.password,
            max_body_bytes=int(max_body_bytes),
        )
    except ValueError as e:
        return UnresolvedWebhook(failure=ResolutionFailure.MISCONFIGURED, reason=str(e))
    except Exception as e:
        # Such as a password encrypted with another Fernet key, or a source type's from_extra failing
        # unexpectedly. Retrying cannot help until the connection or the deployment changes, so this is
        # a misconfiguration, cached like the others, not an error raised on every request.
        log.debug(
            "Could not configure a webhook from its connection",
            conn_id=f"{CONN_ID_PREFIX}{name}",
            exc_info=True,
        )
        return UnresolvedWebhook(
            failure=ResolutionFailure.MISCONFIGURED, reason=f"the connection could not be used: {e!r}"
        )


def resolve_webhook(
    name: str, *, source_type_owners: Mapping[str, str], default_max_body_bytes: int
) -> ConfiguredWebhook | UnresolvedWebhook:
    """
    Resolve webhook ``name`` from connection ``webhook_<name>``, without a cache.

    The connection must have the type ``inbound_webhook``, and its password is the shared secret. Its
    extra may set ``source_type``, which defaults to ``name``, ``max_body_bytes``, and the options the
    source type documents.

    :param name: The webhook's name.
    :param source_type_owners: The package that serves each source type listed, as in
        ``WebhookRegistry``.
    :param default_max_body_bytes: The largest body the webhook accepts if its connection does not
        set ``max_body_bytes``, as in ``WebhookRegistry``.
    """
    conn_id = f"{CONN_ID_PREFIX}{name}"
    backend_failure = None
    # Connection.get_connection_from_secrets would report a backend that fails, such as the metadata
    # database during an outage, as a missing connection, which is answered 404 and cached.
    for backend in ensure_secrets_loaded():
        try:
            conn = call_secrets_backend_method(backend.get_connection, team_name=None, conn_id=conn_id)
        except AirflowSecretsBackendAccessDenied:
            return UnresolvedWebhook(
                failure=ResolutionFailure.MISCONFIGURED,
                reason=f"secrets backend {type(backend).__name__} denies access",
            )
        except Exception as e:
            log.debug(
                "Secrets backend could not read a webhook connection",
                backend=type(backend).__name__,
                conn_id=conn_id,
                exc_info=True,
            )
            backend_failure = backend_failure or f"secrets backend {type(backend).__name__} failed: {e}"
            continue
        if conn is not None:
            return _configure_webhook(
                name,
                conn,
                source_type_owners=source_type_owners,
                default_max_body_bytes=default_max_body_bytes,
            )
    if backend_failure is not None:
        return UnresolvedWebhook(failure=ResolutionFailure.UNAVAILABLE, reason=backend_failure)
    return UnresolvedWebhook(failure=ResolutionFailure.NOT_FOUND, reason=f"there is no connection {conn_id}")


def _is_cacheable(resolution: ConfiguredWebhook | UnresolvedWebhook) -> bool:
    return not (
        isinstance(resolution, UnresolvedWebhook) and resolution.failure is ResolutionFailure.UNAVAILABLE
    )


class WebhookConnectionCache:
    """
    Resolves listed webhooks, reading each one's connection at most once per ``connection_cache_ttl``.

    Deliveries are unauthenticated, so a name that is not listed is refused without a lookup, a failed
    lookup is reused until the TTL is up, and failed verifications read a connection again at most once
    per TTL (``reread``). Otherwise anyone could make every request cost a call to a secrets backend.
    A lookup that a secrets backend failed is not cached, so that an outage is not answered as a
    missing webhook once it is over; it is used only by the requests that waited for it.

    ``get`` and ``reread`` block while a connection is read, for as long as a secrets backend takes, so
    call them from a worker thread rather than the event loop.

    Each API server process keeps its own cache.
    """

    def __init__(self, registry: WebhookRegistry) -> None:
        self._registry = registry
        # Each listed webhook's resolution, with its monotonic() expiry.
        self._resolutions: dict[str, tuple[float, ConfiguredWebhook | UnresolvedWebhook]] = {}
        # One per listed webhook, held while resolving it, so the requests that find its entry expired
        # make one lookup, not one each, and a slow lookup holds up only its own webhook. Only listed
        # names get one.
        self._locks: dict[str, threading.Lock] = {}
        # How many lookups of each listed webhook's connection have finished, and the latest one's result.
        self._lookups: dict[str, tuple[int, ConfiguredWebhook | UnresolvedWebhook]] = {}
        # When a delivery that fails verification may next have each listed webhook's connection read
        # again, in monotonic().
        self._rereads_allowed_at: dict[str, float] = {}

    def _look_up(self, name: str) -> ConfiguredWebhook | UnresolvedWebhook:
        """Read webhook ``name``'s connection; the caller holds its lock."""
        resolution = resolve_webhook(
            name,
            source_type_owners=self._registry.source_type_owners,
            default_max_body_bytes=self._registry.max_body_bytes,
        )
        count, previous = self._lookups.get(name, (0, None))
        self._lookups[name] = (count + 1, resolution)
        if (
            isinstance(resolution, UnresolvedWebhook)
            and resolution.failure is ResolutionFailure.MISCONFIGURED
            and resolution != previous
        ):
            # Once until it changes, not once per lookup: with a TTL of 0 there is a lookup per request.
            log.error(
                "Webhook connection is misconfigured",
                conn_id=f"{CONN_ID_PREFIX}{name}",
                reason=resolution.reason,
            )
        return resolution

    def _get_cached(self, name: str) -> ConfiguredWebhook | UnresolvedWebhook | None:
        expires_at, resolution = self._resolutions.get(name, (0.0, None))
        return resolution if monotonic() < expires_at else None

    def get(self, name: str) -> ConfiguredWebhook | UnresolvedWebhook:
        """Resolve webhook ``name``, reading its connection only if the cached resolution has expired."""
        if name not in self._registry.names:
            return UnresolvedWebhook(
                failure=ResolutionFailure.NOT_FOUND, reason="it is not listed in [webhooks] names"
            )
        if (resolution := self._get_cached(name)) is not None:
            return resolution
        lookups_seen = self._lookups.get(name, (0, None))[0]
        # setdefault is atomic, so concurrent first requests for a name share one lock.
        with self._locks.setdefault(name, threading.Lock()):
            if (resolution := self._get_cached(name)) is not None:
                return resolution
            lookups, latest = self._lookups.get(name, (0, None))
            if lookups != lookups_seen and latest is not None:
                # A lookup finished while this request waited. Using it keeps requests from each making
                # one more lookup, in turn, behind a backend that fails slowly, whose failure is not cached.
                return latest
            resolution = self._look_up(name)
            if _is_cacheable(resolution):
                self._resolutions[name] = (monotonic() + self._registry.connection_cache_ttl, resolution)
        return resolution

    def reread(self, webhook: ConfiguredWebhook) -> ConfiguredWebhook | None:
        """
        Read a webhook's connection again, after a delivery failed verification against ``webhook``.

        The cached entry keeps its expiry, so a read that comes before the secret is changed does not
        keep the old secret cached for longer.

        :return: The webhook as its connection now configures it, to verify the delivery against again,
            so that a secret changed in the connection applies to the first delivery signed with it.
            None if the connection may not be read again yet, or no longer configures a webhook.
        """
        if (ttl := self._registry.connection_cache_ttl) <= 0:
            # Without a cache, the connection was read for this delivery.
            return None
        with self._locks.setdefault(webhook.name, threading.Lock()):
            expires_at, resolution = self._resolutions.get(webhook.name, (0.0, None))
            if isinstance(resolution, ConfiguredWebhook) and resolution is not webhook:
                # Another delivery has had the connection read since this one got it.
                return resolution
            now = monotonic()
            if now < self._rereads_allowed_at.get(webhook.name, -math.inf):
                return None
            self._rereads_allowed_at[webhook.name] = now + ttl
            resolution = self._look_up(webhook.name)
            if _is_cacheable(resolution):
                self._resolutions[webhook.name] = (expires_at, resolution)
        return resolution if isinstance(resolution, ConfiguredWebhook) else None
