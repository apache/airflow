#
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
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import timedelta
from typing import TYPE_CHECKING, Any
from unittest import mock

import pytest
from cryptography.fernet import Fernet
from sqlalchemy.exc import OperationalError

from airflow.models import crypto
from airflow.models.connection import Connection
from airflow.providers_manager import ProviderInfo, ProvidersManager
from airflow.sdk.exceptions import AirflowSecretsBackendAccessDenied
from airflow.secrets.base_secrets import BaseSecretsBackend
from airflow.webhooks.connections import (
    AssetRoute,
    ConfiguredWebhook,
    ResolutionFailure,
    UnresolvedWebhook,
    WebhookConnectionCache,
    WebhookSourceTypeStatus,
    resolve_webhook,
)
from airflow.webhooks.registry import WebhookRegistry

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.webhooks import FakeHmacSource

if TYPE_CHECKING:
    from unittest.mock import MagicMock

    from airflow.webhooks.base import StorePayload, WebhookSource

SECRET = "s3cret"
CONN_ID = "webhook_github"
DEFAULT_MAX_BODY_BYTES = 1048576
REGISTRY = WebhookRegistry(
    names=frozenset({"github", "gitlab"}),
    source_type_owners={},
    connection_cache_ttl=60,
    max_body_bytes=DEFAULT_MAX_BODY_BYTES,
    delivery_retention=timedelta(days=7),
    rate_limit_per_minute=0,
    store_payload="full",
    max_event_payload_bytes=65536,
)


class NotAWebhookSource:
    pass


class RefusingSource(FakeHmacSource):
    @classmethod
    def from_extra(cls, extra: dict[str, Any]) -> RefusingSource:
        raise ValueError("region is required")


class CrashingSource(FakeHmacSource):
    @classmethod
    def from_extra(cls, extra: dict[str, Any]) -> CrashingSource:
        raise TypeError("unexpected extra")


class SummarizingSource(FakeHmacSource):
    default_store_payload = "summary"


class ObservedLock:
    """A lock that tells when a thread has to wait for it."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.contended = threading.Event()

    def __enter__(self) -> None:
        if not self._lock.acquire(blocking=False):
            self.contended.set()
            self._lock.acquire()

    def __exit__(self, *exc_info: object) -> None:
        self._lock.release()


class FakeSecretsBackend(BaseSecretsBackend):
    """Holds connections in memory and counts its lookups."""

    def __init__(self, *connections: Connection, error: Exception | None = None) -> None:
        self.connections = {conn.conn_id: conn for conn in connections}
        self.error = error
        self.lookups = 0

    def get_connection(self, conn_id: str, team_name: str | None = None) -> Connection | None:
        self.lookups += 1
        if self.error is not None:
            raise self.error
        return self.connections.get(conn_id)


def build_connection(
    *, conn_type: str = "inbound_webhook", password: str | None = SECRET, **extra
) -> Connection:
    return Connection(conn_id=CONN_ID, conn_type=conn_type, password=password, extra=extra or None)


def build_misconfiguration(
    reason: str, source_type: str | None = None, source_type_status: WebhookSourceTypeStatus | None = None
) -> UnresolvedWebhook:
    return UnresolvedWebhook(
        failure=ResolutionFailure.MISCONFIGURED,
        reason=reason,
        source_type=source_type,
        source_type_status=source_type_status,
    )


def build_backend_failure() -> OperationalError:
    return OperationalError("SELECT", {}, Exception("database is locked"))


def resolve_github(
    *,
    source_type_owners: dict[str, str] | None = None,
    default_max_body_bytes: int = DEFAULT_MAX_BODY_BYTES,
    default_store_payload: StorePayload = "full",
) -> ConfiguredWebhook | UnresolvedWebhook:
    return resolve_webhook(
        "github",
        source_type_owners=source_type_owners or {},
        default_max_body_bytes=default_max_body_bytes,
        default_store_payload=default_store_payload,
    )


@pytest.fixture(autouse=True)
def providers(cleanup_providers_manager):
    ProvidersManager()._provider_dict = {
        "fake-webhooks": ProviderInfo(
            version="0.0.1",
            data={
                "webhook-sources": [
                    {"source-type": "github", "classpath": f"{FakeHmacSource.__module__}.FakeHmacSource"},
                    {"source-type": "refusing", "classpath": f"{__name__}.RefusingSource"},
                    {"source-type": "crashing", "classpath": f"{__name__}.CrashingSource"},
                    {"source-type": "summarizing", "classpath": f"{__name__}.SummarizingSource"},
                    {"source-type": "not_a_source", "classpath": f"{__name__}.NotAWebhookSource"},
                ]
            },
        ),
        "fake-webhooks-first": ProviderInfo(
            version="0.0.1",
            data={"webhook-sources": [{"source-type": "dup", "classpath": f"{__name__}.RefusingSource"}]},
        ),
        "fake-webhooks-second": ProviderInfo(
            version="0.0.1",
            data={
                "webhook-sources": [
                    {"source-type": "dup", "classpath": f"{FakeHmacSource.__module__}.FakeHmacSource"}
                ]
            },
        ),
    }


@pytest.fixture
def fernet_key():
    with conf_vars({("core", "fernet_key"): Fernet.generate_key().decode()}):
        crypto.get_fernet.cache_clear()
        yield
    crypto.get_fernet.cache_clear()


@pytest.fixture
def backend():
    """The only secrets backend, holding connection webhook_github."""
    backend = FakeSecretsBackend(build_connection())
    with mock.patch(
        "airflow.webhooks.connections.ensure_secrets_loaded", autospec=True, return_value=[backend]
    ):
        yield backend


def assert_configured(
    resolution: ConfiguredWebhook | UnresolvedWebhook | None,
    *,
    source_class: type[WebhookSource] = FakeHmacSource,
    source_type: str = "github",
    secret: str = SECRET,
    max_body_bytes: int = DEFAULT_MAX_BODY_BYTES,
) -> None:
    assert isinstance(resolution, ConfiguredWebhook)
    assert type(resolution.source) is source_class
    assert (resolution.name, resolution.source_type, resolution.secret, resolution.max_body_bytes) == (
        "github",
        source_type,
        secret,
        max_body_bytes,
    )


class TestResolveWebhook:
    @pytest.mark.parametrize(
        ("extra", "expected_max_body_bytes"),
        [
            pytest.param({}, 4096, id="defaults"),
            pytest.param({"source_type": "github", "max_body_bytes": 2048}, 2048, id="extra"),
            pytest.param({"max_body_bytes": 8192}, 8192, id="extra-above-default"),
            # As the connection form saves fields left empty.
            pytest.param({"source_type": "", "max_body_bytes": None}, 4096, id="empty-fields"),
        ],
    )
    def test_connection_configures_the_webhook(self, backend, extra, expected_max_body_bytes):
        backend.connections[CONN_ID] = build_connection(**extra)

        resolution = resolve_github(default_max_body_bytes=4096)

        assert_configured(resolution, max_body_bytes=expected_max_body_bytes)
        assert SECRET not in repr(resolution)

    @pytest.mark.parametrize(
        ("extra", "expected_store_payload"),
        [
            pytest.param({"source_type": "summarizing", "store_payload": "full"}, "full", id="connection"),
            pytest.param({"source_type": "summarizing"}, "summary", id="source-type"),
            pytest.param({}, "none", id="option"),
            # As the connection form saves a field left empty.
            pytest.param({"source_type": "summarizing", "store_payload": ""}, "summary", id="empty-field"),
        ],
    )
    def test_store_payload_is_the_connections_or_else_the_source_types_or_else_the_options(
        self, backend, extra, expected_store_payload
    ):
        backend.connections[CONN_ID] = build_connection(**extra)

        resolution = resolve_github(default_store_payload="none")

        assert isinstance(resolution, ConfiguredWebhook)
        assert resolution.store_payload == expected_store_payload

    @pytest.mark.parametrize(
        ("extra", "expected_routes"),
        [
            pytest.param({}, (), id="none"),
            # As the connection form saves a field left empty.
            pytest.param({"asset_routes": None}, (), id="empty-field"),
            pytest.param(
                {
                    "asset_routes": [
                        {"topic": "github/my-org/web/push", "asset": "s3://exports/web"},
                        {"topic": "github/*/push", "asset": "pushes", "partition_key_field": "ref"},
                    ]
                },
                (
                    AssetRoute(topic="github/my-org/web/push", asset="s3://exports/web"),
                    AssetRoute(topic="github/*/push", asset="pushes", partition_key_field="ref"),
                ),
                id="list",
            ),
            # As in a connection given as a URI.
            pytest.param(
                {"asset_routes": '[{"topic": "github/*/push", "asset": "pushes"}]'},
                (AssetRoute(topic="github/*/push", asset="pushes"),),
                id="json",
            ),
        ],
    )
    def test_asset_routes_are_read_from_the_extra(self, backend, extra, expected_routes):
        backend.connections[CONN_ID] = build_connection(**extra)

        resolution = resolve_github()

        assert isinstance(resolution, ConfiguredWebhook)
        assert resolution.asset_routes == expected_routes

    @mock.patch("airflow.webhooks.connections.mask_secret", autospec=True)
    def test_extra_is_registered_with_the_secrets_masker(self, mock_mask_secret, backend):
        backend.connections[CONN_ID] = build_connection(api_token="t0ken", region=None)

        resolve_github()

        mock_mask_secret.assert_called_once_with({"api_token": "t0ken", "region": None})

    @mock.patch.dict(
        os.environ, {"AIRFLOW_CONN_WEBHOOK_GITHUB": "inbound-webhook://:s3cret@/?max_body_bytes=2048"}
    )
    def test_connection_from_an_environment_variable_uri(self):
        assert_configured(resolve_github(), max_body_bytes=2048)

    @pytest.mark.parametrize(
        ("connection", "expected"),
        [
            pytest.param(
                {"conn_type": "generic"},
                UnresolvedWebhook(
                    failure=ResolutionFailure.WRONG_TYPE,
                    reason="its type is 'generic', not 'inbound_webhook'",
                ),
                id="type",
            ),
            pytest.param(
                {"password": None},
                build_misconfiguration("its password, which holds the shared secret, is empty"),
                id="password",
            ),
            pytest.param(
                {"source_type": 1},
                build_misconfiguration("source_type must be a string"),
                id="non-string-source-type",
            ),
            pytest.param(
                {"source_type": "bitbucket"},
                build_misconfiguration(
                    "no installed provider serves source type 'bitbucket'",
                    "bitbucket",
                    WebhookSourceTypeStatus.UNKNOWN,
                ),
                id="unknown-source-type",
            ),
            pytest.param(
                {"source_type": "hmac_json"},
                build_misconfiguration(
                    "no installed provider serves source type 'hmac_json'; it needs "
                    "apache-airflow-providers-standard 1.21.0 or later",
                    "hmac_json",
                    WebhookSourceTypeStatus.UNKNOWN,
                ),
                id="standard-source-type",
            ),
            pytest.param(
                {"source_type": "not_a_source"},
                build_misconfiguration(
                    "source type 'not_a_source' is not a subclass of WebhookSource",
                    "not_a_source",
                    WebhookSourceTypeStatus.UNKNOWN,
                ),
                id="not-a-webhook-source",
            ),
            pytest.param(
                {"source_type": "refusing"},
                build_misconfiguration("region is required", "refusing", WebhookSourceTypeStatus.OK),
                id="refused-by-source",
            ),
            pytest.param(
                {"max_body_bytes": 0},
                build_misconfiguration(
                    "max_body_bytes must be a positive integer", "github", WebhookSourceTypeStatus.OK
                ),
                id="zero-max-body-bytes",
            ),
            pytest.param(
                {"max_body_bytes": "lots"},
                build_misconfiguration(
                    "max_body_bytes must be a positive integer", "github", WebhookSourceTypeStatus.OK
                ),
                id="non-integer-max-body-bytes",
            ),
            pytest.param(
                {"store_payload": "some"},
                build_misconfiguration(
                    "store_payload must be one of full, summary, none", "github", WebhookSourceTypeStatus.OK
                ),
                id="unknown-store-payload",
            ),
        ],
    )
    def test_misconfigured_connection(self, backend, connection, expected):
        backend.connections[CONN_ID] = build_connection(**connection)

        resolution = resolve_github()

        assert resolution == expected

    @pytest.mark.parametrize(
        ("asset_routes", "reason"),
        [
            pytest.param(
                {"topic": "github/*/push", "asset": "pushes"}, "asset_routes must be a list", id="object"
            ),
            pytest.param("github/*/push", "asset_routes must be a JSON list", id="not-json"),
            pytest.param('{"topic": "github/*/push"}', "asset_routes must be a list", id="json-object"),
            pytest.param(["github/*/push"], "asset_routes[0] must be an object", id="route-not-an-object"),
            pytest.param(
                [{"topic": "github/*/push", "asset": "pushes", "partition_key": "ref"}],
                "asset_routes[0] has unknown keys: partition_key",
                id="unknown-key",
            ),
            pytest.param(
                [{"asset": "pushes"}], "asset_routes[0] topic must be a non-empty string", id="no-topic"
            ),
            pytest.param(
                [{"topic": "github/*/push", "asset": "pushes"}, {"topic": "github/*/push", "asset": ""}],
                "asset_routes[1] asset must be a non-empty string",
                id="empty-asset",
            ),
            pytest.param(
                [{"topic": "github/*/push", "asset": 7}],
                "asset_routes[0] asset must be a non-empty string",
                id="non-string-asset",
            ),
            pytest.param(
                [{"topic": "github/*/push", "asset": "pushes", "partition_key_field": None}],
                "asset_routes[0] partition_key_field must be a non-empty string",
                id="null-partition-key-field",
            ),
            pytest.param(
                [{"topic": "github/*/push", "asset": "pushes", "partition_key_field": ["ref"]}],
                "asset_routes[0] partition_key_field must be a non-empty string",
                id="non-string-partition-key-field",
            ),
        ],
    )
    def test_asset_routes_of_any_other_shape_are_misconfigured(self, backend, asset_routes, reason):
        backend.connections[CONN_ID] = build_connection(asset_routes=asset_routes)

        resolution = resolve_github()

        assert resolution == build_misconfiguration(reason, "github", WebhookSourceTypeStatus.OK)

    @pytest.mark.parametrize(
        "source_type_owners",
        [pytest.param({}, id="no-owner"), pytest.param({"hmac_json": "fake-webhooks-broken"}, id="owner")],
    )
    def test_declared_source_type_whose_class_cannot_be_imported(self, backend, source_type_owners):
        ProvidersManager()._provider_dict["fake-webhooks-broken"] = ProviderInfo(
            version="0.0.1",
            data={
                "webhook-sources": [{"source-type": "hmac_json", "classpath": f"{__name__}.MissingSource"}]
            },
        )
        backend.connections[CONN_ID] = build_connection(source_type="hmac_json")

        resolution = resolve_github(source_type_owners=source_type_owners)

        assert resolution == build_misconfiguration(
            f"fake-webhooks-broken declares source type 'hmac_json', but its class {__name__}.MissingSource "
            "could not be imported or is not a class",
            "hmac_json",
            WebhookSourceTypeStatus.UNKNOWN,
        )

    def test_malformed_extra_is_misconfigured(self, backend):
        # As a metadata database row written before Connection validated extra.
        backend.connections[CONN_ID]._extra = "not json"
        backend.connections[CONN_ID].is_extra_encrypted = False

        resolution = resolve_github()

        assert isinstance(resolution, UnresolvedWebhook)
        assert resolution.failure is ResolutionFailure.MISCONFIGURED
        assert "non-JSON" in resolution.reason

    @pytest.mark.parametrize(
        ("source_type_owners", "expected"),
        [
            pytest.param(
                {},
                build_misconfiguration(
                    "source type 'dup' is declared by fake-webhooks-first, fake-webhooks-second; set "
                    "[webhooks] source_type_owners to the package that serves it",
                    "dup",
                    WebhookSourceTypeStatus.CONFLICT,
                ),
                id="no-owner",
            ),
            pytest.param({"dup": "fake-webhooks-second"}, None, id="owner"),
            pytest.param(
                {"dup": "fake-webhooks"},
                build_misconfiguration(
                    "no installed provider serves source type 'dup' "
                    "([webhooks] source_type_owners names fake-webhooks)",
                    "dup",
                    WebhookSourceTypeStatus.UNKNOWN,
                ),
                id="owner-that-does-not-declare-it",
            ),
        ],
    )
    def test_source_type_declared_by_two_providers(self, backend, source_type_owners, expected):
        backend.connections[CONN_ID] = build_connection(source_type="dup")

        resolution = resolve_github(source_type_owners=source_type_owners)

        if expected is None:
            assert_configured(resolution, source_type="dup")
        else:
            assert resolution == expected

    @pytest.mark.parametrize(
        ("backend_states", "expected_failure"),
        [
            pytest.param(["missing", "missing"], ResolutionFailure.NOT_FOUND, id="not-found"),
            pytest.param(["failing"], ResolutionFailure.UNAVAILABLE, id="failing"),
            pytest.param(["missing", "failing"], ResolutionFailure.UNAVAILABLE, id="missing-then-failing"),
            pytest.param(["failing", "holding"], None, id="failing-then-holding"),
        ],
    )
    def test_backends_are_tried_in_order(self, backend_states, expected_failure):
        backends = [
            FakeSecretsBackend(
                *([build_connection()] if state == "holding" else []),
                error=build_backend_failure() if state == "failing" else None,
            )
            for state in backend_states
        ]

        with mock.patch(
            "airflow.webhooks.connections.ensure_secrets_loaded", autospec=True, return_value=backends
        ):
            resolution = resolve_github()

        if expected_failure is None:
            assert_configured(resolution)
        else:
            assert isinstance(resolution, UnresolvedWebhook)
            assert resolution.failure is expected_failure
        assert [backend.lookups for backend in backends] == [1] * len(backends)

    def test_access_denied_is_misconfigured_and_stops_the_lookup(self):
        backends = [
            FakeSecretsBackend(error=AirflowSecretsBackendAccessDenied("denied")),
            FakeSecretsBackend(build_connection()),
        ]

        with mock.patch(
            "airflow.webhooks.connections.ensure_secrets_loaded", autospec=True, return_value=backends
        ):
            resolution = resolve_github()

        assert resolution == UnresolvedWebhook(
            failure=ResolutionFailure.MISCONFIGURED, reason="secrets backend FakeSecretsBackend denies access"
        )
        assert backends[1].lookups == 0


@mock.patch("airflow.webhooks.connections.monotonic", autospec=True, return_value=1000.0)
class TestWebhookConnectionCache:
    def test_name_not_listed_is_refused_without_a_lookup(self, mock_monotonic, backend):
        resolution = WebhookConnectionCache(REGISTRY).get("jira")

        assert resolution == UnresolvedWebhook(
            failure=ResolutionFailure.NOT_FOUND, reason="it is not listed in [webhooks] names"
        )
        assert backend.lookups == 0

    def test_connection_is_read_once_per_ttl(self, mock_monotonic: MagicMock, backend):
        cache = WebhookConnectionCache(REGISTRY)

        first = cache.get("github")
        assert cache.get("github") is first
        assert backend.lookups == 1

        mock_monotonic.return_value = 1060.0
        assert_configured(cache.get("github"))
        assert backend.lookups == 2

    @pytest.mark.parametrize(
        "connection",
        [
            pytest.param(None, id="not-found"),
            pytest.param({"conn_type": "generic"}, id="misconfigured"),
        ],
    )
    def test_failed_lookup_is_cached(self, mock_monotonic, backend, connection):
        backend.connections = {} if connection is None else {CONN_ID: build_connection(**connection)}
        cache = WebhookConnectionCache(REGISTRY)

        first = cache.get("github")
        assert isinstance(first, UnresolvedWebhook)
        assert cache.get("github") is first
        assert backend.lookups == 1

    @pytest.mark.usefixtures("fernet_key")
    @pytest.mark.parametrize(
        ("source_type", "encrypt_with_another_key", "expected"),
        [
            pytest.param(
                "github",
                True,
                # The password is read before the source type.
                build_misconfiguration("the connection could not be used: InvalidToken()"),
                id="password-encrypted-with-another-key",
            ),
            pytest.param(
                "crashing",
                False,
                build_misconfiguration(
                    "the connection could not be used: TypeError('unexpected extra')",
                    "crashing",
                    WebhookSourceTypeStatus.OK,
                ),
                id="source-type-error",
            ),
        ],
    )
    def test_unexpected_error_is_cached_as_misconfigured(
        self, mock_monotonic, backend, source_type, encrypt_with_another_key, expected
    ):
        conn = build_connection(source_type=source_type)
        if encrypt_with_another_key:
            conn._password = Fernet(Fernet.generate_key()).encrypt(SECRET.encode()).decode()
            conn.is_encrypted = True
        backend.connections[CONN_ID] = conn
        cache = WebhookConnectionCache(REGISTRY)

        resolutions = [cache.get("github") for _ in range(2)]

        assert resolutions == 2 * [expected]
        assert backend.lookups == 1

    def test_misconfiguration_is_logged_once_until_it_changes(self, mock_monotonic, backend, caplog):
        backend.connections[CONN_ID] = build_connection(conn_type="generic")
        # A lookup per request, which must not mean a log line per request.
        cache = WebhookConnectionCache(replace(REGISTRY, connection_cache_ttl=0))

        cache.get("github")
        cache.get("github")
        backend.connections[CONN_ID] = build_connection(password=None)
        cache.get("github")

        assert backend.lookups == 3
        assert [
            (entry["log_level"], entry["conn_id"], entry["reason"])
            for entry in caplog
            if entry["event"] == "Webhook connection is misconfigured"
        ] == [
            ("error", CONN_ID, "its type is 'generic', not 'inbound_webhook'"),
            ("error", CONN_ID, "its password, which holds the shared secret, is empty"),
        ]

    def test_backend_failure_is_not_cached(self, mock_monotonic, backend):
        backend.error = build_backend_failure()
        cache = WebhookConnectionCache(REGISTRY)

        resolutions = [cache.get("github") for _ in range(2)]
        backend.error = None
        resolutions.append(cache.get("github"))

        assert [getattr(resolution, "failure", None) for resolution in resolutions] == [
            ResolutionFailure.UNAVAILABLE,
            ResolutionFailure.UNAVAILABLE,
            None,
        ]
        assert backend.lookups == 3

    def test_connection_is_read_on_every_request_without_a_cache(self, mock_monotonic, backend):
        cache = WebhookConnectionCache(replace(REGISTRY, connection_cache_ttl=0))

        webhook = cache.get("github")
        assert_configured(webhook)
        assert_configured(cache.get("github"))
        assert isinstance(webhook, ConfiguredWebhook)
        # The connection was read for the delivery that failed verification.
        assert cache.reread(webhook) is None
        assert backend.lookups == 2

    def test_failed_verifications_read_the_connection_again_once_per_ttl(
        self, mock_monotonic: MagicMock, backend
    ):
        cache = WebhookConnectionCache(REGISTRY)

        def reread_after_failed_verification() -> ConfiguredWebhook | None:
            webhook = cache.get("github")
            assert isinstance(webhook, ConfiguredWebhook)
            return cache.reread(webhook)

        # Once, however many fail: anyone can send a delivery that fails verification.
        assert reread_after_failed_verification() is not None
        assert reread_after_failed_verification() is None
        assert backend.lookups == 2

        mock_monotonic.return_value = 1059.0
        assert reread_after_failed_verification() is None
        assert backend.lookups == 2

        # The entry has expired, and the connection may be read again.
        mock_monotonic.return_value = 1100.0
        assert reread_after_failed_verification() is not None
        assert backend.lookups == 4

    def test_changed_secret_applies_to_the_first_delivery_signed_with_it(self, mock_monotonic, backend):
        cache = WebhookConnectionCache(REGISTRY)
        webhook = cache.get("github")
        assert isinstance(webhook, ConfiguredWebhook)
        backend.connections[CONN_ID] = build_connection(password="rotated")

        reread = cache.reread(webhook)

        assert_configured(reread, secret="rotated")
        assert cache.get("github") is reread
        assert backend.lookups == 2

    def test_reading_the_connection_again_does_not_keep_it_cached_for_longer(
        self, mock_monotonic: MagicMock, backend
    ):
        cache = WebhookConnectionCache(REGISTRY)
        webhook = cache.get("github")
        assert isinstance(webhook, ConfiguredWebhook)
        # The sender has its new secret before the connection does, so reading it again finds the old.
        mock_monotonic.return_value = 1030.0
        assert_configured(cache.reread(webhook), secret=SECRET)
        backend.connections[CONN_ID] = build_connection(password="rotated")

        mock_monotonic.return_value = 1059.0
        assert_configured(cache.get("github"), secret=SECRET)
        # The connection read at 1000 expires at 1060, as it would have without the second read.
        mock_monotonic.return_value = 1060.0
        assert_configured(cache.get("github"), secret="rotated")
        assert backend.lookups == 3

    def test_failed_verification_uses_a_connection_read_since_without_reading_it_again(
        self, mock_monotonic, backend
    ):
        cache = WebhookConnectionCache(REGISTRY)
        cached = cache.get("github")
        assert isinstance(cached, ConfiguredWebhook)
        # As when another delivery had the connection read again while this one was being verified.
        newer = cache.reread(cached)

        assert cache.reread(cached) is newer
        assert backend.lookups == 2

    @pytest.mark.parametrize(
        ("connection", "backend_fails", "keeps_webhook"),
        [
            pytest.param(None, False, False, id="removed"),
            pytest.param({"conn_type": "generic"}, False, False, id="misconfigured"),
            pytest.param({}, True, True, id="backend-failing"),
        ],
    )
    def test_reading_a_connection_that_no_longer_configures_the_webhook(
        self, mock_monotonic, backend, connection, backend_fails, keeps_webhook
    ):
        cache = WebhookConnectionCache(REGISTRY)
        webhook = cache.get("github")
        assert isinstance(webhook, ConfiguredWebhook)
        backend.connections = {} if connection is None else {CONN_ID: build_connection(**connection)}
        backend.error = build_backend_failure() if backend_fails else None

        assert cache.reread(webhook) is None
        assert (cache.get("github") is webhook) is keeps_webhook
        assert backend.lookups == 2

    def test_slow_lookup_holds_up_only_its_own_webhook(self, mock_monotonic, backend):
        gitlab_lookup_started, finish_gitlab_lookup = threading.Event(), threading.Event()
        get_connection = backend.get_connection

        def get_slowly(conn_id: str, team_name: str | None = None) -> Connection | None:
            if conn_id == "webhook_gitlab":
                gitlab_lookup_started.set()
                finish_gitlab_lookup.wait(timeout=30)
            return get_connection(conn_id, team_name)

        backend.get_connection = get_slowly  # type: ignore[method-assign]
        cache = WebhookConnectionCache(REGISTRY)

        with ThreadPoolExecutor(2) as executor:
            try:
                executor.submit(cache.get, "gitlab")
                assert gitlab_lookup_started.wait(timeout=30)
                # A lock shared by every webhook would keep this waiting until the gitlab lookup finished.
                resolution = executor.submit(cache.get, "github").result(timeout=10)
            finally:
                finish_gitlab_lookup.set()

        assert_configured(resolution)

    def test_requests_that_wait_for_a_failing_lookup_share_it(self, mock_monotonic, backend):
        lookup_started, finish_lookup = threading.Event(), threading.Event()
        get_connection = backend.get_connection

        def fail_slowly(conn_id: str, team_name: str | None = None) -> Connection | None:
            lookup_started.set()
            finish_lookup.wait(timeout=30)
            return get_connection(conn_id, team_name)

        backend.get_connection = fail_slowly  # type: ignore[method-assign]
        backend.error = build_backend_failure()
        cache = WebhookConnectionCache(REGISTRY)
        # Installed before any request, so setdefault keeps it.
        cache._locks["github"] = lock = ObservedLock()  # type: ignore[assignment]

        with ThreadPoolExecutor(2) as executor:
            try:
                first = executor.submit(cache.get, "github")
                assert lookup_started.wait(timeout=30)
                second = executor.submit(cache.get, "github")
                assert lock.contended.wait(timeout=30)
            finally:
                finish_lookup.set()
            resolutions = [first.result(timeout=10), second.result(timeout=10)]

        assert [getattr(resolution, "failure", None) for resolution in resolutions] == [
            ResolutionFailure.UNAVAILABLE,
            ResolutionFailure.UNAVAILABLE,
        ]
        assert backend.lookups == 1
        # Shared only with the requests that waited for it, not cached.
        cache.get("github")
        assert backend.lookups == 2
