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

import asyncio
import contextvars
import datetime
import threading
from contextlib import asynccontextmanager
from unittest import mock

import pytest
import pytest_asyncio
from sqlalchemy import event
from sqlalchemy.engine import MappingResult, Result
from sqlalchemy.ext.asyncio import AsyncSession

from airflow import settings
from airflow.exceptions import AirflowConfigException, AirflowNotFoundException
from airflow.models import crypto
from airflow.models.connection import Connection
from airflow.models.variable import Variable
from airflow.sdk import SecretCache
from airflow.sdk.exceptions import AirflowSecretsBackendAccessDenied
from airflow.secrets.base_secrets import BaseSecretsBackend
from airflow.secrets.metastore import MetastoreBackend
from airflow.secrets.resolver import resolve_connection, resolve_variable

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_connections, clear_db_variables


@pytest.fixture(autouse=True)
def reset_secret_cache():
    SecretCache.reset()
    yield
    SecretCache.reset()


def enable_secret_cache() -> None:
    SecretCache._cache = {}
    SecretCache._ttl = datetime.timedelta(minutes=15)


class LegacyVariableBackend:
    def __init__(self, value: str | None):
        self.value = value
        self.calls: list[str] = []

    def get_variable(self, key: str) -> str | None:  # type: ignore[override]
        self.calls.append(key)
        return self.value


class LegacyConnectionBackend:
    def __init__(self, connection: Connection | None):
        self.connection = connection
        self.calls: list[str] = []

    def get_connection(self, conn_id: str) -> Connection | None:
        self.calls.append(conn_id)
        return self.connection


class NativeVariableBackend:
    def __init__(self, value: str | None):
        self.value = value
        self.calls: list[tuple[str, str | None]] = []

    async def aget_variable(self, key: str, team_name: str | None = None) -> str | None:
        self.calls.append((key, team_name))
        return self.value

    def get_variable(self, key: str, team_name: str | None = None) -> str | None:
        raise AssertionError("sync fallback must not run")


class SyncMetastoreOverride(MetastoreBackend):
    def __init__(self):
        self.calls: list[str] = []

    def get_variable(self, key: str) -> str | None:  # type: ignore[override]
        self.calls.append(key)
        return "sync-override"


class NativeOnlyBackend:
    """Native methods are given by each test; the synchronous methods must never run."""

    def get_variable(self, key: str) -> str | None:
        raise AssertionError("sync fallback must not run")

    def get_connection(self, conn_id: str) -> Connection | None:
        raise AssertionError("sync fallback must not run")


RESOURCES = [
    pytest.param(resolve_variable, "get_variable", SecretCache.get_variable, id="variable"),
    pytest.param(resolve_connection, "get_connection", SecretCache.get_connection_uri, id="connection"),
]


@mock.patch("airflow.secrets.resolver.ensure_secrets_loaded", autospec=True)
@pytest.mark.asyncio
async def test_legacy_signatures_and_empty_values(mock_ensure_secrets_loaded):
    backend = LegacyVariableBackend("")
    mock_ensure_secrets_loaded.return_value = [backend]

    assert await resolve_variable("empty") == ""
    assert backend.calls == ["empty"]


@mock.patch("airflow.secrets.resolver.mask_secret", autospec=True)
@mock.patch("airflow.secrets.resolver.ensure_secrets_loaded", autospec=True)
@pytest.mark.parametrize("cached", [False, True], ids=["backend", "cache-hit"])
@pytest.mark.asyncio
async def test_resolved_variable_is_masked(mock_ensure_secrets_loaded, mock_mask_secret, cached):
    mock_ensure_secrets_loaded.return_value = [LegacyVariableBackend("secret-value")]
    if cached:
        enable_secret_cache()
        SecretCache.save_variable("key", "secret-value")

    assert await resolve_variable("key") == "secret-value"
    mock_mask_secret.assert_called_once_with("secret-value", "key")


@mock.patch("airflow.secrets.resolver.ensure_secrets_loaded", autospec=True)
@pytest.mark.asyncio
async def test_native_async_method_is_preferred(mock_ensure_secrets_loaded):
    backend = NativeVariableBackend("native")
    mock_ensure_secrets_loaded.return_value = [backend]

    assert await resolve_variable("key") == "native"
    assert backend.calls == [("key", None)]


@mock.patch("airflow.secrets.resolver.ensure_secrets_loaded", autospec=True)
@pytest.mark.asyncio
async def test_first_matching_backend_stops_resolution(mock_ensure_secrets_loaded):
    first = LegacyVariableBackend("first")
    later = LegacyVariableBackend("later")
    mock_ensure_secrets_loaded.return_value = [first, later]

    assert await resolve_variable("key") == "first"
    assert first.calls == ["key"]
    assert later.calls == []


@mock.patch("airflow.secrets.resolver.ensure_secrets_loaded", autospec=True)
@pytest.mark.asyncio
async def test_sync_override_wins_over_inherited_async_method(mock_ensure_secrets_loaded):
    backend = SyncMetastoreOverride()
    mock_ensure_secrets_loaded.return_value = [backend]

    assert await resolve_variable("key") == "sync-override"
    assert backend.calls == ["key"]


@mock.patch("airflow.secrets.resolver.ensure_secrets_loaded", autospec=True)
@pytest.mark.asyncio
async def test_internal_type_error_is_not_retried_without_team_name(mock_ensure_secrets_loaded):
    failing = mock.create_autospec(BaseSecretsBackend, instance=True)
    failing.get_variable.side_effect = TypeError("backend bug")
    succeeding = NativeVariableBackend("next")
    mock_ensure_secrets_loaded.return_value = [failing, succeeding]

    assert await resolve_variable("key") == "next"
    failing.get_variable.assert_called_once_with(key="key", team_name=None)


@mock.patch("airflow.secrets.resolver.ensure_secrets_loaded", autospec=True)
@pytest.mark.parametrize("resource", ["variable", "connection"])
@pytest.mark.asyncio
async def test_broken_native_method_falls_through_without_sync_retry(mock_ensure_secrets_loaded, resource):
    class BrokenNativeBackend(NativeOnlyBackend):
        async def aget_variable(self, key: str) -> str | None:
            raise RuntimeError("native failure")

        async def aget_connection(self, conn_id: str) -> Connection | None:
            raise RuntimeError("native failure")

    expected = Connection(conn_id="next", uri="http://example.com")
    succeeding = (
        LegacyVariableBackend("next") if resource == "variable" else LegacyConnectionBackend(expected)
    )
    mock_ensure_secrets_loaded.return_value = [BrokenNativeBackend(), succeeding]

    if resource == "variable":
        assert await resolve_variable("next") == "next"
    else:
        assert await resolve_connection("next") is expected
    assert succeeding.calls == ["next"]


@mock.patch("airflow.secrets.resolver.ensure_secrets_loaded", autospec=True)
@pytest.mark.asyncio
async def test_full_connection_override_is_preserved(mock_ensure_secrets_loaded):
    expected = Connection(conn_id="custom", uri="postgresql://user:password@host/db")
    backend = LegacyConnectionBackend(expected)
    mock_ensure_secrets_loaded.return_value = [backend]

    assert await resolve_connection("custom") is expected
    assert backend.calls == ["custom"]


class JsonConnectionBackend(BaseSecretsBackend):
    def get_conn_value(self, conn_id: str) -> str | None:  # type: ignore[override]
        return '{"conn_type": "http", "host": "example.com", "password": "secret"}'


@mock.patch("airflow.secrets.resolver.ensure_secrets_loaded", autospec=True)
@pytest.mark.asyncio
async def test_inherited_connection_deserialization_is_preserved(mock_ensure_secrets_loaded):
    backend = JsonConnectionBackend()
    backend._set_connection_class(Connection)
    mock_ensure_secrets_loaded.return_value = [backend]

    connection = await resolve_connection("json")

    assert type(connection) is Connection
    assert connection.conn_type == "http"
    assert connection.host == "example.com"


@mock.patch("airflow.secrets.resolver.ensure_secrets_loaded", autospec=True)
@pytest.mark.asyncio
async def test_team_name_is_forwarded(mock_ensure_secrets_loaded):
    backend = NativeVariableBackend("team-value")
    mock_ensure_secrets_loaded.return_value = [backend]

    with conf_vars({("core", "multi_team"): "True"}):
        assert await resolve_variable("key", team_name="analytics") == "team-value"
    assert backend.calls == [("key", "analytics")]


@mock.patch("airflow.secrets.resolver.ensure_secrets_loaded", autospec=True)
@pytest.mark.asyncio
async def test_variable_miss_is_cached(mock_ensure_secrets_loaded):
    enable_secret_cache()
    backend = LegacyVariableBackend(None)
    mock_ensure_secrets_loaded.return_value = [backend]

    with pytest.raises(KeyError):
        await resolve_variable("missing")
    with pytest.raises(KeyError):
        await resolve_variable("missing")

    assert backend.calls == ["missing"]


@mock.patch("airflow.secrets.resolver.ensure_secrets_loaded", autospec=True)
@pytest.mark.asyncio
async def test_connection_miss_is_not_cached(mock_ensure_secrets_loaded):
    enable_secret_cache()
    backend = LegacyConnectionBackend(None)
    mock_ensure_secrets_loaded.return_value = [backend]

    for _ in range(2):
        with pytest.raises(AirflowNotFoundException, match="isn't defined"):
            await resolve_connection("missing")

    assert backend.calls == ["missing", "missing"]


@mock.patch("airflow.secrets.resolver.ensure_secrets_loaded", autospec=True)
@pytest.mark.asyncio
async def test_connection_cache_hit_returns_core_connection(mock_ensure_secrets_loaded):
    enable_secret_cache()
    SecretCache.save_connection_uri("cached", "http://user:password@example.com")

    connection = await resolve_connection("cached")

    assert type(connection) is Connection
    assert connection.host == "example.com"
    mock_ensure_secrets_loaded.assert_not_called()


@mock.patch("airflow.secrets.resolver.ensure_secrets_loaded", autospec=True)
@pytest.mark.asyncio
async def test_connection_match_is_cached(mock_ensure_secrets_loaded):
    enable_secret_cache()
    backend = LegacyConnectionBackend(Connection(conn_id="cached", uri="http://example.com"))
    mock_ensure_secrets_loaded.return_value = [backend]

    assert (await resolve_connection("cached")).host == "example.com"
    assert (await resolve_connection("cached")).host == "example.com"

    assert backend.calls == ["cached"]


@mock.patch("airflow.secrets.resolver.ensure_secrets_loaded", autospec=True)
@pytest.mark.asyncio
async def test_expired_variable_cache_entry_is_refreshed(mock_ensure_secrets_loaded):
    enable_secret_cache()
    backend = mock.create_autospec(BaseSecretsBackend, instance=True)
    backend.get_variable.side_effect = ["first", "second"]
    mock_ensure_secrets_loaded.return_value = [backend]

    assert await resolve_variable("key") == "first"
    SecretCache._ttl = datetime.timedelta(0)
    assert await resolve_variable("key") == "second"


@mock.patch("airflow.secrets.resolver.ensure_secrets_loaded", autospec=True)
@pytest.mark.parametrize(("resolver", "method_name", "read_cache"), RESOURCES)
@pytest.mark.asyncio
async def test_access_denial_stops_chain_and_does_not_cache(
    mock_ensure_secrets_loaded, resolver, method_name, read_cache
):
    enable_secret_cache()
    denied = mock.create_autospec(BaseSecretsBackend, instance=True)
    getattr(denied, method_name).side_effect = AirflowSecretsBackendAccessDenied
    later = mock.create_autospec(BaseSecretsBackend, instance=True)
    mock_ensure_secrets_loaded.return_value = [denied, later]

    with pytest.raises(AirflowSecretsBackendAccessDenied):
        await resolver("protected")

    getattr(later, method_name).assert_not_called()
    with pytest.raises(SecretCache.NotPresentException):
        read_cache("protected")


@mock.patch("airflow.secrets.resolver.ensure_secrets_loaded", autospec=True)
@pytest.mark.parametrize(("resolver", "method_name", "read_cache"), RESOURCES)
@pytest.mark.asyncio
async def test_cancellation_stops_chain_and_does_not_cache(
    mock_ensure_secrets_loaded, resolver, method_name, read_cache
):
    enable_secret_cache()
    started = asyncio.Event()

    class BlockingBackend(NativeOnlyBackend):
        async def aget_variable(self, key: str) -> str | None:
            started.set()
            await asyncio.Event().wait()
            return None

        async def aget_connection(self, conn_id: str) -> Connection | None:
            started.set()
            await asyncio.Event().wait()
            return None

    later = mock.create_autospec(BaseSecretsBackend, instance=True)
    mock_ensure_secrets_loaded.return_value = [BlockingBackend(), later]
    task = asyncio.create_task(resolver("cancelled"))
    await started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    getattr(later, method_name).assert_not_called()
    with pytest.raises(SecretCache.NotPresentException):
        read_cache("cancelled")


@mock.patch("airflow.secrets.resolver.ensure_secrets_loaded", autospec=True)
@pytest.mark.parametrize("resolver", [resolve_variable, resolve_connection])
@pytest.mark.asyncio
async def test_scoped_lookup_requires_multi_team_mode(mock_ensure_secrets_loaded, resolver):
    with conf_vars({("core", "multi_team"): "False"}), pytest.raises(ValueError, match="Multi-team mode"):
        await resolver("key", team_name="analytics")

    mock_ensure_secrets_loaded.assert_not_called()


@mock.patch("airflow.secrets.resolver.ensure_secrets_loaded", autospec=True)
@pytest.mark.parametrize("cache_enabled", [False, True], ids=["cache-disabled", "cache-enabled"])
@pytest.mark.parametrize(("resolver", "method_name", "read_cache"), RESOURCES)
@pytest.mark.asyncio
async def test_secret_cache_leaves_the_event_loop_only_when_initialized(
    mock_ensure_secrets_loaded, resolver, method_name, read_cache, cache_enabled, monkeypatch
):
    if cache_enabled:
        enable_secret_cache()
    loop_thread = threading.get_ident()
    cache_threads: list[int] = []

    def record_cache_read(*args, **kwargs):
        cache_threads.append(threading.get_ident())
        return read_cache(*args, **kwargs)

    monkeypatch.setattr(SecretCache, read_cache.__name__, record_cache_read)
    mock_ensure_secrets_loaded.return_value = [
        LegacyVariableBackend("value")
        if method_name == "get_variable"
        else LegacyConnectionBackend(Connection(conn_id="key", uri="http://example.com"))
    ]

    await resolver("key")

    assert len(cache_threads) == 1
    assert (cache_threads[0] != loop_thread) is cache_enabled


@mock.patch("airflow.secrets.resolver.ensure_secrets_loaded", autospec=True)
@pytest.mark.asyncio
async def test_cold_fernet_key_is_loaded_off_the_event_loop_once(mock_ensure_secrets_loaded):
    connection = Connection(conn_id="key", uri="http://user:password@example.com")
    mock_ensure_secrets_loaded.return_value = [LegacyConnectionBackend(connection)]
    loop_thread = threading.get_ident()
    key_reads: list[int] = []
    read_option = crypto.conf.get

    def record_key_read(section, key, *args, **kwargs):
        if (section, key) == ("core", "FERNET_KEY"):
            key_reads.append(threading.get_ident())
        return read_option(section, key, *args, **kwargs)

    crypto.get_fernet.cache_clear()
    try:
        with mock.patch.object(crypto.conf, "get", autospec=True, side_effect=record_key_read):
            for _ in range(2):
                assert (await resolve_connection("key")).password == "password"
    finally:
        crypto.get_fernet.cache_clear()

    assert len(key_reads) == 1
    assert key_reads[0] != loop_thread


@mock.patch("airflow.secrets.resolver.ensure_secrets_loaded", autospec=True)
@pytest.mark.asyncio
async def test_failed_fernet_key_load_does_not_stop_resolution(mock_ensure_secrets_loaded):
    mock_ensure_secrets_loaded.return_value = [LegacyVariableBackend("value")]
    read_option = crypto.conf.get

    def fail_key_read(section, key, *args, **kwargs):
        if (section, key) == ("core", "FERNET_KEY"):
            raise AirflowConfigException("fernet_key_cmd failed")
        return read_option(section, key, *args, **kwargs)

    crypto.get_fernet.cache_clear()
    try:
        with mock.patch.object(crypto.conf, "get", autospec=True, side_effect=fail_key_read):
            assert await resolve_variable("key") == "value"
    finally:
        crypto.get_fernet.cache_clear()


def run_until_loop_progresses(
    started: threading.Event,
    progressed: threading.Event,
    release: threading.Event,
    result: list[bool],
) -> None:
    started.wait(timeout=1)
    result.append(progressed.wait(timeout=0.5))
    release.set()


@mock.patch("airflow.secrets.resolver.ensure_secrets_loaded", autospec=True)
@pytest.mark.parametrize("resource", ["variable", "connection"])
@pytest.mark.parametrize("boundary", ["backend", "initialization"])
@pytest.mark.asyncio
async def test_blocking_resolution_yields_event_loop(
    mock_ensure_secrets_loaded, resource, boundary, monkeypatch
):
    loop = asyncio.get_running_loop()
    started = threading.Event()
    progressed = threading.Event()
    release = threading.Event()
    observed: list[bool] = []
    async_started = asyncio.Event()
    connection = Connection(conn_id="key", uri="http://example.com")
    backend = (
        LegacyVariableBackend("value") if resource == "variable" else LegacyConnectionBackend(connection)
    )
    resolver = resolve_variable if resource == "variable" else resolve_connection
    mock_ensure_secrets_loaded.return_value = [backend]

    def blocking_call(*args, **kwargs):
        started.set()
        loop.call_soon_threadsafe(async_started.set)
        release.wait(timeout=1)
        if boundary == "initialization":
            return [backend]
        return "value" if resource == "variable" else connection

    if boundary == "initialization":
        mock_ensure_secrets_loaded.side_effect = blocking_call
    else:
        method = f"get_{resource}"
        monkeypatch.setattr(
            backend, method, mock.create_autospec(getattr(backend, method), side_effect=blocking_call)
        )

    helper = threading.Thread(target=run_until_loop_progresses, args=(started, progressed, release, observed))
    helper.start()
    task = asyncio.create_task(resolver("key"))
    try:
        await asyncio.wait_for(async_started.wait(), timeout=1)
        progressed.set()
        result = await task
        if resource == "variable":
            assert result == "value"
        else:
            assert isinstance(result, Connection)
            assert result.host == "example.com"
        assert observed == [True]
    finally:
        release.set()
        helper.join(timeout=1)


@mock.patch("airflow.secrets.resolver.ensure_secrets_loaded", autospec=True)
@pytest.mark.asyncio
async def test_thread_fallback_preserves_contextvars(mock_ensure_secrets_loaded):
    request_context = contextvars.ContextVar("request_context")
    request_context.set("request-value")
    observed: list[str | None] = []

    class ContextBackend:
        def get_variable(self, key: str) -> str | None:
            observed.append(request_context.get(None))
            return "value"

    mock_ensure_secrets_loaded.return_value = [ContextBackend()]

    assert await resolve_variable("key") == "value"
    assert observed == ["request-value"]


@pytest_asyncio.fixture
async def fresh_async_engine():
    """Give this test's event loop its own async engine, then restore the previous globals."""
    previous_engine, previous_session_factory = settings.async_engine, settings.AsyncSession
    engine = None
    try:
        settings._configure_async_session()
        engine = settings.async_engine
        yield engine
    finally:
        try:
            engine = engine if engine is not None else settings.async_engine
            if engine is not None and engine is not previous_engine:
                await engine.dispose()
        finally:
            settings.async_engine, settings.AsyncSession = previous_engine, previous_session_factory


@pytest.mark.db_test
@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["configure", "dispose"])
@mock.patch("sqlalchemy.ext.asyncio.AsyncEngine.dispose", autospec=True)
async def test_fresh_async_engine_restores_globals_after_failure(mock_dispose, failure):
    previous_engine, previous_factory = settings.async_engine, settings.AsyncSession
    original_configure = settings._configure_async_session

    def configure():
        original_configure()
        if failure == "configure":
            raise RuntimeError("configure failed")

    with mock.patch.object(settings, "_configure_async_session", autospec=True, side_effect=configure):
        generator = fresh_async_engine.__wrapped__()
        try:
            if failure == "configure":
                with pytest.raises(RuntimeError, match="configure failed"):
                    await generator.__anext__()
            else:
                await generator.__anext__()
                mock_dispose.side_effect = RuntimeError("dispose failed")
                with pytest.raises(RuntimeError, match="dispose failed"):
                    await generator.aclose()
            mock_dispose.assert_awaited_once()
            assert settings.async_engine is previous_engine
            assert settings.AsyncSession is previous_factory
        finally:
            await generator.aclose()
            settings.async_engine, settings.AsyncSession = previous_engine, previous_factory


@pytest.mark.db_test
@pytest.mark.asyncio
async def test_native_metastore_reads_use_async_session_without_hydrating_entities(
    session, testing_team, fresh_async_engine
):
    clear_db_connections()
    clear_db_variables()
    session.add_all(
        [
            Connection(conn_id="native", uri="http://user:password@example.com", description="description"),
            Connection(
                conn_id="team-native",
                uri="http://team-user:password@team.example.com",
                team_name=testing_team.name,
            ),
            Variable(key="native", val="value"),
            Variable(key="team-native", val="team-value", team_name=testing_team.name),
        ]
    )
    session.commit()
    loaded = []

    def record_load(target, context):
        loaded.append(target)

    event.listen(Variable, "load", record_load)
    event.listen(Connection, "load", record_load)
    try:
        # Read the session factory through the module: ``fresh_async_engine`` rebinds it.
        async with settings.AsyncSession() as async_session:
            backend = MetastoreBackend()
            assert await backend.aget_variable("native", session=async_session) == "value"
            assert (
                await backend.aget_variable("team-native", team_name=testing_team.name, session=async_session)
                == "team-value"
            )
            connection = await backend.aget_connection("native", session=async_session)
            team_connection = await backend.aget_connection(
                "team-native", team_name=testing_team.name, session=async_session
            )
    finally:
        event.remove(Variable, "load", record_load)
        event.remove(Connection, "load", record_load)

    assert loaded == []
    assert type(connection) is Connection
    assert connection.host == "example.com"
    assert connection.password == "password"
    assert connection.description == "description"
    assert type(team_connection) is Connection
    assert team_connection.host == "team.example.com"
    assert team_connection.password == "password"
    assert team_connection.team_name == testing_team.name
    clear_db_connections()
    clear_db_variables()


@pytest.mark.db_test
@mock.patch("airflow.secrets.resolver.ensure_secrets_loaded", autospec=True)
@pytest.mark.parametrize("operation", ["set", "delete"])
@pytest.mark.asyncio
async def test_sync_write_and_delete_invalidate_async_populated_cache(
    mock_ensure_secrets_loaded, operation, session
):
    enable_secret_cache()
    mock_ensure_secrets_loaded.return_value = [LegacyVariableBackend("cached-value")]
    assert await resolve_variable("cached-key") == "cached-value"

    if operation == "set":
        with mock.patch.object(Variable, "check_for_write_conflict", autospec=True):
            Variable.set("cached-key", "new-value", session=session)
    else:
        Variable.delete("cached-key", session=session)

    with pytest.raises(SecretCache.NotPresentException):
        SecretCache.get_variable("cached-key")


@pytest.fixture
def empty_async_session():
    session = mock.AsyncMock(spec=AsyncSession)
    result = mock.Mock(spec=Result)
    result.first.return_value = None
    mappings = mock.Mock(spec=MappingResult)
    mappings.first.return_value = None
    result.mappings.return_value = mappings
    session.execute.return_value = result
    return session


@mock.patch("airflow.secrets.metastore.create_session_async", autospec=True)
@pytest.mark.parametrize("method_name", ["aget_variable", "aget_connection"])
@pytest.mark.parametrize("failure", [None, RuntimeError("database failure")], ids=["success", "error"])
@pytest.mark.asyncio
async def test_owned_metastore_session_closes_on_success_and_error(
    mock_create_session_async, failure, method_name, empty_async_session
):
    exited = False
    fake_session = empty_async_session
    fake_session.execute.side_effect = failure

    @asynccontextmanager
    async def session_context():
        nonlocal exited
        try:
            yield fake_session
        finally:
            exited = True

    mock_create_session_async.return_value = session_context()

    method = getattr(MetastoreBackend(), method_name)
    if failure is None:
        assert await method("key") is None
    else:
        with pytest.raises(RuntimeError, match="database failure"):
            await method("key")

    assert exited


@mock.patch("airflow.secrets.metastore.create_session_async", autospec=True)
@pytest.mark.parametrize("method_name", ["aget_variable", "aget_connection"])
@pytest.mark.asyncio
async def test_borrowed_metastore_session_is_not_owned(
    mock_create_session_async, method_name, empty_async_session
):
    fake_session = empty_async_session

    method = getattr(MetastoreBackend(), method_name)
    assert await method("key", session=fake_session) is None

    mock_create_session_async.assert_not_called()
    fake_session.commit.assert_not_awaited()
    fake_session.close.assert_not_awaited()


@mock.patch("airflow.secrets.metastore.create_session_async", autospec=True)
@pytest.mark.parametrize("method_name", ["aget_variable", "aget_connection"])
@pytest.mark.asyncio
async def test_owned_metastore_session_closes_on_cancellation(mock_create_session_async, method_name):
    entered = asyncio.Event()
    exited = asyncio.Event()
    query_started = asyncio.Event()
    fake_session = mock.AsyncMock(spec=AsyncSession)

    async def execute(statement):
        query_started.set()
        await asyncio.Event().wait()

    fake_session.execute.side_effect = execute

    @asynccontextmanager
    async def session_context():
        entered.set()
        try:
            yield fake_session
        finally:
            exited.set()

    mock_create_session_async.return_value = session_context()
    method = getattr(MetastoreBackend(), method_name)
    task = asyncio.create_task(method("key"))
    await entered.wait()
    await query_started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert exited.is_set()
