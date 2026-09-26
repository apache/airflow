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
from sqlalchemy.ext.asyncio import AsyncSession

from airflow.exceptions import AirflowNotFoundException
from airflow.models.connection import Connection
from airflow.models.variable import Variable
from airflow.sdk import SecretCache
from airflow.sdk.exceptions import AirflowSecretsBackendAccessDenied
from airflow.secrets.async_resolution import resolve_connection, resolve_variable
from airflow.secrets.base_secrets import BaseSecretsBackend
from airflow.secrets.metastore import MetastoreBackend

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


@mock.patch("airflow.secrets.async_resolution.ensure_secrets_loaded")
@pytest.mark.asyncio
async def test_legacy_signatures_and_empty_values(mock_ensure_secrets_loaded):
    backend = LegacyVariableBackend("")
    mock_ensure_secrets_loaded.return_value = [backend]

    assert await resolve_variable("empty") == ""
    assert backend.calls == ["empty"]


@mock.patch("airflow.secrets.async_resolution.ensure_secrets_loaded")
@pytest.mark.asyncio
async def test_native_async_method_is_preferred(mock_ensure_secrets_loaded):
    backend = NativeVariableBackend("native")
    mock_ensure_secrets_loaded.return_value = [backend]

    assert await resolve_variable("key") == "native"
    assert backend.calls == [("key", None)]


@mock.patch("airflow.secrets.async_resolution.ensure_secrets_loaded")
@pytest.mark.asyncio
async def test_first_matching_backend_stops_resolution(mock_ensure_secrets_loaded):
    first = LegacyVariableBackend("first")
    later = LegacyVariableBackend("later")
    mock_ensure_secrets_loaded.return_value = [first, later]

    assert await resolve_variable("key") == "first"
    assert first.calls == ["key"]
    assert later.calls == []


@mock.patch("airflow.secrets.async_resolution.ensure_secrets_loaded")
@pytest.mark.asyncio
async def test_sync_override_wins_over_inherited_async_method(mock_ensure_secrets_loaded):
    backend = SyncMetastoreOverride()
    mock_ensure_secrets_loaded.return_value = [backend]

    assert await resolve_variable("key") == "sync-override"
    assert backend.calls == ["key"]


@mock.patch("airflow.secrets.async_resolution.ensure_secrets_loaded")
@pytest.mark.asyncio
async def test_internal_type_error_is_not_retried_without_team_name(mock_ensure_secrets_loaded):
    failing = mock.Mock()
    failing.get_variable.side_effect = TypeError("backend bug")
    succeeding = NativeVariableBackend("next")
    mock_ensure_secrets_loaded.return_value = [failing, succeeding]

    assert await resolve_variable("key") == "next"
    failing.get_variable.assert_called_once_with(key="key", team_name=None)


@mock.patch("airflow.secrets.async_resolution.ensure_secrets_loaded")
@pytest.mark.asyncio
async def test_broken_native_method_is_not_retried_synchronously(mock_ensure_secrets_loaded):
    class BrokenNativeBackend:
        async def aget_variable(self, key: str) -> str | None:
            raise RuntimeError("native failure")

        def get_variable(self, key: str) -> str | None:
            raise AssertionError("sync fallback must not run")

    succeeding = LegacyVariableBackend("next")
    mock_ensure_secrets_loaded.return_value = [BrokenNativeBackend(), succeeding]

    assert await resolve_variable("key") == "next"
    assert succeeding.calls == ["key"]


@mock.patch("airflow.secrets.async_resolution.ensure_secrets_loaded")
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


@mock.patch("airflow.secrets.async_resolution.ensure_secrets_loaded")
@pytest.mark.asyncio
async def test_inherited_connection_deserialization_is_preserved(mock_ensure_secrets_loaded):
    backend = JsonConnectionBackend()
    backend._set_connection_class(Connection)
    mock_ensure_secrets_loaded.return_value = [backend]

    connection = await resolve_connection("json")

    assert type(connection) is Connection
    assert connection.conn_type == "http"
    assert connection.host == "example.com"


@mock.patch("airflow.secrets.async_resolution.ensure_secrets_loaded")
@pytest.mark.asyncio
async def test_team_name_is_forwarded(mock_ensure_secrets_loaded):
    backend = NativeVariableBackend("team-value")
    mock_ensure_secrets_loaded.return_value = [backend]

    with conf_vars({("core", "multi_team"): "True"}):
        assert await resolve_variable("key", team_name="analytics") == "team-value"
    assert backend.calls == [("key", "analytics")]


@mock.patch("airflow.secrets.async_resolution.ensure_secrets_loaded")
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


@mock.patch("airflow.secrets.async_resolution.ensure_secrets_loaded")
@pytest.mark.asyncio
async def test_connection_miss_is_not_cached(mock_ensure_secrets_loaded):
    enable_secret_cache()
    backend = LegacyConnectionBackend(None)
    mock_ensure_secrets_loaded.return_value = [backend]

    for _ in range(2):
        with pytest.raises(AirflowNotFoundException, match="isn't defined"):
            await resolve_connection("missing")

    assert backend.calls == ["missing", "missing"]


@mock.patch("airflow.secrets.async_resolution.ensure_secrets_loaded")
@pytest.mark.asyncio
async def test_connection_cache_hit_returns_core_connection(mock_ensure_secrets_loaded):
    enable_secret_cache()
    SecretCache.save_connection_uri("cached", "http://user:password@example.com")

    connection = await resolve_connection("cached")

    assert type(connection) is Connection
    assert connection.host == "example.com"
    mock_ensure_secrets_loaded.assert_not_called()


@mock.patch("airflow.secrets.async_resolution.ensure_secrets_loaded")
@pytest.mark.asyncio
async def test_connection_match_is_cached(mock_ensure_secrets_loaded):
    enable_secret_cache()
    backend = LegacyConnectionBackend(Connection(conn_id="cached", uri="http://example.com"))
    mock_ensure_secrets_loaded.return_value = [backend]

    assert (await resolve_connection("cached")).host == "example.com"
    assert (await resolve_connection("cached")).host == "example.com"

    assert backend.calls == ["cached"]


@mock.patch("airflow.secrets.async_resolution.ensure_secrets_loaded")
@pytest.mark.asyncio
async def test_expired_variable_cache_entry_is_refreshed(mock_ensure_secrets_loaded):
    enable_secret_cache()
    backend = mock.Mock()
    backend.get_variable.side_effect = ["first", "second"]
    mock_ensure_secrets_loaded.return_value = [backend]

    assert await resolve_variable("key") == "first"
    SecretCache._ttl = datetime.timedelta(0)
    assert await resolve_variable("key") == "second"


@mock.patch("airflow.secrets.async_resolution.ensure_secrets_loaded")
@pytest.mark.asyncio
async def test_access_denial_stops_chain_and_does_not_cache(mock_ensure_secrets_loaded):
    enable_secret_cache()
    denied = mock.Mock()
    denied.get_variable.side_effect = AirflowSecretsBackendAccessDenied
    later = mock.Mock()
    mock_ensure_secrets_loaded.return_value = [denied, later]

    with pytest.raises(AirflowSecretsBackendAccessDenied):
        await resolve_variable("protected")

    later.get_variable.assert_not_called()
    with pytest.raises(SecretCache.NotPresentException):
        SecretCache.get_variable("protected")


@mock.patch("airflow.secrets.async_resolution.ensure_secrets_loaded")
@pytest.mark.asyncio
async def test_connection_access_denial_stops_chain_and_does_not_cache(mock_ensure_secrets_loaded):
    enable_secret_cache()
    denied = mock.Mock()
    denied.get_connection.side_effect = AirflowSecretsBackendAccessDenied
    later = mock.Mock()
    mock_ensure_secrets_loaded.return_value = [denied, later]

    with pytest.raises(AirflowSecretsBackendAccessDenied):
        await resolve_connection("protected")

    later.get_connection.assert_not_called()
    with pytest.raises(SecretCache.NotPresentException):
        SecretCache.get_connection_uri("protected")


@mock.patch("airflow.secrets.async_resolution.ensure_secrets_loaded")
@pytest.mark.asyncio
async def test_cancellation_stops_chain_and_does_not_cache(mock_ensure_secrets_loaded):
    enable_secret_cache()
    started = asyncio.Event()

    class BlockingBackend:
        async def aget_variable(self, key: str) -> str | None:
            started.set()
            await asyncio.Event().wait()
            return None

        def get_variable(self, key: str) -> str | None:
            raise AssertionError("sync fallback must not run")

    later = mock.Mock()
    mock_ensure_secrets_loaded.return_value = [BlockingBackend(), later]
    task = asyncio.create_task(resolve_variable("cancelled"))
    await started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    later.get_variable.assert_not_called()
    with pytest.raises(SecretCache.NotPresentException):
        SecretCache.get_variable("cancelled")


@mock.patch("airflow.secrets.async_resolution.ensure_secrets_loaded")
@pytest.mark.asyncio
async def test_connection_cancellation_stops_chain_and_does_not_cache(mock_ensure_secrets_loaded):
    enable_secret_cache()
    started = asyncio.Event()

    class BlockingBackend:
        async def aget_connection(self, conn_id: str) -> Connection | None:
            started.set()
            await asyncio.Event().wait()
            return None

        def get_connection(self, conn_id: str) -> Connection | None:
            raise AssertionError("sync fallback must not run")

    later = mock.Mock()
    mock_ensure_secrets_loaded.return_value = [BlockingBackend(), later]
    task = asyncio.create_task(resolve_connection("cancelled"))
    await started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    later.get_connection.assert_not_called()
    with pytest.raises(SecretCache.NotPresentException):
        SecretCache.get_connection_uri("cancelled")


@mock.patch("airflow.secrets.async_resolution.ensure_secrets_loaded")
@pytest.mark.asyncio
async def test_broken_native_connection_falls_through_without_sync_retry(mock_ensure_secrets_loaded):
    class BrokenNativeBackend:
        async def aget_connection(self, conn_id: str) -> Connection | None:
            raise RuntimeError("native failure")

        def get_connection(self, conn_id: str) -> Connection | None:
            raise AssertionError("sync fallback must not run")

    expected = Connection(conn_id="next", uri="http://example.com")
    succeeding = LegacyConnectionBackend(expected)
    mock_ensure_secrets_loaded.return_value = [BrokenNativeBackend(), succeeding]

    assert await resolve_connection("next") is expected
    assert succeeding.calls == ["next"]


@mock.patch("airflow.secrets.async_resolution.ensure_secrets_loaded")
@pytest.mark.parametrize("resolver", [resolve_variable, resolve_connection])
@pytest.mark.asyncio
async def test_scoped_lookup_requires_multi_team_mode(mock_ensure_secrets_loaded, resolver):
    with conf_vars({("core", "multi_team"): "False"}), pytest.raises(ValueError, match="Multi-team mode"):
        await resolver("key", team_name="analytics")

    mock_ensure_secrets_loaded.assert_not_called()


def run_until_loop_progresses(
    started: threading.Event,
    progressed: threading.Event,
    release: threading.Event,
    result: list[bool],
) -> None:
    started.wait(timeout=1)
    result.append(progressed.wait(timeout=0.5))
    release.set()


@mock.patch("airflow.secrets.async_resolution.ensure_secrets_loaded")
@pytest.mark.asyncio
async def test_legacy_backend_yields_event_loop(mock_ensure_secrets_loaded):
    loop = asyncio.get_running_loop()
    started = threading.Event()
    progressed = threading.Event()
    release = threading.Event()
    result: list[bool] = []
    async_started = asyncio.Event()

    class BlockingBackend:
        def get_variable(self, key: str) -> str | None:
            started.set()
            loop.call_soon_threadsafe(async_started.set)
            release.wait(timeout=1)
            return "value"

    mock_ensure_secrets_loaded.return_value = [BlockingBackend()]
    helper = threading.Thread(
        target=run_until_loop_progresses,
        args=(started, progressed, release, result),
    )
    helper.start()
    task = asyncio.create_task(resolve_variable("key"))
    await asyncio.wait_for(async_started.wait(), timeout=1)
    progressed.set()

    assert await task == "value"
    helper.join(timeout=1)
    assert result == [True]


@mock.patch("airflow.secrets.async_resolution.ensure_secrets_loaded")
@pytest.mark.asyncio
async def test_legacy_connection_backend_yields_event_loop(mock_ensure_secrets_loaded):
    loop = asyncio.get_running_loop()
    started = threading.Event()
    progressed = threading.Event()
    release = threading.Event()
    result: list[bool] = []
    async_started = asyncio.Event()

    class BlockingBackend:
        def get_connection(self, conn_id: str) -> Connection | None:
            started.set()
            loop.call_soon_threadsafe(async_started.set)
            release.wait(timeout=1)
            return Connection(conn_id=conn_id, uri="http://example.com")

    mock_ensure_secrets_loaded.return_value = [BlockingBackend()]
    helper = threading.Thread(
        target=run_until_loop_progresses,
        args=(started, progressed, release, result),
    )
    helper.start()
    task = asyncio.create_task(resolve_connection("key"))
    await asyncio.wait_for(async_started.wait(), timeout=1)
    progressed.set()

    assert (await task).host == "example.com"
    helper.join(timeout=1)
    assert result == [True]


@mock.patch("airflow.sdk.SecretCache.get_variable")
@pytest.mark.asyncio
async def test_initialized_cache_yields_event_loop(mock_get_variable):
    loop = asyncio.get_running_loop()
    started = threading.Event()
    progressed = threading.Event()
    release = threading.Event()
    result: list[bool] = []
    async_started = asyncio.Event()

    def blocking_cache_get(key: str, team_name: str | None = None) -> str:
        started.set()
        loop.call_soon_threadsafe(async_started.set)
        release.wait(timeout=1)
        return "cached"

    mock_get_variable.side_effect = blocking_cache_get
    helper = threading.Thread(
        target=run_until_loop_progresses,
        args=(started, progressed, release, result),
    )
    helper.start()
    task = asyncio.create_task(resolve_variable("key"))
    await asyncio.wait_for(async_started.wait(), timeout=1)
    progressed.set()

    assert await task == "cached"
    helper.join(timeout=1)
    assert result == [True]


@mock.patch("airflow.sdk.SecretCache.get_connection_uri")
@pytest.mark.asyncio
async def test_initialized_connection_cache_yields_event_loop(mock_get_connection_uri):
    loop = asyncio.get_running_loop()
    started = threading.Event()
    progressed = threading.Event()
    release = threading.Event()
    result: list[bool] = []
    async_started = asyncio.Event()

    def blocking_cache_get(conn_id: str, team_name: str | None = None) -> str:
        started.set()
        loop.call_soon_threadsafe(async_started.set)
        release.wait(timeout=1)
        return "http://example.com"

    mock_get_connection_uri.side_effect = blocking_cache_get
    helper = threading.Thread(
        target=run_until_loop_progresses,
        args=(started, progressed, release, result),
    )
    helper.start()
    task = asyncio.create_task(resolve_connection("key"))
    await asyncio.wait_for(async_started.wait(), timeout=1)
    progressed.set()

    assert (await task).host == "example.com"
    helper.join(timeout=1)
    assert result == [True]


@mock.patch("airflow.secrets.async_resolution.ensure_secrets_loaded")
@pytest.mark.asyncio
async def test_backend_initialization_yields_event_loop(mock_ensure_secrets_loaded):
    loop = asyncio.get_running_loop()
    started = threading.Event()
    progressed = threading.Event()
    release = threading.Event()
    result: list[bool] = []
    async_started = asyncio.Event()

    def load_backends():
        started.set()
        loop.call_soon_threadsafe(async_started.set)
        release.wait(timeout=1)
        return [LegacyVariableBackend("value")]

    mock_ensure_secrets_loaded.side_effect = load_backends
    helper = threading.Thread(
        target=run_until_loop_progresses,
        args=(started, progressed, release, result),
    )
    helper.start()
    task = asyncio.create_task(resolve_variable("key"))
    await asyncio.wait_for(async_started.wait(), timeout=1)
    progressed.set()

    assert await task == "value"
    helper.join(timeout=1)
    assert result == [True]


@mock.patch("airflow.secrets.async_resolution.ensure_secrets_loaded")
@pytest.mark.asyncio
async def test_connection_backend_initialization_yields_event_loop(mock_ensure_secrets_loaded):
    loop = asyncio.get_running_loop()
    started = threading.Event()
    progressed = threading.Event()
    release = threading.Event()
    result: list[bool] = []
    async_started = asyncio.Event()

    def load_backends():
        started.set()
        loop.call_soon_threadsafe(async_started.set)
        release.wait(timeout=1)
        return [LegacyConnectionBackend(Connection(conn_id="key", uri="http://example.com"))]

    mock_ensure_secrets_loaded.side_effect = load_backends
    helper = threading.Thread(
        target=run_until_loop_progresses,
        args=(started, progressed, release, result),
    )
    helper.start()
    task = asyncio.create_task(resolve_connection("key"))
    await asyncio.wait_for(async_started.wait(), timeout=1)
    progressed.set()

    assert (await task).host == "example.com"
    helper.join(timeout=1)
    assert result == [True]


@mock.patch("airflow.secrets.async_resolution.ensure_secrets_loaded")
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


@pytest.mark.db_test
@pytest.mark.asyncio
async def test_native_metastore_reads_use_async_session(session, testing_team):
    clear_db_connections()
    clear_db_variables()
    session.add_all(
        [
            Connection(conn_id="native", uri="http://user:password@example.com"),
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

    from airflow import settings

    # Rebuild the engine on this test's loop, and read the rebound globals through the
    # module: a name imported from ``airflow.settings`` before the call stays bound to the
    # previous engine. Dispose before the loop closes so pooled connections do not leak
    # into the next test's loop.
    settings._configure_async_session()
    try:
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
        await settings.async_engine.dispose()

    assert type(connection) is Connection
    assert connection.host == "example.com"
    assert type(team_connection) is Connection
    assert team_connection.host == "team.example.com"
    clear_db_connections()
    clear_db_variables()


@pytest.mark.db_test
@mock.patch("airflow.secrets.async_resolution.ensure_secrets_loaded")
@pytest.mark.parametrize("operation", ["set", "delete"])
@pytest.mark.asyncio
async def test_sync_write_and_delete_invalidate_async_populated_cache(
    mock_ensure_secrets_loaded, operation, session
):
    enable_secret_cache()
    mock_ensure_secrets_loaded.return_value = [LegacyVariableBackend("cached-value")]
    assert await resolve_variable("cached-key") == "cached-value"

    if operation == "set":
        with mock.patch.object(Variable, "check_for_write_conflict"):
            Variable.set("cached-key", "new-value", session=session)
    else:
        Variable.delete("cached-key", session=session)

    with pytest.raises(SecretCache.NotPresentException):
        SecretCache.get_variable("cached-key")


@mock.patch("airflow.secrets.metastore.create_session_async")
@pytest.mark.parametrize("method_name", ["aget_variable", "aget_connection"])
@pytest.mark.parametrize("failure", [None, RuntimeError("database failure")], ids=["success", "error"])
@pytest.mark.asyncio
async def test_owned_metastore_session_closes_on_success_and_error(
    mock_create_session_async, failure, method_name
):
    exited = False
    fake_session = mock.AsyncMock(spec=AsyncSession)
    if failure is None:
        fake_session.scalar.return_value = None
    else:
        fake_session.scalar.side_effect = failure

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


@mock.patch("airflow.secrets.metastore.create_session_async")
@pytest.mark.parametrize("method_name", ["aget_variable", "aget_connection"])
@pytest.mark.asyncio
async def test_borrowed_metastore_session_is_not_owned(mock_create_session_async, method_name):
    fake_session = mock.AsyncMock(spec=AsyncSession)
    fake_session.scalar.return_value = None

    method = getattr(MetastoreBackend(), method_name)
    assert await method("key", session=fake_session) is None

    mock_create_session_async.assert_not_called()
    fake_session.commit.assert_not_awaited()
    fake_session.close.assert_not_awaited()


@mock.patch("airflow.secrets.metastore.create_session_async")
@pytest.mark.parametrize("method_name", ["aget_variable", "aget_connection"])
@pytest.mark.asyncio
async def test_owned_metastore_session_closes_on_cancellation(mock_create_session_async, method_name):
    entered = asyncio.Event()
    exited = asyncio.Event()
    scalar_started = asyncio.Event()
    fake_session = mock.AsyncMock(spec=AsyncSession)

    async def scalar(statement):
        scalar_started.set()
        await asyncio.Event().wait()

    fake_session.scalar.side_effect = scalar

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
    await scalar_started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert exited.is_set()
