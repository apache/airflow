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

import json
import logging
import threading
from unittest import mock

import pytest

from airflow.models.connection import Connection
from airflow.providers.common.compat.connection import get_async_connection, get_async_extra_dejson
from airflow.providers.common.compat.sdk import BaseHook


class MockAgetBaseHook:
    def __init__(*args, **kargs):
        pass

    async def aget_connection(self, conn_id: str):
        return Connection(
            conn_id="test_conn",
            conn_type="http",
            password="secret_token_aget",
        )


class MockBaseHook:
    def __init__(*args, **kargs):
        pass

    def get_connection(self, conn_id: str):
        return Connection(
            conn_id="test_conn_sync",
            conn_type="http",
            password="secret_token",
        )


class TestGetAsyncConnection:
    @mock.patch("airflow.providers.common.compat.connection.BaseHook", new_callable=MockAgetBaseHook)
    @pytest.mark.asyncio
    async def test_get_async_connection_with_aget(self, _, caplog):
        with caplog.at_level(logging.DEBUG):
            conn = await get_async_connection("test_conn")
        assert conn.password == "secret_token_aget"
        assert conn.conn_type == "http"
        assert "Get connection using `MockAgetBaseHook.aget_connection()`." in caplog.text

    @mock.patch("airflow.providers.common.compat.connection.BaseHook", new_callable=MockBaseHook)
    @pytest.mark.asyncio
    async def test_get_async_connection_with_get_connection(self, _, caplog):
        with caplog.at_level(logging.DEBUG):
            conn = await get_async_connection("test_conn")
        assert conn.password == "secret_token"
        assert conn.conn_type == "http"
        assert "Get connection using `MockBaseHook.get_connection()`." in caplog.text

    @mock.patch("airflow.providers.common.compat.connection.BaseHook", new_callable=MockAgetBaseHook)
    @pytest.mark.asyncio
    async def test_get_async_connection_honors_passed_hook(self, _):
        class OverrideHook:
            @classmethod
            async def aget_connection(cls, conn_id: str):
                return Connection(conn_id="override", conn_type="http", password="override_token")

        conn = await get_async_connection("test_conn", hook=OverrideHook)
        assert conn.password == "override_token"

    @mock.patch("airflow.providers.common.compat.connection.BaseHook", new_callable=MockBaseHook)
    @pytest.mark.asyncio
    async def test_get_async_connection_honors_passed_hook_get_connection(self, _):
        class OverrideHook:
            @classmethod
            def get_connection(cls, conn_id: str):
                return Connection(conn_id="override", conn_type="http", password="override_token")

        conn = await get_async_connection("test_conn", hook=OverrideHook)
        assert conn.password == "override_token"


class _ConnectionBeforeAirflow332:
    """The Task SDK ``Connection`` before Airflow 3.3.2, without ``aextra_dejson``."""


@mock.patch("airflow.providers.common.compat.connection.Connection", _ConnectionBeforeAirflow332)
class TestGetAsyncConnectionBeforeAirflow332:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("as_instance", [False, True], ids=["hook-class", "hook-instance"])
    async def test_base_aget_connection_gives_way_to_get_connection_in_a_worker_thread(
        self, as_instance, caplog
    ):
        """``BaseHook.aget_connection`` masks synchronously there, so ``get_connection`` runs off the loop."""
        loop_thread = threading.get_ident()
        threads = []

        class SyncOnlyHook(BaseHook):
            @classmethod
            def get_connection(cls, conn_id: str):
                threads.append(threading.get_ident())
                return Connection(conn_id=conn_id, conn_type="http", password="sync_token")

        hook = SyncOnlyHook() if as_instance else SyncOnlyHook
        with caplog.at_level(logging.DEBUG):
            conn = await get_async_connection("test_conn", hook=hook)

        assert conn.password == "sync_token"
        assert len(threads) == 1
        assert threads[0] != loop_thread
        assert "Get connection using `SyncOnlyHook.get_connection()`." in caplog.text

    @pytest.mark.asyncio
    async def test_overridden_aget_connection_is_honored(self):
        class AsyncOverrideHook(BaseHook):
            @classmethod
            def get_connection(cls, conn_id: str):
                raise AssertionError("get_connection must not run when aget_connection is overridden")

            @classmethod
            async def aget_connection(cls, conn_id: str):
                return Connection(conn_id=conn_id, conn_type="http", password="async_token")

        conn = await get_async_connection("test_conn", hook=AsyncOverrideHook)

        assert conn.password == "async_token"

    @pytest.mark.asyncio
    async def test_own_aget_connection_is_honored_without_base_aget_connection(self):
        """Before Airflow 3.1 ``BaseHook`` has no ``aget_connection``, so a hook's own one is used."""

        class BaseHookBeforeAirflow31:
            """The ``BaseHook`` before Airflow 3.1, without ``aget_connection``."""

        class AsyncOwnHook(BaseHookBeforeAirflow31):
            @classmethod
            async def aget_connection(cls, conn_id: str):
                return Connection(conn_id=conn_id, conn_type="http", password="async_token")

        with mock.patch("airflow.providers.common.compat.connection.BaseHook", BaseHookBeforeAirflow31):
            conn = await get_async_connection("test_conn", hook=AsyncOwnHook)

        assert conn.password == "async_token"


def _raising_extra_dejson():
    return mock.PropertyMock(side_effect=AssertionError("extra_dejson must not run on the event loop"))


class TestGetAsyncExtraDejson:
    @pytest.mark.asyncio
    async def test_uses_aextra_dejson_when_available(self, caplog):
        """Airflow 3.3.2+: the extra comes from ``Connection.aextra_dejson()``, masked asynchronously."""
        conn = mock.Mock(spec=["extra", "extra_dejson", "aextra_dejson"])
        conn.aextra_dejson = mock.AsyncMock(return_value={"api_key": "secret"})
        type(conn).extra_dejson = _raising_extra_dejson()

        with caplog.at_level(logging.DEBUG):
            extra = await get_async_extra_dejson(conn)

        assert extra == {"api_key": "secret"}
        conn.aextra_dejson.assert_awaited_once_with()
        assert "Get connection extra using `Connection.aextra_dejson()`." in caplog.text

    @pytest.mark.asyncio
    async def test_falls_back_to_extra_dejson_in_a_worker_thread(self, caplog):
        """Older Airflow: ``extra_dejson`` runs off the event loop thread, where its sync masking is safe."""
        conn = mock.Mock(spec=["extra", "extra_dejson"])
        loop_thread = threading.get_ident()
        threads = []

        def extra_dejson():
            threads.append(threading.get_ident())
            return {"api_key": "secret"}

        type(conn).extra_dejson = mock.PropertyMock(side_effect=extra_dejson)

        with caplog.at_level(logging.DEBUG):
            extra = await get_async_extra_dejson(conn)

        assert extra == {"api_key": "secret"}
        assert len(threads) == 1
        assert threads[0] != loop_thread
        assert "Get connection extra using `Connection.extra_dejson` in a worker thread." in caplog.text

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("extra", "expected"),
        [
            pytest.param(None, {}, id="no-extra"),
            pytest.param(json.dumps({"timeout": 30}), {"timeout": 30}, id="extra"),
        ],
    )
    async def test_with_a_connection(self, extra, expected):
        conn = Connection(conn_id="test_conn", conn_type="http", extra=extra)

        assert await get_async_extra_dejson(conn) == expected
