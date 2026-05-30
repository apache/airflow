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

from unittest import mock

import pytest

from airflow.models.connection import Connection
from airflow.providers.common.compat.connection import get_async_connection


class MockAgetBaseHook:
    @classmethod
    async def aget_connection(cls, conn_id: str):
        return Connection(
            conn_id="test_conn",
            conn_type="http",
            password="secret_token_aget",
        )


class MockBaseHook:
    @classmethod
    def get_connection(cls, conn_id: str):
        return Connection(
            conn_id="test_conn_sync",
            conn_type="http",
            password="secret_token",
        )


class MockAsyncSubclass(MockAgetBaseHook):
    @classmethod
    async def aget_connection(cls, conn_id: str):
        return Connection(
            conn_id=conn_id,
            conn_type="http",
            password="secret_token_async_subclass",
        )


class MockSyncSubclass(MockBaseHook):
    @classmethod
    def get_connection(cls, conn_id: str):
        return Connection(
            conn_id=conn_id,
            conn_type="http",
            password="secret_token_sync_subclass",
        )


class TestGetAsyncConnection:
    @mock.patch("airflow.providers.common.compat.connection.BaseHook", new=MockAgetBaseHook)
    @pytest.mark.asyncio
    async def test_get_async_connection_with_aget(self):
        conn = await get_async_connection("test_conn")
        assert conn.password == "secret_token_aget"
        assert conn.conn_type == "http"

    @mock.patch("airflow.providers.common.compat.connection.BaseHook", new=MockBaseHook)
    @pytest.mark.asyncio
    async def test_get_async_connection_with_get_connection(self):
        conn = await get_async_connection("test_conn")
        assert conn.password == "secret_token"
        assert conn.conn_type == "http"

    @pytest.mark.asyncio
    async def test_get_async_connection_uses_hook_class_aget_connection_override(self):
        conn = await get_async_connection("test_conn", hook_class=MockAsyncSubclass)

        assert conn.conn_id == "test_conn"
        assert conn.password == "secret_token_async_subclass"

    @pytest.mark.asyncio
    async def test_get_async_connection_uses_hook_class_get_connection_override(self):
        conn = await get_async_connection("test_conn", hook_class=MockSyncSubclass)

        assert conn.conn_id == "test_conn"
        assert conn.password == "secret_token_sync_subclass"
