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

from airflow.providers.common.compat.hook import get_async_hook
from airflow.providers.common.compat.sdk import BaseHook

_MOCK_HOOK = mock.MagicMock(spec=BaseHook)


class MockAgetBaseHook:
    def __init__(self, *args, **kwargs):
        self.last_call = {}

    async def aget_hook(self, conn_id: str, hook_params: dict | None = None):
        self.last_call = {"conn_id": conn_id, "hook_params": hook_params}
        return _MOCK_HOOK


class MockBaseHook:
    def __init__(self, *args, **kwargs):
        self.last_call = {}

    def get_hook(self, conn_id: str, hook_params: dict | None = None):
        self.last_call = {"conn_id": conn_id, "hook_params": hook_params}
        return _MOCK_HOOK


class TestGetAsyncHook:
    @pytest.mark.parametrize("hook_params", [None, {"key": "value"}])
    @mock.patch("airflow.providers.common.compat.hook.BaseHook", new_callable=MockAgetBaseHook)
    @pytest.mark.asyncio
    async def test_get_async_hook_uses_aget_hook_when_available(self, mock_hook_class, hook_params):
        result = await get_async_hook("test_conn", hook_params=hook_params)
        assert result is _MOCK_HOOK
        assert mock_hook_class.last_call == {"conn_id": "test_conn", "hook_params": hook_params}

    @pytest.mark.parametrize("hook_params", [None, {"key": "value"}])
    @mock.patch("airflow.providers.common.compat.hook.BaseHook", new_callable=MockBaseHook)
    @pytest.mark.asyncio
    async def test_get_async_hook_falls_back_to_get_hook(self, mock_hook_class, hook_params):
        result = await get_async_hook("test_conn", hook_params=hook_params)
        assert result is _MOCK_HOOK
        assert mock_hook_class.last_call == {"conn_id": "test_conn", "hook_params": hook_params}
