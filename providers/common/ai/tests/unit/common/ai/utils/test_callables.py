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

import functools

import pytest

from airflow.providers.common.ai.utils.callables import is_async_callable


async def async_fn(value):
    return value


def sync_fn(value):
    return value


class TestIsAsyncCallable:
    @pytest.mark.parametrize(
        "fn",
        [
            async_fn,
            functools.partial(async_fn, object()),
        ],
    )
    def test_detects_async_functions_and_partials(self, fn):
        assert is_async_callable(fn) is True

    def test_detects_async_callable_object(self):
        class AsyncCallable:
            async def __call__(self):
                return "ok"

        assert is_async_callable(AsyncCallable()) is True

    @pytest.mark.parametrize(
        "fn",
        [
            sync_fn,
            functools.partial(sync_fn, object()),
        ],
    )
    def test_rejects_sync_functions_and_partials(self, fn):
        assert is_async_callable(fn) is False

    def test_rejects_sync_callable_object(self):
        class SyncCallable:
            def __call__(self):
                return "ok"

        assert is_async_callable(SyncCallable()) is False
