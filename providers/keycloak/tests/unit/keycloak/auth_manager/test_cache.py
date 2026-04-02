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

import threading
import time

import pytest

from airflow.providers.keycloak.auth_manager import cache as cache_module
from airflow.providers.keycloak.auth_manager.cache import single_flight


@pytest.fixture(autouse=True)
def _clear_cache():
    cache_module._cache.clear()
    cache_module._pending_requests.clear()
    yield
    cache_module._cache.clear()
    cache_module._pending_requests.clear()


class TestSingleFlight:
    def test_returns_query_result(self):
        result = single_flight(("key",), lambda: {"a", "b"})
        assert result == {"a", "b"}

    def test_cache_hit(self):
        call_count = 0

        def query():
            nonlocal call_count
            call_count += 1
            return {"a"}

        single_flight(("key",), query)
        single_flight(("key",), query)

        assert call_count == 1

    def test_different_keys_not_cached(self):
        call_count = 0

        def query():
            nonlocal call_count
            call_count += 1
            return {"a"}

        single_flight(("key1",), query)
        single_flight(("key2",), query)

        assert call_count == 2

    def test_cache_expires(self):
        call_count = 0

        def query():
            nonlocal call_count
            call_count += 1
            return {"a"}

        single_flight(("key",), query)

        # Expire the cache entry by backdating its timestamp
        for k in cache_module._cache:
            ts, val = cache_module._cache[k]
            cache_module._cache[k] = (ts - cache_module._CACHE_TTL_SECONDS - 1, val)

        single_flight(("key",), query)
        assert call_count == 2

    def test_concurrent_dedup(self):
        """Multiple threads with the same key coalesce into one call."""
        gate = threading.Event()
        call_count = 0

        def slow_query():
            nonlocal call_count
            call_count += 1
            gate.wait(timeout=5)
            return {"a"}

        results = [None] * 5
        errors = []

        def run(index):
            try:
                results[index] = single_flight(("key",), slow_query)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=run, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()

        time.sleep(0.1)
        gate.set()

        for t in threads:
            t.join(timeout=5)

        assert not errors, f"Threads raised errors: {errors}"
        for r in results:
            assert r == {"a"}
        assert call_count == 1

    def test_failed_query_allows_retry(self):
        """If the worker thread fails, another thread can retry."""
        call_count = 0

        def failing_then_ok():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("boom")
            return {"a"}

        with pytest.raises(ValueError, match="boom"):
            single_flight(("key",), failing_then_ok)

        result = single_flight(("key",), failing_then_ok)
        assert result == {"a"}
        assert call_count == 2
