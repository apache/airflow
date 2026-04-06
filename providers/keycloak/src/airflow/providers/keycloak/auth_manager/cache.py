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
from collections.abc import Callable

_CACHE_TTL_SECONDS = 30
_SINGLE_FLIGHT_TIMEOUT_SECONDS = 60

# Maps cache keys to (timestamp, result) pairs for TTL-based expiration.
_cache: dict[tuple, tuple[float, frozenset[str]]] = {}
# Tracks in-flight requests: maps cache keys to events that waiting threads block on.
_pending_requests: dict[tuple, threading.Event] = {}
_cache_lock = threading.Lock()


def _cache_get(key: tuple) -> frozenset[str] | None:
    entry = _cache.get(key)
    if entry and (time.monotonic() - entry[0]) < _CACHE_TTL_SECONDS:
        return entry[1]
    return None


def _cache_set(key: tuple, value: frozenset[str]) -> None:
    with _cache_lock:
        _cache[key] = (time.monotonic(), value)
        now = time.monotonic()
        for k in [k for k, (ts, _) in _cache.items() if now - ts > _CACHE_TTL_SECONDS * 2]:
            _cache.pop(k, None)


def single_flight(cache_key: tuple, query_keycloak: Callable[[], set[str]]) -> set[str]:
    """Return cached result, wait for a pending request, or run the query ourselves."""
    # Fast path: check cache without lock
    cached = _cache_get(cache_key)
    if cached is not None:
        return set(cached)

    with _cache_lock:
        cached = _cache_get(cache_key)
        if cached is not None:
            return set(cached)

        event = _pending_requests.get(cache_key)
        if event is not None:
            is_worker = False
        else:
            event = threading.Event()
            _pending_requests[cache_key] = event
            is_worker = True

    if not is_worker:
        # Wait for the other thread to finish
        event.wait(timeout=_SINGLE_FLIGHT_TIMEOUT_SECONDS)
        cached = _cache_get(cache_key)
        if cached is not None:
            return set(cached)
        # If the other thread failed, fall through and do the work ourselves

    try:
        result = query_keycloak()
        _cache_set(cache_key, frozenset(result))
        return result
    finally:
        with _cache_lock:
            event = _pending_requests.pop(cache_key, None)
        if event is not None:
            event.set()
