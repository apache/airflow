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

from datetime import datetime, timezone

import httpx
import pytest

from airflow.dag_processing.api_client import _CallableBearerAuth, _parse_iso_datetime


class TestParseIsoDatetime:
    @pytest.mark.parametrize(
        "value",
        [
            "2026-06-03T03:52:13.938753Z",  # trailing Z (server format; rejected by 3.10 fromisoformat)
            "2026-06-03T03:52:13.938753+00:00",  # explicit offset
        ],
    )
    def test_parses_utc_with_or_without_z(self, value):
        parsed = _parse_iso_datetime(value)
        assert parsed == datetime(2026, 6, 3, 3, 52, 13, 938753, tzinfo=timezone.utc)
        assert parsed.utcoffset() == timezone.utc.utcoffset(None)


def _drive_auth_flow(auth: _CallableBearerAuth, statuses: list[int]) -> list[str | None]:
    """Drive ``auth.auth_flow`` the way httpx does, feeding it the given response statuses.

    Returns the ``Authorization`` header value sent for each request (snapshotted at send time,
    since httpx mutates the one request object in place on a 401-triggered retry). A retry shows up
    as a second entry.
    """
    gen = auth.auth_flow(httpx.Request("GET", "http://host/path"))
    sent_auth: list[str | None] = []
    try:
        request = next(gen)
        for status in statuses:
            sent_auth.append(request.headers.get("Authorization"))
            request = gen.send(httpx.Response(status, request=request))
    except StopIteration:
        pass
    return sent_auth


class TestCallableBearerAuth:
    def test_attaches_bearer_token_from_getter(self):
        auth = _CallableBearerAuth(lambda: "abc")
        assert _drive_auth_flow(auth, [200]) == ["Bearer abc"]

    def test_no_header_when_token_is_none(self):
        auth = _CallableBearerAuth(lambda: None)
        assert _drive_auth_flow(auth, [200]) == [None]

    def test_token_is_cached_within_ttl(self):
        calls = []

        def getter():
            calls.append(1)
            return "t"

        auth = _CallableBearerAuth(getter, cache_ttl=1000.0)
        _drive_auth_flow(auth, [200])
        _drive_auth_flow(auth, [200])
        # Second request reused the cached token rather than re-reading.
        assert len(calls) == 1

    def test_401_rereads_token_and_retries_once(self):
        tokens = iter(["stale", "fresh"])
        auth = _CallableBearerAuth(lambda: next(tokens), cache_ttl=1000.0)

        # First request carries the stale token; the 401 re-reads (bypassing the cache) and retries
        # with the fresh one.
        assert _drive_auth_flow(auth, [401, 200]) == ["Bearer stale", "Bearer fresh"]

    def test_no_retry_when_reread_token_unchanged(self):
        auth = _CallableBearerAuth(lambda: "same", cache_ttl=1000.0)
        # Re-read returned the same token, so there's nothing to retry with.
        assert _drive_auth_flow(auth, [401, 200]) == ["Bearer same"]
