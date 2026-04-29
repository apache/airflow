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
"""Wire-level tests for ``airflow.providers.elasticsearch._compat.apply_compat_with``.

These tests intercept ``elastic_transport.Transport.perform_request`` so they
observe the exact ``Accept`` / ``Content-Type`` headers the helper produces.
Asserting on ``client._headers`` (or any other in-memory state on the client)
would not be enough: the elasticsearch-py client always re-applies its own
per-API-method content-negotiation headers right before the request goes out,
which is the very behaviour this helper has to override. Only what the
``Transport`` sees on the wire matters.
"""

from __future__ import annotations

import contextlib

import pytest
from elastic_transport import Transport
from elasticsearch import Elasticsearch

from airflow.providers.elasticsearch._compat import apply_compat_with

from tests_common.test_utils.config import conf_vars


def _trigger_calls(client: Elasticsearch) -> None:
    """Drive ``search``, ``info`` and ``bulk`` against the spy transport.

    Each call hits the spy and raises; we swallow that and rely on the spy's
    ``captured`` list to make assertions.
    """
    for action in (
        lambda: client.search(index="airflow-logs", query={"match_all": {}}),
        lambda: client.info(),
        lambda: client.bulk(operations=[{"index": {"_index": "x"}}, {"hello": "world"}]),
    ):
        with contextlib.suppress(RuntimeError):
            action()


@pytest.fixture
def wire_capture(monkeypatch):
    """Replace ``Transport.perform_request`` with a recording spy.

    The spy is installed at the *class* level (not on an instance) so it is
    picked up by both the original transport and by the wrapper produced by
    :func:`apply_compat_with`. The wrapper resolves the original
    ``perform_request`` at wrap time, so the spy must already be in place when
    ``apply_compat_with`` runs.
    """
    captured: list[dict] = []

    def spy(self, method, target, *, body=None, headers=None, **kwargs):
        captured.append(
            {"method": method, "target": target, "headers": dict(headers or {})}
        )
        raise RuntimeError("captured")

    monkeypatch.setattr(Transport, "perform_request", spy)
    return captured


@pytest.mark.parametrize("unset_value", ["", None])
def test_apply_compat_with_unset_does_not_wrap_transport(unset_value):
    """When the option is unset the helper returns the client untouched."""
    client = Elasticsearch("http://localhost:9200")
    original = client.transport.perform_request
    with conf_vars({("elasticsearch", "es_compat_with"): unset_value}):
        same = apply_compat_with(client)
    assert same is client
    assert client.transport.perform_request is original


def test_apply_compat_with_pins_compatible_with_8(wire_capture):
    """With ``es_compat_with = "8"`` every outbound call carries ``compatible-with=8``."""
    with conf_vars({("elasticsearch", "es_compat_with"): "8"}):
        client = apply_compat_with(Elasticsearch("http://localhost:9200"))

    _trigger_calls(client)

    assert {c["method"] for c in wire_capture} == {"POST", "GET", "PUT"}
    expected_json = "application/vnd.elasticsearch+json; compatible-with=8"
    expected_ndjson = "application/vnd.elasticsearch+x-ndjson; compatible-with=8"

    by_method = {c["method"]: c for c in wire_capture}
    assert by_method["POST"]["headers"]["accept"] == expected_json
    assert by_method["POST"]["headers"]["content-type"] == expected_json
    assert by_method["GET"]["headers"]["accept"] == expected_json
    # ``info()`` does not send a body, so content-type is absent on the wire
    assert by_method["GET"]["headers"].get("content-type") in (None, "")
    assert by_method["PUT"]["headers"]["accept"] == expected_json
    # bulk preserves the ndjson form so the server can stream the body
    assert by_method["PUT"]["headers"]["content-type"] == expected_ndjson


def test_apply_compat_with_pins_compatible_with_7(wire_capture):
    """The helper accepts arbitrary major version strings, not just ``"8"``."""
    with conf_vars({("elasticsearch", "es_compat_with"): "7"}):
        client = apply_compat_with(Elasticsearch("http://localhost:9200"))

    _trigger_calls(client)
    assert wire_capture, "spy should have captured at least one call"
    assert all(
        c["headers"]["accept"] == "application/vnd.elasticsearch+json; compatible-with=7"
        for c in wire_capture
    )
