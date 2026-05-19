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

from airflow.providers.common.compat.sdk import AirflowConfigException
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
        captured.append({"method": method, "target": target, "headers": dict(headers or {})})
        raise RuntimeError("captured")

    monkeypatch.setattr(Transport, "perform_request", spy)
    return captured


@pytest.mark.parametrize("unset_value", ["", None])
def test_apply_compat_with_unset_does_not_wrap_transport(unset_value):
    """When the option is unset the helper returns the client untouched.

    The wrap installs ``perform_request`` as an instance attribute on the
    transport, so the most precise way to assert the helper is a no-op is to
    check that no such instance attribute was set (i.e. lookup still resolves
    to the class method). Identity (``is``) comparison on
    ``transport.perform_request`` would not work even in the no-op case
    because attribute access on a bound method produces a fresh wrapper each
    time.

    Note: ``conf_vars`` removes the override when the value is ``None``, so
    both parametrized cases ultimately resolve to the provider yaml default
    (``""``). The parametrize is kept to document that callers passing either
    sentinel get the no-op path; the actual ``None`` branch in the helper is
    covered by ``test_apply_compat_with_unset_via_missing_conf`` below.
    """
    client = Elasticsearch("http://localhost:9200")
    assert "perform_request" not in client.transport.__dict__
    with conf_vars({("elasticsearch", "es_compat_with"): unset_value}):
        same = apply_compat_with(client)
    assert same is client
    assert "perform_request" not in client.transport.__dict__


def test_apply_compat_with_unset_via_missing_conf(monkeypatch):
    """Cover the ``conf.get`` returning ``None`` branch directly."""
    from airflow.providers.elasticsearch import _compat

    monkeypatch.setattr(_compat.conf, "get", lambda *args, **kwargs: None)
    client = Elasticsearch("http://localhost:9200")
    assert apply_compat_with(client) is client
    assert "perform_request" not in client.transport.__dict__


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


def test_apply_compat_with_preserves_text_plain_for_cat_apis(wire_capture):
    """Cat APIs send ``Accept: text/plain[,application/json]``; the wrapper must
    preserve the ``text/plain`` part. Earlier revisions of the helper unconditionally
    rewrote ``accept`` to ``application/vnd.elasticsearch+json; compatible-with=N``,
    which silently turned every ``cat.*`` response into JSON instead of plain text.

    We mirror elasticsearch-py's own ``mimetype_header_to_compat`` (only
    ``application/(json|x-ndjson|vnd.mapbox-vector-tile)`` parts get the
    ``compatible-with`` suffix), so this test fails fast if anyone reverts to the
    blanket overwrite.
    """
    with conf_vars({("elasticsearch", "es_compat_with"): "8"}):
        client = apply_compat_with(Elasticsearch("http://localhost:9200"))

    for action in (lambda: client.cat.help(), lambda: client.cat.indices()):
        with contextlib.suppress(RuntimeError):
            action()

    accepts = [c["headers"].get("accept") for c in wire_capture]
    # ``cat.help`` ships ``text/plain`` only; it must come through verbatim.
    assert "text/plain" in accepts, accepts
    # ``cat.indices`` ships ``text/plain,application/json``; the JSON half gets
    # the ``compatible-with=8`` suffix, the text/plain half stays put.
    assert any(
        a and a.startswith("text/plain,application/vnd.elasticsearch+json; compatible-with=8")
        for a in accepts
    ), accepts


def test_apply_compat_with_handles_pascal_case_headers(monkeypatch):
    """Defensive: if ``elastic_transport`` ever forwards PascalCase header keys,
    the rewrite must still apply (a lowercase-only ``dict.get`` would silently
    no-op and let ``compatible-with=<client_major>`` ship to the server).
    """
    seen: dict = {}

    def spy(self, method, target, *, body=None, headers=None, **kwargs):
        seen["headers"] = dict(headers or {})
        raise RuntimeError("captured")

    monkeypatch.setattr(Transport, "perform_request", spy)

    with conf_vars({("elasticsearch", "es_compat_with"): "8"}):
        client = apply_compat_with(Elasticsearch("http://localhost:9200"))

    # Drive the wrapper with PascalCase keys directly — bypassing the
    # ``_BaseClient.perform_request`` normalization.
    with contextlib.suppress(RuntimeError):
        client.transport.perform_request(
            "GET",
            "/",
            headers={"Accept": "application/json", "Content-Type": "application/json"},
        )

    assert seen["headers"]["Accept"] == "application/vnd.elasticsearch+json; compatible-with=8"
    assert seen["headers"]["Content-Type"] == "application/vnd.elasticsearch+json; compatible-with=8"


def test_apply_compat_with_strips_whitespace_in_config(wire_capture):
    """Operators occasionally write ``es_compat_with = " 8"``; the helper must
    strip whitespace before interpolating into the wire header, otherwise the
    server returns 400 and the helper fails open in a confusing way.
    """
    with conf_vars({("elasticsearch", "es_compat_with"): " 8 "}):
        client = apply_compat_with(Elasticsearch("http://localhost:9200"))

    with contextlib.suppress(RuntimeError):
        client.search(index="airflow-logs", query={"match_all": {}})

    assert wire_capture[-1]["headers"]["accept"] == "application/vnd.elasticsearch+json; compatible-with=8"


@pytest.mark.parametrize("bad_value", ["v8", "8.0", "abc", "8;9"])
def test_apply_compat_with_rejects_non_numeric_major(bad_value):
    """A non-numeric ``es_compat_with`` would otherwise produce malformed wire
    headers (``compatible-with=v8``) and a per-request 400 storm. Fail fast at
    construction time with a config exception so the misconfiguration is
    obvious in the worker startup log.
    """
    with conf_vars({("elasticsearch", "es_compat_with"): bad_value}):
        with pytest.raises(AirflowConfigException, match="es_compat_with"):
            apply_compat_with(Elasticsearch("http://localhost:9200"))


def test_apply_compat_with_is_idempotent():
    """Calling ``apply_compat_with`` twice on the same client only wraps once."""
    with conf_vars({("elasticsearch", "es_compat_with"): "8"}):
        client = apply_compat_with(Elasticsearch("http://localhost:9200"))
        first_wrapper = client.transport.__dict__["perform_request"]
        apply_compat_with(client)
        second_wrapper = client.transport.__dict__["perform_request"]
    assert first_wrapper is second_wrapper
