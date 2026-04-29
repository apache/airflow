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
"""Helpers shared between the Elasticsearch hooks and log handler.

Currently this exposes a single helper, :func:`apply_compat_with`, that lets the
provider keep working against an Elasticsearch server whose major version does
not match the installed ``elasticsearch`` Python client major. See the helper
docstring for the regression context.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.providers.common.compat.sdk import conf

if TYPE_CHECKING:
    import elasticsearch


_JSON_MIME = "application/vnd.elasticsearch+json"
_NDJSON_MIME = "application/vnd.elasticsearch+x-ndjson"


def apply_compat_with(client: elasticsearch.Elasticsearch) -> elasticsearch.Elasticsearch:
    """Pin the ``compatible-with`` HTTP content-negotiation level for ``client``.

    The ``elasticsearch`` Python client always negotiates ``compatible-with=<client_major>``
    on every request; an Elasticsearch server with a different major version rejects
    the request with HTTP 400 ``media_type_header_exception``. This is what happens
    when an ``elasticsearch>=9`` client (the current default) talks to an
    Elasticsearch 8.x server, which broke remote logging for ES 8 deployments
    starting with provider 6.5.1.

    When ``[elasticsearch] es_compat_with`` is set to a major version string
    (e.g. ``"7"``, ``"8"``, ``"9"``) this helper wraps the client's transport
    so every outbound request carries
    ``Accept: application/vnd.elasticsearch+json; compatible-with=<major>``
    and the matching ``Content-Type`` (using the ``+x-ndjson`` form for bulk
    requests so multi-line bodies still parse on the server).

    When the option is unset the client is returned unchanged and behaves
    exactly as before.
    """
    compat = conf.get("elasticsearch", "es_compat_with", fallback=None)
    if not compat:
        return client

    transport = client.transport
    if "perform_request" in transport.__dict__:
        # Already wrapped on this transport instance — no-op so repeated calls
        # to ``apply_compat_with`` (e.g. across hook reuse) stay idempotent.
        return client

    json_media = f"{_JSON_MIME}; compatible-with={compat}"
    ndjson_media = f"{_NDJSON_MIME}; compatible-with={compat}"

    original_perform_request = transport.perform_request

    def perform_request(method, target, *, body=None, headers=None, **kwargs):  # type: ignore[no-untyped-def]
        merged = dict(headers) if headers else {}
        if merged.get("accept"):
            merged["accept"] = json_media
        content_type = merged.get("content-type") or ""
        # ``+x-ndjson`` is the only streaming form elasticsearch-py uses today
        # (bulk requests). ``"ndjson" in ct`` already matches both ``ndjson``
        # and ``x-ndjson``, so a single check is enough.
        if "ndjson" in content_type:
            merged["content-type"] = ndjson_media
        elif content_type:
            merged["content-type"] = json_media
        return original_perform_request(method, target, body=body, headers=merged, **kwargs)

    transport.perform_request = perform_request
    return client
