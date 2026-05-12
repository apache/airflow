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
"""
Helpers shared between the Elasticsearch hooks and log handler.

Currently this exposes a single helper, :func:`apply_compat_with`, that lets the
provider keep working against an Elasticsearch server whose major version does
not match the installed ``elasticsearch`` Python client major. See the helper
docstring for the regression context.
"""

from __future__ import annotations

import functools
import re
from typing import TYPE_CHECKING

from airflow.providers.common.compat.sdk import AirflowConfigException, conf

if TYPE_CHECKING:
    import elasticsearch


# Matches the JSON / NDJSON / mapbox vector-tile mimetypes the ``elasticsearch``
# client negotiates, in either form: the raw ``application/json`` (what the
# generated client code writes into ``__headers``) and the already-rewritten
# ``application/vnd.elasticsearch+json; compatible-with=<N>`` (what
# ``mimetype_header_to_compat`` in ``elasticsearch/_sync/client/_base.py``
# emits before handing the request to the transport). Both forms must be
# rewritten to ``compatible-with=<configured>``; anything else (notably
# ``text/plain`` used by the cat APIs) is left intact, mirroring upstream's
# selective substitution behaviour.
_COMPAT_MIMETYPE_RE = re.compile(
    r"application/(?:vnd\.elasticsearch\+)?(json|x-ndjson|vnd\.mapbox-vector-tile)"
    r"(?:\s*;\s*compatible-with=\d+)?"
)
_COMPAT_MAJOR_RE = re.compile(r"^\d+$")


def apply_compat_with(client: elasticsearch.Elasticsearch) -> elasticsearch.Elasticsearch:
    """
    Pin the ``compatible-with`` HTTP content-negotiation level for ``client``.

    The ``elasticsearch`` Python client always negotiates ``compatible-with=<client_major>``
    on every request; an Elasticsearch server with a different major version rejects
    the request with HTTP 400 ``media_type_header_exception``. This is what happens
    when an ``elasticsearch>=9`` client (the current default) talks to an
    Elasticsearch 8.x server, which broke remote logging for ES 8 deployments
    starting with provider 6.5.1.

    When ``[elasticsearch] es_compat_with`` is set to a major version string
    (e.g. ``"7"``, ``"8"``, ``"9"``) this helper wraps the client's transport
    so every outbound request rewrites the ``compatible-with=<N>`` parameter on
    the JSON / NDJSON / mapbox vector-tile parts of the ``Accept`` and
    ``Content-Type`` headers. Non-JSON parts (notably ``text/plain`` used by
    the cat APIs) are preserved verbatim, mirroring how elasticsearch-py's own
    ``mimetype_header_to_compat`` handles content negotiation.

    When the option is unset the client is returned unchanged and behaves
    exactly as before.
    """
    raw = conf.get("elasticsearch", "es_compat_with", fallback=None)
    compat = (raw or "").strip()
    if not compat:
        return client
    if not _COMPAT_MAJOR_RE.match(compat):
        raise AirflowConfigException(
            "[elasticsearch] es_compat_with must be a positive integer major version "
            f"(e.g. '7', '8', '9'); got {raw!r}."
        )

    transport = client.transport
    if "perform_request" in transport.__dict__:
        # Already wrapped on this transport instance — no-op so repeated calls
        # to ``apply_compat_with`` (e.g. across hook reuse) stay idempotent.
        return client

    sub = rf"application/vnd.elasticsearch+\g<1>; compatible-with={compat}"
    original_perform_request = transport.perform_request

    # Accept ``*args, **kwargs`` so the wrapper survives future elastic_transport
    # ``perform_request`` signature changes (new keyword-only params, reordered
    # positionals). ``functools.wraps`` preserves the original ``__name__`` /
    # ``__doc__`` / ``__wrapped__`` for tooling and introspection.
    @functools.wraps(original_perform_request)
    def perform_request(*args, **kwargs):  # type: ignore[no-untyped-def]
        headers = kwargs.get("headers")
        if not headers:
            return original_perform_request(*args, **kwargs)
        # Walk every key case-insensitively so a future elastic_transport that
        # forwards PascalCase headers does not silently bypass the rewrite.
        merged = dict(headers)
        for key in list(merged):
            if key.lower() in ("accept", "content-type") and merged[key]:
                merged[key] = _COMPAT_MIMETYPE_RE.sub(sub, merged[key])
        kwargs["headers"] = merged
        return original_perform_request(*args, **kwargs)

    # ``setattr`` instead of direct attribute assignment so mypy does not flag a
    # ``method-assign`` error — we are *intentionally* shadowing the bound method
    # at the instance level (the idempotency guard above checks the instance
    # ``__dict__``).
    setattr(transport, "perform_request", perform_request)
    return client
