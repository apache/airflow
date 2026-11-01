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
from __future__ import annotations

import os


def _resolve_otlp_protocol(specific_env_var: str) -> str:
    """
    Return the OTLP transport per the OTel SDK environment-variable spec.

    Returns ``'http/protobuf'`` (default) or ``'grpc'``. ``specific_env_var`` is
    the signal-specific override (e.g. ``OTEL_EXPORTER_OTLP_METRICS_PROTOCOL``
    or ``OTEL_EXPORTER_OTLP_TRACES_PROTOCOL``); falls back to the generic
    ``OTEL_EXPORTER_OTLP_PROTOCOL`` and finally the default.

    See:
      https://opentelemetry.io/docs/specs/otel/protocol/exporter/#specify-protocol
    """
    return (
        os.environ.get(specific_env_var) or os.environ.get("OTEL_EXPORTER_OTLP_PROTOCOL") or "http/protobuf"
    )


def _format_url_host(host: str | None) -> str | None:
    """
    Bracket IPv6 host literals for embedding in a URL authority.

    Per RFC 3986 §3.2.2, IPv6 hosts in a URI must be enclosed in square
    brackets so the ``:`` separators do not conflict with the ``host:port``
    delimiter. Hostnames, IPv4 literals, and already-bracketed v6 literals
    are returned unchanged. ``None`` is passed through so existing
    error-logging paths keep their shape.
    """
    if host is not None and ":" in host and not host.startswith("["):
        return f"[{host}]"
    return host
