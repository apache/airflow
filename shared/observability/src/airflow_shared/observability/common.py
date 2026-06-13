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
