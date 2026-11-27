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
"""Resource sub-clients for the local REST client."""

from __future__ import annotations

from urllib.parse import quote


def quote_segment(value: object) -> str:
    """
    URL-encode a single path segment.

    Identifiers such as connection IDs, variable keys, pool names, task IDs, and run IDs may contain
    characters that are reserved in URLs (``/``, ``?``, ``#``, ...). Encoding each segment with
    ``safe=""`` ensures they map to the intended route rather than being misrouted or truncated. The
    ``~`` wildcard accepted by some collection routes is left intact (``quote`` treats it as safe).
    """
    return quote(str(value), safe="")
