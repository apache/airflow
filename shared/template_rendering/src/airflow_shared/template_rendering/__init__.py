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

import logging

log = logging.getLogger(__name__)

# Public truncation configuration used by ``truncate_rendered_value``.
# Exposed as module-level constants to avoid duplicating literals in callers/tests.
TRUNCATE_MIN_CONTENT_LENGTH = 7
TRUNCATE_PREFIX = "Truncated. You can change this behaviour in [core]max_templated_field_length. "
TRUNCATE_SUFFIX = "..."


def truncate_rendered_value(rendered: str, max_length: int) -> str:
    """
    Truncate a rendered template value to approximately ``max_length`` characters.

    Behavior:

    * If ``max_length <= 0``, an empty string is returned.
    * A fixed prefix (``TRUNCATE_PREFIX``) and suffix (``TRUNCATE_SUFFIX``) are always
      included when truncation occurs. The minimal truncation-only message is::

          f"{TRUNCATE_PREFIX}{TRUNCATE_SUFFIX}"

    * If ``max_length`` is smaller than the length of this truncation-only message, that
      message is returned in full, even though its length may exceed ``max_length``.
    * Otherwise, space remaining after the prefix and suffix is allocated to the original
      ``rendered`` content. Content is only appended if at least
      ``TRUNCATE_MIN_CONTENT_LENGTH`` characters are available; if fewer are available,
      the truncation-only message is returned instead.

    Note: this function is best-effort — the return value is intended to be no longer than
    ``max_length``, but when ``max_length < len(TRUNCATE_PREFIX + TRUNCATE_SUFFIX)`` it
    intentionally returns a longer string to preserve the full truncation message.
    """
    if max_length <= 0:
        return ""

    trunc_only = f"{TRUNCATE_PREFIX}{TRUNCATE_SUFFIX}"

    if max_length < len(trunc_only):
        return trunc_only

    # Compute available space for content
    overhead = len(TRUNCATE_PREFIX) + len(TRUNCATE_SUFFIX)
    available = max_length - overhead

    if available < TRUNCATE_MIN_CONTENT_LENGTH:
        return trunc_only

    # Slice content to fit and construct final string
    content = rendered[:available].rstrip()
    result = f"{TRUNCATE_PREFIX}{content}{TRUNCATE_SUFFIX}"

    if len(result) > max_length:
        log.warning(
            "Truncated value still exceeds max_length=%d; this should not happen.",
            max_length,
        )

    return result


__all__ = [
    "TRUNCATE_MIN_CONTENT_LENGTH",
    "TRUNCATE_PREFIX",
    "TRUNCATE_SUFFIX",
    "truncate_rendered_value",
]
