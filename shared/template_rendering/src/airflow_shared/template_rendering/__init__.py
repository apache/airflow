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


def truncate_rendered_value(rendered: str, max_length: int) -> str:
    MIN_CONTENT_LENGTH = 7

    if max_length <= 0:
        return ""

    # Build truncation message once, return if max_length is too small
    prefix = "Truncated. You can change this behaviour in [core]max_templated_field_length. "
    suffix = "..."
    trunc_only = f"{prefix}{suffix}"

    if max_length < len(trunc_only):
        return trunc_only

    # Compute available space for content
    overhead = len(prefix) + len(suffix)
    available = max_length - overhead

    if available < MIN_CONTENT_LENGTH:
        return trunc_only

    # Slice content to fit and construct final string
    content = rendered[:available].rstrip()
    result = f"{prefix}{content}{suffix}"

    if len(result) > max_length:
        log.warning(
            "Truncated value still exceeds max_length=%d; this should not happen.",
            max_length,
        )

    return result


__all__ = ["truncate_rendered_value"]
