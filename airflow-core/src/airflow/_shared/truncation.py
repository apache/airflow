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
"""Shared truncation utilities for Airflow."""

from __future__ import annotations


def truncate_rendered_value(rendered: str, max_length: int) -> str:
    """
    Truncate rendered value respecting max_length while accounting for prefix, suffix, and repr() quotes.
    
    Args:
        rendered: The rendered value to truncate
        max_length: The maximum allowed length for the output
        
    Returns:
        Truncated string that respects the max_length constraint
    """
    if max_length <= 0:
        return ""

    prefix = "Truncated. You can change this behaviour in [core]max_templated_field_length. "
    suffix = "... "

    if max_length <= len(suffix):
        return suffix[:max_length]

    if max_length <= len(prefix) + len(suffix):
        # Not enough space for prefix + suffix + content, return truncated prefix + suffix
        return (prefix + suffix)[:max_length]

    # We have enough space for prefix + some content + suffix
    # Need to account for the fact that !r may add quotes, so we need to be more conservative
    available = max_length - len(prefix) - len(suffix)
    # If we're using !r formatting, it may add quotes, so we need to account for that
    # For strings, repr() adds 2 characters (quotes) around the content
    tentative_content = rendered[:available]
    tentative_repr = repr(tentative_content)
    if len(prefix) + len(tentative_repr) + len(suffix) <= max_length:
        return f"{prefix}{tentative_repr}{suffix}"
    else:
        # Need to reduce content length to account for the quotes added by repr()
        # We need to find the right content length so that len(prefix) + len(repr(content)) + len(suffix) <= max_length
        target_repr_length = max_length - len(prefix) - len(suffix)
        # Since repr adds quotes, we need to find content length where len(repr(content)) <= target_repr_length
        # For a string, repr adds 2 quotes, so content length should be target_repr_length - 2
        content_length = max(0, target_repr_length - 2)  # -2 for the quotes
        content_part = rendered[:content_length]
        return f"{prefix}{repr(content_part)}{suffix}"