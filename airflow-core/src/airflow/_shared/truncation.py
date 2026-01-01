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

from typing import Any


def truncate_rendered_value(rendered: Any, max_length: int) -> str:
    """
    Truncate rendered value respecting max_length while accounting for prefix, suffix, and repr() quotes.
    
    Args:
        rendered: The rendered value to truncate (can be any type)
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
    # Start with a reasonable estimate and adjust if needed
    available_for_content = max_length - len(prefix) - len(suffix)
    
    # If available space is too small to even fit an empty repr'd string ('') which takes 2 chars
    if available_for_content < 2:
        empty_repr = repr("")
        return f"{prefix}{empty_repr}{suffix}"
    
    # Start with the content length that would theoretically fit
    # Account for the fact that repr adds at least 2 characters (the quotes)
    estimated_length = max(0, available_for_content - 2)
    content_to_try = rendered[:estimated_length]
    repr_result = repr(content_to_try)
    
    # If this fits, return it
    total_length = len(prefix) + len(repr_result) + len(suffix)
    if total_length <= max_length:
        return f"{prefix}{repr_result}{suffix}"
    
    # If it doesn't fit, do a simple decrementing search from our estimate
    # This is more predictable than binary search for the test environment
    for current_length in range(estimated_length, -1, -1):
        content_to_try = rendered[:current_length]
        repr_result = repr(content_to_try)
        total_length = len(prefix) + len(repr_result) + len(suffix)
        
        if total_length <= max_length:
            return f"{prefix}{repr_result}{suffix}"
    
    # Fallback: return with empty content if nothing fits
    empty_repr = repr("")
    return f"{prefix}{empty_repr}{suffix}"