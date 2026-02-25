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

from airflow_shared.template_rendering import truncate_rendered_value


def test_truncate_rendered_value_prioritizes_message():
    """Test that truncation message is always shown first, content only if space allows."""
    test_cases = [
        (1, "test", "Minimum value"),
        (3, "test", "At ellipsis length"),
        (5, "test", "Very small"),
        (10, "password123", "Small"),
        (20, "secret_value", "Small with content"),
        (50, "This is a test string", "Medium"),
        (83, "Hello World", "At prefix+suffix boundary v1"),
        (84, "Hello World", "Just above boundary v1"),
        (86, "Hello World", "At overhead boundary v2"),
        (90, "short", "Normal case - short string"),
        (100, "This is a longer string", "Normal case"),
        (
            150,
            "x" * 200,
            "Large max_length, long string",
        ),
        (100, "None", "String 'None'"),
        (100, "True", "String 'True'"),
        (100, "{'key': 'value'}", "Dict-like string"),
        (100, "test's", "String with apostrophe"),
        (90, '"quoted"', "String with quotes"),
    ]

    prefix = "Truncated. You can change this behaviour in [core]max_templated_field_length. "
    suffix = "..."
    trunc_only = f"{prefix}{suffix}"
    trunc_only_len = len(trunc_only)  # 81
    overhead = len(prefix) + len(suffix)  # 81
    # Content is only shown when available >= MIN_CONTENT_LENGTH (7)
    min_length_for_content = overhead + 7  # 88

    for max_length, rendered, description in test_cases:
        result = truncate_rendered_value(rendered, max_length)

        # Always should contain the prefix message
        assert result.startswith(prefix), f"Failed for {description}: result should start with prefix"

        # For very small max_length values, should return message only
        if max_length < trunc_only_len:
            assert result == trunc_only, (
                f"Failed for {description}: max_length={max_length} < {trunc_only_len}, "
                f"expected message only, got: {result}"
            )
        # For max_length values that don't leave enough room for content (available < 7)
        elif max_length < min_length_for_content:
            assert result == trunc_only, (
                f"Failed for {description}: max_length={max_length} < {min_length_for_content}, "
                f"expected message only, got: {result}"
            )
        # For larger values, should show message + content
        else:
            # Should end with suffix
            assert result.endswith(suffix), f"Failed for {description}: result should end with suffix"
            # Total length should not exceed max_length
            assert len(result) <= max_length, (
                f"Failed for {description}: result length {len(result)} > max_length {max_length}"
            )


def test_truncate_rendered_value_exact_expected_output():
    """Test that truncation produces exact expected output: message first, then content when space allows."""
    prefix = "Truncated. You can change this behaviour in [core]max_templated_field_length. "
    suffix = "..."
    trunc_only = prefix + suffix

    test_cases = [
        (1, "test", trunc_only),
        (3, "test", trunc_only),
        (5, "test", trunc_only),
        (10, "password123", trunc_only),
        (20, "secret_value", trunc_only),
        (50, "This is a test string", trunc_only),
        (83, "Hello World", trunc_only),
        (84, "Hello World", trunc_only),
        (86, "Hello World", trunc_only),
        (90, "short", prefix + "short" + suffix),
        (100, "This is a longer string", prefix + "This is a longer st" + suffix),
        (150, "x" * 200, prefix + "x" * 69 + suffix),
        (100, "None", prefix + "None" + suffix),
        (100, "True", prefix + "True" + suffix),
        (100, "{'key': 'value'}", prefix + "{'key': 'value'}" + suffix),
        (100, "test's", prefix + "test's" + suffix),
        (90, '"quoted"', prefix + '"quoted"' + suffix),
    ]

    for max_length, rendered, expected in test_cases:
        result = truncate_rendered_value(rendered, max_length)
        assert result == expected, (
            f"max_length={max_length}, rendered={rendered!r}:\n"
            f"  expected: {expected!r}\n"
            f"  got:      {result!r}"
        )
