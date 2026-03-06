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

from airflow_shared.template_rendering import (
    TRUNCATE_MIN_CONTENT_LENGTH,
    TRUNCATE_PREFIX,
    TRUNCATE_SUFFIX,
    truncate_rendered_value,
)


def test_truncate_rendered_value_prioritizes_message():
    """Test that truncation message is always shown first, content only if space allows."""
    trunc_only = f"{TRUNCATE_PREFIX}{TRUNCATE_SUFFIX}"
    trunc_only_len = len(trunc_only)
    overhead = len(TRUNCATE_PREFIX) + len(TRUNCATE_SUFFIX)
    min_length_for_content = overhead + TRUNCATE_MIN_CONTENT_LENGTH

    test_cases = [
        (1, "test", "Minimum value"),
        (3, "test", "At ellipsis length"),
        (5, "test", "Very small"),
        (10, "password123", "Small"),
        (20, "secret_value", "Small with content"),
        (50, "This is a test string", "Medium"),
        (overhead + 1, "Hello World", "At prefix+suffix boundary v1"),
        (overhead + 2, "Hello World", "Just above boundary v1"),
        (min_length_for_content - 3, "Hello World", "At overhead boundary v2"),
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

    for max_length, rendered, description in test_cases:
        result = truncate_rendered_value(rendered, max_length)

        assert result.startswith(TRUNCATE_PREFIX), (
            f"Failed for {description}: result should start with prefix"
        )

        if max_length < trunc_only_len or max_length < min_length_for_content:
            assert result == trunc_only, (
                f"Failed for {description}: max_length={max_length}, expected message only, got: {result}"
            )
        else:
            assert result.endswith(TRUNCATE_SUFFIX), (
                f"Failed for {description}: result should end with suffix"
            )
            assert len(result) <= max_length, (
                f"Failed for {description}: result length {len(result)} > max_length {max_length}"
            )


def test_truncate_rendered_value_exact_expected_output():
    """Test that truncation produces exact expected output: message first, then content when space allows."""
    trunc_only = TRUNCATE_PREFIX + TRUNCATE_SUFFIX
    overhead = len(TRUNCATE_PREFIX) + len(TRUNCATE_SUFFIX)
    min_length_for_content = overhead + TRUNCATE_MIN_CONTENT_LENGTH

    test_cases = [
        (1, "test", trunc_only),
        (3, "test", trunc_only),
        (5, "test", trunc_only),
        (10, "password123", trunc_only),
        (20, "secret_value", trunc_only),
        (50, "This is a test string", trunc_only),
        (overhead + 1, "Hello World", trunc_only),
        (overhead + 2, "Hello World", trunc_only),
        (min_length_for_content - 3, "Hello World", trunc_only),
        (90, "short", TRUNCATE_PREFIX + "short" + TRUNCATE_SUFFIX),
        (100, "This is a longer string", TRUNCATE_PREFIX + "This is a longer st" + TRUNCATE_SUFFIX),
        (150, "x" * 200, TRUNCATE_PREFIX + "x" * 69 + TRUNCATE_SUFFIX),
        (100, "None", TRUNCATE_PREFIX + "None" + TRUNCATE_SUFFIX),
        (100, "True", TRUNCATE_PREFIX + "True" + TRUNCATE_SUFFIX),
        (100, "{'key': 'value'}", TRUNCATE_PREFIX + "{'key': 'value'}" + TRUNCATE_SUFFIX),
        (100, "test's", TRUNCATE_PREFIX + "test's" + TRUNCATE_SUFFIX),
        (90, '"quoted"', TRUNCATE_PREFIX + '"quoted"' + TRUNCATE_SUFFIX),
    ]

    for max_length, rendered, expected in test_cases:
        result = truncate_rendered_value(rendered, max_length)
        assert result == expected, (
            f"max_length={max_length}, rendered={rendered!r}:\n"
            f"  expected: {expected!r}\n"
            f"  got:      {result!r}"
        )
