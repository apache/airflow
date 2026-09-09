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

import pytest

from airflow.providers.common.ai.sandbox.output import (
    format_size,
    render_file_window,
    truncate_output,
)


class TestFormatSize:
    @pytest.mark.parametrize(
        ("num_bytes", "expected"),
        [(0, "0B"), (512, "512B"), (1024, "1.0KB"), (1536, "1.5KB"), (5 * 1024 * 1024, "5.0MB")],
    )
    def test_renders_human_units(self, num_bytes, expected):
        assert format_size(num_bytes) == expected


class TestTruncateOutput:
    def test_short_output_is_untouched(self):
        assert truncate_output("a\nb", max_lines=10, max_bytes=100) == "a\nb"

    def test_empty_output_is_untouched(self):
        assert truncate_output("", max_lines=10, max_bytes=100) == ""

    def test_keeps_the_tail_not_the_head(self):
        # Arrange: the interesting part of command output is the end.
        text = "\n".join(f"line{i}" for i in range(10))

        # Act
        result = truncate_output(text, max_lines=3, max_bytes=1000)

        # Assert
        assert result.splitlines()[-3:] == ["line7", "line8", "line9"]
        assert "line0" not in result
        assert result.startswith("[... earlier output truncated]")

    def test_byte_cap_applies_before_line_cap_when_it_bites_first(self):
        text = "\n".join(["x" * 20] * 10)

        result = truncate_output(text, max_lines=100, max_bytes=45)

        # Two 20-byte lines plus one separator is 41; a third would exceed 45.
        assert len(result.splitlines()) == 3  # marker + 2 kept lines
        assert result.startswith("[... earlier output truncated]")

    def test_never_emits_a_partial_line(self):
        # Budget fits the 50-byte tail line but not the 5-byte head line plus its
        # separator, so the head is dropped whole rather than sliced.
        text = "short\n" + "y" * 50

        result = truncate_output(text, max_lines=10, max_bytes=52)

        assert "short" not in result
        assert result.splitlines()[-1] == "y" * 50

    def test_single_line_wider_than_the_cap_keeps_its_tail(self):
        # A giant final log record should still show its end, where the error is.
        result = truncate_output("A" * 10 + "B" * 10, max_lines=10, max_bytes=10)

        assert result.endswith("B" * 10)

    def test_backend_truncation_is_marked_even_when_the_remainder_fits(self):
        # The backend already dropped bytes, so the model must not be told this
        # is the whole output just because what survived fits under the cap.
        result = truncate_output("tail", max_lines=10, max_bytes=1000, already_truncated=True)

        assert result == "[... earlier output truncated]\ntail"


class TestRenderFileWindow:
    def test_small_file_is_returned_whole(self):
        result = render_file_window(b"a\nb\nc", offset=None, limit=None, max_lines=10, max_bytes=100)

        assert result == "a\nb\nc"

    def test_keeps_the_head_and_reports_a_continuation_offset(self):
        data = "\n".join(f"line{i}" for i in range(10)).encode()

        result = render_file_window(data, offset=None, limit=None, max_lines=3, max_bytes=1000)

        assert result.splitlines()[:3] == ["line0", "line1", "line2"]
        assert "read on with offset=4" in result
        assert "7 more lines" in result

    def test_offset_and_limit_select_a_window(self):
        data = "\n".join(f"line{i}" for i in range(10)).encode()

        result = render_file_window(data, offset=3, limit=2, max_lines=100, max_bytes=1000)

        assert result.splitlines()[:2] == ["line2", "line3"]

    def test_offset_past_the_end_says_so(self):
        result = render_file_window(b"a\nb", offset=99, limit=None, max_lines=10, max_bytes=100)

        assert "no lines at offset 99" in result
        assert "file has 2 lines" in result

    def test_first_line_wider_than_the_cap_points_at_the_shell(self):
        result = render_file_window(b"z" * 200, offset=None, limit=None, max_lines=10, max_bytes=50)

        assert "longer than the" in result
        assert "shell command" in result

    def test_invalid_utf8_is_replaced_not_raised(self):
        result = render_file_window(b"ok\n\xff\xfe", offset=None, limit=None, max_lines=10, max_bytes=100)

        assert result.startswith("ok")
