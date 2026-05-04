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
from ci.prek.newsfragments import VALID_CHANGE_TYPES, validate_newsfragment

ALL_CHANGE_TYPES = sorted(VALID_CHANGE_TYPES)
NON_SIGNIFICANT_CHANGE_TYPES = sorted(VALID_CHANGE_TYPES - {"significant"})


class TestNewsfragmentFilenameValidation:
    @pytest.mark.parametrize("change_type", ALL_CHANGE_TYPES)
    def test_valid_filename_all_types(self, change_type):
        errors = validate_newsfragment(f"12345.{change_type}.rst", ["A change"])
        assert errors == []

    def test_too_few_parts(self):
        errors = validate_newsfragment("12345.rst", ["A change"])
        assert len(errors) == 1
        assert "unexpected filename" in errors[0]

    def test_too_many_parts(self):
        errors = validate_newsfragment("12345.bugfix.extra.rst", ["A change"])
        assert len(errors) == 1
        assert "unexpected filename" in errors[0]

    def test_invalid_change_type(self):
        errors = validate_newsfragment("12345.invalid.rst", ["A change"])
        assert len(errors) == 1
        assert "unexpected type" in errors[0]


class TestNewsfragmentContentValidation:
    @pytest.mark.parametrize("change_type", NON_SIGNIFICANT_CHANGE_TYPES)
    def test_non_significant_single_line_ok(self, change_type):
        errors = validate_newsfragment(f"123.{change_type}.rst", ["Fix something"])
        assert errors == []

    @pytest.mark.parametrize("change_type", NON_SIGNIFICANT_CHANGE_TYPES)
    def test_non_significant_multi_line_fails(self, change_type):
        errors = validate_newsfragment(f"123.{change_type}.rst", ["Fix something", "More details"])
        assert len(errors) == 1
        assert "single line" in errors[0]

    def test_significant_single_line_ok(self):
        errors = validate_newsfragment("123.significant.rst", ["Big change"])
        assert errors == []

    def test_significant_two_lines_fails(self):
        errors = validate_newsfragment("123.significant.rst", ["Big change", "Second line"])
        assert len(errors) == 1
        assert "1, or 3+ lines" in errors[0]

    def test_significant_three_lines_with_blank_second_ok(self):
        errors = validate_newsfragment("123.significant.rst", ["Big change", "", "Details here"])
        assert errors == []

    def test_significant_three_lines_without_blank_second_fails(self):
        errors = validate_newsfragment("123.significant.rst", ["Big change", "Not blank", "Details"])
        assert len(errors) == 1
        assert "empty second line" in errors[0]

    def test_significant_many_lines_ok(self):
        lines = ["Big change", "", "Details here", "More details", "Even more"]
        errors = validate_newsfragment("123.significant.rst", lines)
        assert errors == []
