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
from ci.prek.changelog_duplicates import extract_pr_numbers, find_duplicates, known_exceptions


class TestExtractPrNumbers:
    @pytest.mark.parametrize(
        "line, expected_prs",
        [
            ("* Fix something (#12345)", ["12345"]),
            ("* Fix something (#1)", ["1"]),
            ("* Fix something (#123456)", ["123456"]),
            ("Some change (#99999)`", ["99999"]),
            ("Some change (#99999)``", ["99999"]),
            ("* ``Fix something (#12345, #67890)``", ["12345", "67890"]),
            ("* Fix something (#1, #2, #3)", ["1", "2", "3"]),
        ],
    )
    def test_extracts_pr_numbers(self, line, expected_prs):
        assert extract_pr_numbers(line) == expected_prs

    @pytest.mark.parametrize(
        "line",
        [
            "* Fix something without PR number",
            "* Fix something (#1234567)",  # 7 digits, too many
            "* Fix something (#abc)",
            "",
            "Just some text",
        ],
    )
    def test_no_match(self, line):
        assert extract_pr_numbers(line) == []


class TestFindDuplicates:
    def test_no_duplicates(self):
        lines = [
            "* Fix A (#1001)",
            "* Fix B (#1002)",
            "* Fix C (#1003)",
        ]
        assert find_duplicates(lines) == []

    def test_with_duplicate(self):
        lines = [
            "* Fix A (#1001)",
            "* Fix B (#1001)",
        ]
        assert find_duplicates(lines) == ["1001"]

    def test_known_exception_not_reported(self):
        lines = [
            "* Fix A (#14738)",
            "* Fix B (#14738)",
        ]
        assert find_duplicates(lines) == []

    def test_mixed_lines(self):
        lines = [
            "# Changelog",
            "",
            "* Fix A (#1001)",
            "Some description",
            "* Fix B (#1002)",
        ]
        assert find_duplicates(lines) == []

    def test_multiple_duplicates(self):
        lines = [
            "* Fix A (#1001)",
            "* Fix B (#1002)",
            "* Fix C (#1001)",
            "* Fix D (#1002)",
        ]
        assert find_duplicates(lines) == ["1001", "1002"]

    def test_empty_input(self):
        assert find_duplicates([]) == []

    @pytest.mark.parametrize(
        "lines, expected",
        [
            (["* Fix A (#1001, #1002)", "* Fix B (#1002)"], ["1002"]),
            (["* Fix A (#1001, #1002)", "* Fix B (#1001, #1003)"], ["1001"]),
        ],
    )
    def test_grouped_pr_numbers(self, lines, expected):
        assert find_duplicates(lines) == expected

    def test_all_known_exceptions_are_strings(self):
        for exc in known_exceptions:
            assert isinstance(exc, str)
            assert exc.isdigit()
