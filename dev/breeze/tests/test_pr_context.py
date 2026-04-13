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

from airflow_breeze.utils.pr_context import (
    extract_cross_references,
    extract_file_paths_from_diff,
    find_overlapping_prs,
)


class TestExtractFilePathsFromDiff:
    def test_basic_diff(self):
        diff = (
            "diff --git a/src/foo.py b/src/foo.py\n"
            "index abc..def 100644\n"
            "--- a/src/foo.py\n"
            "+++ b/src/foo.py\n"
            "@@ -1,3 +1,4 @@\n"
            " line1\n"
            "+line2\n"
            "diff --git a/src/bar.py b/src/bar.py\n"
            "index ghi..jkl 100644\n"
        )
        assert extract_file_paths_from_diff(diff) == ["src/foo.py", "src/bar.py"]

    def test_deduplicates(self):
        diff = "diff --git a/x.py b/x.py\ndiff --git a/x.py b/x.py\n"
        assert extract_file_paths_from_diff(diff) == ["x.py"]

    def test_empty_diff(self):
        assert extract_file_paths_from_diff("") == []

    def test_rename(self):
        diff = "diff --git a/old/path.py b/new/path.py\n"
        assert extract_file_paths_from_diff(diff) == ["old/path.py"]


class TestExtractCrossReferences:
    def test_basic_refs(self):
        body = "This fixes #123 and relates to #456."
        assert extract_cross_references(body) == [123, 456]

    def test_excludes_self(self):
        body = "See #100 and #200"
        assert extract_cross_references(body, exclude_number=100) == [200]

    def test_deduplicates(self):
        body = "Fixes #123. Also see #123 again."
        assert extract_cross_references(body) == [123]

    def test_no_refs(self):
        assert extract_cross_references("Just a plain description.") == []

    def test_ignores_anchors(self):
        body = "See [link](#section) and #42"
        # #section is not a number so only #42 matches
        assert extract_cross_references(body) == [42]

    def test_ignores_mid_word(self):
        body = "color#123 should not match but #456 should"
        assert extract_cross_references(body) == [456]


class TestFindOverlappingPrs:
    def test_basic_overlap(self):
        target_files = ["src/foo.py", "src/bar.py"]
        other_prs = {
            200: ["src/foo.py", "src/baz.py"],
            300: ["src/qux.py"],
            400: ["src/bar.py", "src/foo.py"],
        }
        result = find_overlapping_prs(target_files, 100, other_prs)
        assert 200 in result
        assert 400 in result
        assert 300 not in result
        # PR 400 has 2 overlapping files, should come first
        assert next(iter(result.keys())) == 400

    def test_excludes_self(self):
        result = find_overlapping_prs(["a.py"], 100, {100: ["a.py"]})
        assert result == {}

    def test_empty_files(self):
        assert find_overlapping_prs([], 100, {200: ["a.py"]}) == {}

    def test_no_overlap(self):
        result = find_overlapping_prs(["a.py"], 100, {200: ["b.py"]})
        assert result == {}
