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

import json
import urllib.error
from email.message import Message
from unittest import mock

import pytest
from ci.prek import newsfragments
from ci.prek.newsfragments import (
    VALID_CHANGE_TYPES,
    check_pr_number,
    find_misnumbered_newsfragments,
    main,
    validate_newsfragment,
)

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


class TestNewsfragmentPrNumber:
    @pytest.mark.parametrize(
        ("filenames", "expected"),
        [
            pytest.param(["airflow-core/newsfragments/123.bugfix.rst"], [], id="matching"),
            pytest.param(
                ["airflow-core/newsfragments/124.bugfix.rst"],
                ["airflow-core/newsfragments/124.bugfix.rst"],
                id="other-number",
            ),
            pytest.param(
                ["airflow-core/newsfragments/1234.bugfix.rst"],
                ["airflow-core/newsfragments/1234.bugfix.rst"],
                id="number-with-the-same-prefix",
            ),
            pytest.param(["airflow-core/docs/124.bugfix.rst", "README.md"], [], id="not-a-newsfragment"),
        ],
    )
    def test_find_misnumbered_newsfragments(self, filenames, expected):
        assert find_misnumbered_newsfragments("123", filenames) == expected

    @mock.patch.object(newsfragments, "get_github_items", autospec=True)
    def test_check_pr_number_ignores_removed_files(self, mock_get_items):
        mock_get_items.side_effect = [
            [{"name": "area:core"}],
            [
                {"filename": "airflow-core/newsfragments/124.bugfix.rst", "status": "added"},
                {"filename": "airflow-core/newsfragments/99.bugfix.rst", "status": "removed"},
            ],
        ]

        errors = check_pr_number("123")

        assert len(errors) == 1
        assert "airflow-core/newsfragments/124.bugfix.rst" in errors[0]
        assert mock_get_items.call_args_list == [
            mock.call("repos/apache/airflow/issues/123/labels"),
            mock.call("repos/apache/airflow/pulls/123/files"),
        ]

    @mock.patch.object(newsfragments, "get_github_items", autospec=True)
    def test_check_pr_number_is_skipped_by_label(self, mock_get_items):
        mock_get_items.return_value = [{"name": "skip newsfragment check"}]

        assert check_pr_number("123") == []
        mock_get_items.assert_called_once()

    @pytest.mark.parametrize(
        ("pr_number", "expected_calls", "expected_exit"),
        [
            pytest.param(None, [], 0, id="local-run-accepts-any-number"),
            pytest.param("123", [mock.call("123")], 1, id="ci"),
        ],
    )
    @mock.patch.object(newsfragments, "check_pr_number", autospec=True, return_value=["wrong number"])
    def test_main_checks_pr_number_only_in_ci(
        self, mock_check, monkeypatch, tmp_path, pr_number, expected_calls, expected_exit
    ):
        if pr_number:
            monkeypatch.setenv("PR_NUMBER", pr_number)
        else:
            monkeypatch.delenv("PR_NUMBER", raising=False)
        newsfragment = tmp_path / "newsfragments" / "124.bugfix.rst"
        newsfragment.parent.mkdir()
        newsfragment.write_text("A change\n")

        assert main([str(newsfragment)]) == expected_exit
        assert mock_check.call_args_list == expected_calls

    @pytest.mark.parametrize(
        "error",
        [
            pytest.param(urllib.error.URLError("no network"), id="url-error"),
            pytest.param(
                urllib.error.HTTPError("https://api.github.com", 403, "rate limited", Message(), None),
                id="http-error",
            ),
        ],
    )
    @mock.patch.object(newsfragments, "check_pr_number", autospec=True)
    def test_main_reports_an_unreadable_pull_request(self, mock_check, monkeypatch, capsys, error):
        monkeypatch.setenv("PR_NUMBER", "123")
        mock_check.side_effect = error

        assert main([]) == 1
        assert "Could not read pull request 123 from GitHub" in capsys.readouterr().out

    @pytest.mark.parametrize(
        ("token", "expected_authorization"),
        [pytest.param(None, None, id="anonymous"), pytest.param("secret", "Bearer secret", id="token")],
    )
    @mock.patch.object(newsfragments.urllib.request, "urlopen", autospec=True)
    def test_get_github_items_reads_every_page(
        self, mock_urlopen, monkeypatch, token, expected_authorization
    ):
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        if token:
            monkeypatch.setenv("GITHUB_TOKEN", token)
        pages = [[{"n": i} for i in range(100)], [{"n": 100}]]
        mock_urlopen.return_value.__enter__.side_effect = [
            mock.Mock(read=mock.Mock(return_value=json.dumps(page).encode())) for page in pages
        ]

        items = newsfragments.get_github_items("repos/apache/airflow/pulls/1/files")

        assert len(items) == 101
        requests = [call.args[0] for call in mock_urlopen.call_args_list]
        assert [r.full_url for r in requests] == [
            "https://api.github.com/repos/apache/airflow/pulls/1/files?per_page=100&page=1",
            "https://api.github.com/repos/apache/airflow/pulls/1/files?per_page=100&page=2",
        ]
        assert requests[0].get_header("Authorization") == expected_authorization
