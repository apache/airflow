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

import io

import pytest
from ci.prek import check_no_conventional_commit_message as hook


class TestIsConventionalCommit:
    @pytest.mark.parametrize(
        "subject",
        [
            "feat: add new operator",
            "fix: bug in scheduler",
            "fix(cli): dags test failure",
            "chore: bump deps",
            "docs: update readme",
            "refactor!: drop old api",
            "feat(scope)!: breaking change",
            "FEAT(api)!: shouting type",
            "Fix: capitalized type",
            "ci: tweak workflow",
            "test: add coverage",
            "revert: undo change",
            "build: tweak wheel",
            "perf: speed up parsing",
            "style: reformat",
            "fix/issue-66592: something",
            "Bugfix/make edge resilient to something",
        ],
    )
    def test_conventional_subjects_are_detected(self, subject):
        assert hook.is_conventional_commit(subject) is True

    @pytest.mark.parametrize(
        "subject",
        [
            "Fix airflow dags test command failure without serialized Dags",
            "UI: Fix Grid view not refreshing after task actions",
            "API: add new endpoint",
            "Add conventional commit",
            'Revert "Add something broken"',
            "Merge branch 'main' into feature",
            "Fixup test flakiness",
            # A non-Conventional-Commit type word before a colon must not match.
            "Scheduler: improve logging",
            # Looks close but no colon after the type.
            "fix the broken thing",
        ],
    )
    def test_plain_prose_subjects_are_allowed(self, subject):
        assert hook.is_conventional_commit(subject) is False


class TestGetSubjectLine:
    def test_first_non_comment_line_returned(self, tmp_path):
        path = tmp_path / "COMMIT_EDITMSG"
        path.write_text("\n# a comment\nfeat: do thing\n# trailing comment\n")
        assert hook.get_subject_line(str(path)) == "feat: do thing"

    def test_only_comments_and_blank_lines_returns_none(self, tmp_path):
        path = tmp_path / "COMMIT_EDITMSG"
        path.write_text("\n# only a comment\n\n")
        assert hook.get_subject_line(str(path)) is None


class TestMain:
    def test_commit_msg_file_with_conventional_subject_fails(self, tmp_path, capsys):
        path = tmp_path / "COMMIT_EDITMSG"
        path.write_text("feat: shiny new thing\n")
        assert hook.main([str(path)]) == 1
        assert "Conventional Commits" in capsys.readouterr().out

    def test_commit_msg_file_with_plain_subject_passes(self, tmp_path):
        path = tmp_path / "COMMIT_EDITMSG"
        path.write_text("Fix a real bug in the scheduler\n")
        assert hook.main([str(path)]) == 0

    def test_empty_commit_message_file_passes(self, tmp_path):
        path = tmp_path / "COMMIT_EDITMSG"
        path.write_text("\n# nothing here\n")
        assert hook.main([str(path)]) == 0

    def test_stdin_mode_flags_any_conventional_subject(self, monkeypatch, capsys):
        monkeypatch.setattr("sys.stdin", io.StringIO("Fix real bug\nfeat: shiny thing\nUI: tweak grid\n"))
        assert hook.main(["--from-stdin"]) == 1
        out = capsys.readouterr().out
        assert "feat: shiny thing" in out

    def test_stdin_mode_all_clean_passes(self, monkeypatch):
        monkeypatch.setattr("sys.stdin", io.StringIO("Fix real bug\nUI: tweak grid\nAPI: add endpoint\n"))
        assert hook.main(["--from-stdin"]) == 0
