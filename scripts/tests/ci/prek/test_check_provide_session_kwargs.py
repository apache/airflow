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

import ast
import os
import subprocess
import textwrap
from pathlib import Path

import pytest
from ci.prek import check_provide_session_kwargs as hook
from ci.prek.check_provide_session_kwargs import (
    ProvideSessionAllowlistManager,
    _check_provide_session_kwargs,
    _count_violations,
    _expand_for_allowlist_edits,
    _has_provide_session_decorator,
    _iter_positional_session_in_provide_session,
    _parse_tracked_allowlist,
    _session_is_positional,
)


@pytest.fixture
def find_violations(write_python_file):
    """Factory fixture: write code to a temp file and return positional-session violations."""

    def _check(code: str) -> list[tuple[ast.FunctionDef | ast.AsyncFunctionDef, ast.arg]]:
        path = write_python_file(code)
        return list(_iter_positional_session_in_provide_session(path))

    return _check


@pytest.fixture
def create_fake_repo(tmp_path, monkeypatch):
    """Create a fake repo layout and patch REPO_ROOT so paths resolve correctly."""
    monkeypatch.setattr(hook, "REPO_ROOT", tmp_path)

    def _write(rel: str, code: str) -> Path:
        path = tmp_path / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(code))
        return path

    return _write


@pytest.fixture
def create_git_repo(create_fake_repo, tmp_path):
    """Initialise ``tmp_path`` as a git repo so ``git show HEAD:<file>`` works.

    Returns a helper that commits the current working-tree contents under a given
    message, so tests can stage a "previous" allowlist at HEAD before mutating it.
    """
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@t",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@t",
    }

    def _run(*args: str) -> None:
        subprocess.run(["git", "-C", str(tmp_path), *args], check=True, env=env, capture_output=True)

    _run("init", "-q", "-b", "main")
    _run("config", "commit.gpgsign", "false")

    def _commit(message: str) -> None:
        _run("add", "-A")
        _run("commit", "-q", "--allow-empty", "-m", message)

    return _commit


class TestHasProvideSessionDecorator:
    def test_provide_session_name(self):
        func = ast.parse("@provide_session\ndef foo(): pass").body[0]
        assert _has_provide_session_decorator(func.decorator_list) is True

    def test_provide_session_attribute(self):
        func = ast.parse("@utils.provide_session\ndef foo(): pass").body[0]
        assert _has_provide_session_decorator(func.decorator_list) is True

    def test_no_decorator(self):
        func = ast.parse("def foo(): pass").body[0]
        assert _has_provide_session_decorator(func.decorator_list) is False

    def test_unrelated_decorator(self):
        func = ast.parse("@staticmethod\ndef foo(): pass").body[0]
        assert _has_provide_session_decorator(func.decorator_list) is False

    def test_multiple_decorators_including_provide_session(self):
        func = ast.parse("@staticmethod\n@provide_session\ndef foo(): pass").body[0]
        assert _has_provide_session_decorator(func.decorator_list) is True


class TestSessionIsPositional:
    def test_no_session_arg(self):
        func = ast.parse("def foo(x, y): pass").body[0]
        assert _session_is_positional(func.args) is None

    def test_session_positional(self):
        func = ast.parse("def foo(session=NEW_SESSION): pass").body[0]
        argument = _session_is_positional(func.args)
        assert argument is not None
        assert argument.arg == "session"

    def test_session_keyword_only(self):
        func = ast.parse("def foo(*, session=NEW_SESSION): pass").body[0]
        assert _session_is_positional(func.args) is None

    def test_session_positional_among_other_args(self):
        func = ast.parse("def foo(x, y, session=NEW_SESSION): pass").body[0]
        argument = _session_is_positional(func.args)
        assert argument is not None
        assert argument.arg == "session"

    def test_session_kwonly_after_other_positional(self):
        func = ast.parse("def foo(x, y, *, session=NEW_SESSION): pass").body[0]
        assert _session_is_positional(func.args) is None

    def test_session_positional_only(self):
        func = ast.parse("def foo(session, /, x): pass").body[0]
        argument = _session_is_positional(func.args)
        assert argument is not None
        assert argument.arg == "session"


class TestIterPositionalSessionInProvideSession:
    def test_keyword_only_session_is_clean(self, find_violations):
        code = """\
        @provide_session
        def foo(*, session=NEW_SESSION):
            pass
        """
        assert find_violations(code) == []

    def test_positional_session_is_flagged(self, find_violations):
        code = """\
        @provide_session
        def foo(session=NEW_SESSION):
            pass
        """
        violations = find_violations(code)
        assert len(violations) == 1
        func, argument = violations[0]
        assert func.name == "foo"
        assert argument.arg == "session"

    def test_no_provide_session_decorator_is_ignored(self, find_violations):
        code = """\
        def foo(session=NEW_SESSION):
            pass
        """
        assert find_violations(code) == []

    def test_async_function_with_positional_session_is_flagged(self, find_violations):
        code = """\
        @provide_session
        async def foo(session=NEW_SESSION):
            pass
        """
        violations = find_violations(code)
        assert len(violations) == 1

    def test_method_with_positional_session_is_flagged(self, find_violations):
        code = """\
        class C:
            @provide_session
            def foo(self, session=NEW_SESSION):
                pass
        """
        violations = find_violations(code)
        assert len(violations) == 1
        assert violations[0][0].name == "foo"

    def test_attribute_decorator_is_recognised(self, find_violations):
        code = """\
        @airflow.utils.session.provide_session
        def foo(session=NEW_SESSION):
            pass
        """
        violations = find_violations(code)
        assert len(violations) == 1

    def test_count_violations_multiple_in_file(self, write_python_file):
        code = """\
        @provide_session
        def a(session=NEW_SESSION):
            pass

        @provide_session
        def b(x, session=NEW_SESSION):
            pass

        @provide_session
        def c(*, session=NEW_SESSION):
            pass
        """
        path = write_python_file(code)
        assert _count_violations(path) == 2

    def test_syntax_error_returns_no_violations(self, write_python_file):
        path = write_python_file("def foo(:\n    pass")
        assert _count_violations(path) == 0

    def test_invalid_utf8_does_not_crash(self, tmp_path):
        path = tmp_path / "invalid_utf8.py"
        path.write_bytes(b"# bad byte: \xff\n@provide_session\ndef foo(session=NEW_SESSION):\n    pass\n")

        assert _count_violations(path) == 1


class TestProvideSessionAllowlistManager:
    def test_load_missing_file_returns_empty(self, tmp_path):
        manager = ProvideSessionAllowlistManager(tmp_path / "missing.txt")
        assert manager.load() == {}

    def test_save_and_load_round_trip(self, tmp_path):
        manager = ProvideSessionAllowlistManager(tmp_path / "allowlist.txt")
        manager.save({"b/file.py": 2, "a/file.py": 1})
        # Sorted by key in the file
        text = (tmp_path / "allowlist.txt").read_text()
        assert text.splitlines() == ["a/file.py::1", "b/file.py::2"]
        assert manager.load() == {"a/file.py": 1, "b/file.py": 2}

    def test_load_skips_blank_and_malformed_lines(self, tmp_path):
        path = tmp_path / "allowlist.txt"
        path.write_text("\nvalid/file.py::3\nnocount\n::5\nbad::notanumber\n")
        assert ProvideSessionAllowlistManager(path).load() == {"valid/file.py": 3}

    @pytest.mark.usefixtures("create_fake_repo")
    def test_load_skips_unsafe_entries(self, tmp_path):
        """Entries that escape REPO_ROOT (absolute paths or `..` segments) are ignored."""
        path = tmp_path / "allowlist.txt"
        path.write_text("airflow-core/src/airflow/safe.py::1\n../escape.py::1\n/etc/passwd::1\n")
        # `create_fake_repo` patches REPO_ROOT to tmp_path so the safety check is meaningful.
        assert ProvideSessionAllowlistManager(path).load() == {"airflow-core/src/airflow/safe.py": 1}


class TestCheckProvideSessionKwargs:
    def test_no_violations_in_clean_file(self, create_fake_repo, tmp_path):
        path = create_fake_repo(
            "airflow-core/src/airflow/clean.py",
            """\
            @provide_session
            def foo(*, session=NEW_SESSION):
                pass
            """,
        )
        manager = ProvideSessionAllowlistManager(tmp_path / "allowlist.txt")
        assert _check_provide_session_kwargs([path], {}, manager) == 0

    def test_new_violation_fails(self, create_fake_repo, tmp_path):
        path = create_fake_repo(
            "airflow-core/src/airflow/bad.py",
            """\
            @provide_session
            def foo(session=NEW_SESSION):
                pass
            """,
        )
        manager = ProvideSessionAllowlistManager(tmp_path / "allowlist.txt")
        assert _check_provide_session_kwargs([path], {}, manager) == 1

    def test_violation_within_allowlist_passes(self, create_fake_repo, tmp_path):
        path = create_fake_repo(
            "airflow-core/src/airflow/grandfathered.py",
            """\
            @provide_session
            def foo(session=NEW_SESSION):
                pass
            """,
        )
        manager = ProvideSessionAllowlistManager(tmp_path / "allowlist.txt")
        allowlist = {"airflow-core/src/airflow/grandfathered.py": 1}
        assert _check_provide_session_kwargs([path], allowlist, manager) == 0

    def test_exceeding_allowlist_fails(self, create_fake_repo, tmp_path):
        path = create_fake_repo(
            "airflow-core/src/airflow/grew.py",
            """\
            @provide_session
            def a(session=NEW_SESSION):
                pass

            @provide_session
            def b(session=NEW_SESSION):
                pass
            """,
        )
        manager = ProvideSessionAllowlistManager(tmp_path / "allowlist.txt")
        allowlist = {"airflow-core/src/airflow/grew.py": 1}
        assert _check_provide_session_kwargs([path], allowlist, manager) == 1

    def test_reducing_violations_tightens_allowlist(self, create_fake_repo, tmp_path):
        path = create_fake_repo(
            "airflow-core/src/airflow/improved.py",
            """\
            @provide_session
            def foo(session=NEW_SESSION):
                pass

            @provide_session
            def bar(*, session=NEW_SESSION):
                pass
            """,
        )
        manager = ProvideSessionAllowlistManager(tmp_path / "allowlist.txt")
        allowlist = {"airflow-core/src/airflow/improved.py": 2}
        # Exit non-zero so pre-commit reports the modified allowlist
        assert _check_provide_session_kwargs([path], allowlist, manager) == 1
        assert manager.load() == {"airflow-core/src/airflow/improved.py": 1}

    def test_fixing_all_violations_removes_entry(self, create_fake_repo, tmp_path):
        path = create_fake_repo(
            "airflow-core/src/airflow/fixed.py",
            """\
            @provide_session
            def foo(*, session=NEW_SESSION):
                pass
            """,
        )
        manager = ProvideSessionAllowlistManager(tmp_path / "allowlist.txt")
        allowlist = {"airflow-core/src/airflow/fixed.py": 1}
        assert _check_provide_session_kwargs([path], allowlist, manager) == 1
        assert manager.load() == {}

    def test_non_python_file_is_skipped(self, create_fake_repo, tmp_path):
        path = create_fake_repo(
            "airflow-core/src/airflow/not_python.txt", "@provide_session\ndef foo(session=N): pass\n"
        )
        manager = ProvideSessionAllowlistManager(tmp_path / "allowlist.txt")
        assert _check_provide_session_kwargs([path], {}, manager) == 0

    @pytest.mark.usefixtures("create_fake_repo")
    def test_missing_allowlist_file_fails_loudly(self, tmp_path):
        """Passing the allowlist path when the file is missing must fail, not silently pass."""
        allowlist_path = tmp_path / "allowlist.txt"
        manager = ProvideSessionAllowlistManager(allowlist_path)
        assert not allowlist_path.exists()
        assert _check_provide_session_kwargs([allowlist_path.resolve()], {}, manager) == 1


class TestExpandForAllowlistEdits:
    def test_unchanged_when_allowlist_not_in_paths(self, create_fake_repo, tmp_path):
        py = create_fake_repo("airflow-core/src/airflow/x.py", "pass")
        manager = ProvideSessionAllowlistManager(tmp_path / "allowlist.txt")
        assert _expand_for_allowlist_edits([py], manager, {"airflow-core/src/airflow/x.py": 1}) == [py]

    def test_appends_allowlisted_files_when_allowlist_edited(self, create_fake_repo, tmp_path):
        allowlist_path = tmp_path / "allowlist.txt"
        manager = ProvideSessionAllowlistManager(allowlist_path)
        listed = create_fake_repo("airflow-core/src/airflow/listed.py", "pass")
        # Pass a resolved path — matches production behavior (``main()`` resolves argv).
        result = _expand_for_allowlist_edits(
            [allowlist_path.resolve()],
            manager,
            {"airflow-core/src/airflow/listed.py": 1, "airflow-core/src/airflow/gone.py": 1},
        )
        assert allowlist_path.resolve() in result
        assert listed in result
        # File in allowlist that does not exist on disk should be ignored.
        assert (tmp_path / "airflow-core/src/airflow/gone.py").resolve() not in result

    def test_detection_robust_to_symlinked_allowlist(self, create_fake_repo, tmp_path):
        """A symlink pointing at the allowlist file must still trigger expansion."""
        allowlist_path = tmp_path / "allowlist.txt"
        manager = ProvideSessionAllowlistManager(allowlist_path)
        listed = create_fake_repo("airflow-core/src/airflow/listed.py", "pass")
        manager.save({"airflow-core/src/airflow/listed.py": 1})

        symlink = tmp_path / "allowlist_link.txt"
        symlink.symlink_to(allowlist_path)

        # Production resolves argv before calling the helper — a symlinked path resolves
        # to the real allowlist file and must be recognised as an allowlist edit.
        result = _expand_for_allowlist_edits([symlink.resolve()], manager, manager.load())

        assert listed in result

    def test_includes_parse_tracked_allowlist_entries_when_removed(
        self, create_fake_repo, create_git_repo, tmp_path
    ):
        """Removing an entry from the allowlist must still re-check the previously-listed file."""
        rel = "airflow-core/src/airflow/dropped.py"
        create_fake_repo(
            rel,
            """\
            @provide_session
            def foo(session=NEW_SESSION):
                pass
            """,
        )
        allowlist_path = tmp_path / "allowlist.txt"
        manager = ProvideSessionAllowlistManager(allowlist_path)
        manager.save({rel: 1})
        create_git_repo("seed allowlist at HEAD")

        # Working tree: remove the entry, but the offending file still exists.
        allowlist_path.write_text("")
        current = manager.load()
        assert current == {}

        expanded = _expand_for_allowlist_edits([allowlist_path.resolve()], manager, current)
        # The previously-listed file must be re-validated.
        assert (tmp_path / rel).resolve() in expanded

        # And the full check should fail because the file still has positional sessions.
        assert _check_provide_session_kwargs(expanded, current, manager) == 1

    @pytest.mark.usefixtures("create_fake_repo")
    def test_parse_tracked_allowlist_empty_when_no_git_history(self, tmp_path):
        """Without a git repo the git-tracked allowlist lookup returns empty and does not crash."""
        manager = ProvideSessionAllowlistManager(tmp_path / "allowlist.txt")
        assert _parse_tracked_allowlist(manager) == {}

    def test_re_validates_listed_files_so_loosening_cannot_bypass(self, create_fake_repo, tmp_path, capsys):
        """Editing only the allowlist must still trigger validation of listed files."""
        rel = "airflow-core/src/airflow/loosened.py"
        create_fake_repo(
            rel,
            """\
            @provide_session
            def foo(session=NEW_SESSION):
                pass

            @provide_session
            def bar(session=NEW_SESSION):
                pass
            """,
        )
        allowlist_path = tmp_path / "allowlist.txt"
        manager = ProvideSessionAllowlistManager(allowlist_path)
        # Allowlist loosened to 5 although file only has 2 positional sessions.
        allowlist = {rel: 5}
        manager.save(allowlist)

        # Only the allowlist file is "changed"; without re-validation this would return 0.
        # Resolve the path to mirror what ``main()`` does in production.
        paths = _expand_for_allowlist_edits([allowlist_path.resolve()], manager, allowlist)
        rc = _check_provide_session_kwargs(paths, allowlist, manager)

        # Tightened from 5 -> 2, so the hook exits non-zero to surface the modified allowlist.
        assert rc == 1
        assert manager.load() == {rel: 2}


class TestCleanup:
    def test_cleanup_removes_stale_entries(self, create_fake_repo, tmp_path):
        create_fake_repo("airflow-core/src/airflow/keeper.py", "pass")
        allowlist_path = tmp_path / "allowlist.txt"
        manager = ProvideSessionAllowlistManager(allowlist_path)
        manager.save(
            {
                "airflow-core/src/airflow/keeper.py": 1,
                "airflow-core/src/airflow/gone.py": 1,
            }
        )
        assert manager.cleanup() == 0
        assert manager.load() == {"airflow-core/src/airflow/keeper.py": 1}

    def test_cleanup_empty_allowlist(self, tmp_path):
        manager = ProvideSessionAllowlistManager(tmp_path / "allowlist.txt")
        assert manager.cleanup() == 0
