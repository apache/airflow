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

from unittest.mock import patch

import pytest

from airflow_breeze.commands.release_command import find_latest_release_candidate


class TestFindLatestReleaseCandidate:
    """Test the find_latest_release_candidate function."""

    def test_find_latest_rc_single_candidate(self, tmp_path):
        """Test finding release candidate when only one exists."""
        svn_dev_repo = tmp_path / "dev" / "airflow"
        svn_dev_repo.mkdir(parents=True)

        # Create a single RC directory
        (svn_dev_repo / "3.0.5rc1").mkdir()

        result = find_latest_release_candidate("3.0.5", str(svn_dev_repo), component="airflow")
        assert result == "3.0.5rc1"

    def test_find_latest_rc_multiple_candidates(self, tmp_path):
        """Test finding latest release candidate when multiple exist."""
        svn_dev_repo = tmp_path / "dev" / "airflow"
        svn_dev_repo.mkdir(parents=True)

        # Create multiple RC directories
        (svn_dev_repo / "3.0.5rc1").mkdir()
        (svn_dev_repo / "3.0.5rc2").mkdir()
        (svn_dev_repo / "3.0.5rc3").mkdir()
        (svn_dev_repo / "3.0.5rc10").mkdir()  # Test that rc10 > rc3

        result = find_latest_release_candidate("3.0.5", str(svn_dev_repo), component="airflow")
        assert result == "3.0.5rc10"

    def test_find_latest_rc_ignores_other_versions(self, tmp_path):
        """Test that function ignores RCs for other versions."""
        svn_dev_repo = tmp_path / "dev" / "airflow"
        svn_dev_repo.mkdir(parents=True)

        # Create RCs for different versions
        (svn_dev_repo / "3.0.4rc1").mkdir()
        (svn_dev_repo / "3.0.5rc1").mkdir()
        (svn_dev_repo / "3.0.5rc2").mkdir()
        (svn_dev_repo / "3.0.6rc1").mkdir()

        result = find_latest_release_candidate("3.0.5", str(svn_dev_repo), component="airflow")
        assert result == "3.0.5rc2"

    def test_find_latest_rc_ignores_non_rc_directories(self, tmp_path):
        """Test that function ignores directories that don't match RC pattern."""
        svn_dev_repo = tmp_path / "dev" / "airflow"
        svn_dev_repo.mkdir(parents=True)

        # Create RC directory and non-RC directories
        (svn_dev_repo / "3.0.5rc1").mkdir()
        (svn_dev_repo / "3.0.5").mkdir()  # Final release directory
        (svn_dev_repo / "some-other-dir").mkdir()

        result = find_latest_release_candidate("3.0.5", str(svn_dev_repo), component="airflow")
        assert result == "3.0.5rc1"

    def test_find_latest_rc_no_match(self, tmp_path):
        """Test that function returns None when no matching RC found."""
        svn_dev_repo = tmp_path / "dev" / "airflow"
        svn_dev_repo.mkdir(parents=True)

        # Create RCs for different version
        (svn_dev_repo / "3.0.4rc1").mkdir()

        result = find_latest_release_candidate("3.0.5", str(svn_dev_repo), component="airflow")
        assert result is None

    def test_find_latest_rc_directory_not_exists(self, tmp_path):
        """Test that function returns None when directory doesn't exist."""
        svn_dev_repo = tmp_path / "dev" / "airflow"
        # Don't create the directory

        result = find_latest_release_candidate("3.0.5", str(svn_dev_repo), component="airflow")
        assert result is None

    def test_find_latest_rc_empty_directory(self, tmp_path):
        """Test that function returns None when directory is empty."""
        svn_dev_repo = tmp_path / "dev" / "airflow"
        svn_dev_repo.mkdir(parents=True)

        result = find_latest_release_candidate("3.0.5", str(svn_dev_repo), component="airflow")
        assert result is None

    def test_find_latest_rc_task_sdk_component(self, tmp_path):
        """Test finding release candidate for task-sdk component."""
        svn_dev_repo = tmp_path / "dev" / "airflow"
        task_sdk_dir = svn_dev_repo / "task-sdk"
        task_sdk_dir.mkdir(parents=True)

        # Create multiple Task SDK RC directories
        (task_sdk_dir / "1.0.5rc1").mkdir()
        (task_sdk_dir / "1.0.5rc2").mkdir()
        (task_sdk_dir / "1.0.5rc3").mkdir()

        result = find_latest_release_candidate("1.0.5", str(svn_dev_repo), component="task-sdk")
        assert result == "1.0.5rc3"

    def test_find_latest_rc_task_sdk_ignores_airflow_rcs(self, tmp_path):
        """Test that task-sdk component ignores airflow RCs."""
        svn_dev_repo = tmp_path / "dev" / "airflow"
        svn_dev_repo.mkdir(parents=True)
        task_sdk_dir = svn_dev_repo / "task-sdk"
        task_sdk_dir.mkdir()

        # Create airflow RC (should be ignored)
        (svn_dev_repo / "3.0.5rc1").mkdir()
        # Create task-sdk RC
        (task_sdk_dir / "1.0.5rc1").mkdir()

        result = find_latest_release_candidate("1.0.5", str(svn_dev_repo), component="task-sdk")
        assert result == "1.0.5rc1"

    def test_find_latest_rc_handles_oserror(self, tmp_path):
        """Test that function handles OSError gracefully."""
        svn_dev_repo = tmp_path / "dev" / "airflow"
        svn_dev_repo.mkdir(parents=True)

        with patch("os.listdir", side_effect=OSError("Permission denied")):
            result = find_latest_release_candidate("3.0.5", str(svn_dev_repo), component="airflow")
            assert result is None


class FakeDirEntry:
    def __init__(self, name: str, *, is_dir: bool):
        self.name = name
        self._is_dir = is_dir

    def is_dir(self) -> bool:
        return self._is_dir


@pytest.fixture
def release_cmd():
    """Lazy import the release command module."""
    import airflow_breeze.commands.release_command as module

    return module


def test_remove_old_release_only_collects_release_directories(monkeypatch, release_cmd):
    version = "3.0.5"
    task_sdk_version = "1.0.5"
    svn_release_repo = "/svn/release/repo"

    # Arrange: entries include current release, old release directories, a matching "file", and non-release directory.
    entries = [
        FakeDirEntry(version, is_dir=True),  # current release: should be skipped
        FakeDirEntry("3.0.4", is_dir=True),  # old release dir: should be included
        FakeDirEntry("3.0.3", is_dir=True),  # old release dir: should be included
        FakeDirEntry("3.0.2", is_dir=False),  # matches pattern but not a directory: excluded
        FakeDirEntry("task-sdk", is_dir=True),  # task-sdk directory: will be scanned separately
        FakeDirEntry("not-a-release", is_dir=True),  # directory but not matching pattern: excluded
    ]

    # Task SDK directory entries
    task_sdk_entries = [
        FakeDirEntry(task_sdk_version, is_dir=True),  # current task-sdk release: should be skipped
        FakeDirEntry("1.0.4", is_dir=True),  # old task-sdk release: should be included
        FakeDirEntry("1.0.3", is_dir=True),  # old task-sdk release: should be included
    ]

    chdir_calls: list[str] = []
    console_messages: list[str] = []
    run_command_calls: list[list[str]] = []
    getcwd_calls: list[int] = []
    path_exists_calls: list[str] = []

    def fake_confirm_action(prompt: str, **_kwargs) -> bool:
        # First prompt decides whether we scan. We want to.
        if prompt == "Do you want to look for old releases to remove?":
            return True
        # For each candidate, we decline removal to avoid running svn commands.
        if prompt.startswith("Remove old release "):
            return False
        raise AssertionError(f"Unexpected confirm prompt: {prompt}")

    def fake_getcwd() -> str:
        getcwd_calls.append(1)
        return "/original/dir"

    def fake_path_exists(path: str) -> bool:
        path_exists_calls.append(path)
        return path == "/svn/release/repo/task-sdk"

    def fake_scandir(path=None):
        if path == "/svn/release/repo/task-sdk":
            return iter(task_sdk_entries)
        return iter(entries)

    monkeypatch.setattr(release_cmd.os, "getcwd", fake_getcwd)
    monkeypatch.setattr(release_cmd.os, "chdir", lambda p: chdir_calls.append(p))
    monkeypatch.setattr(release_cmd.os, "scandir", fake_scandir)
    monkeypatch.setattr(release_cmd.os.path, "exists", fake_path_exists)
    monkeypatch.setattr(release_cmd.os.path, "join", lambda *args: "/".join(args))
    monkeypatch.setattr(release_cmd, "confirm_action", fake_confirm_action)
    monkeypatch.setattr(release_cmd, "console_print", lambda msg="": console_messages.append(str(msg)))
    monkeypatch.setattr(release_cmd, "run_command", lambda cmd, **_kwargs: run_command_calls.append(cmd))

    # Act
    release_cmd.remove_old_release(
        version=version, task_sdk_version=task_sdk_version, svn_release_repo=svn_release_repo
    )

    # Assert: only directory entries matching RELEASE_PATTERN, excluding current version, and sorted.
    assert svn_release_repo in chdir_calls
    assert "/original/dir" in chdir_calls
    assert "The following old Airflow releases should be removed: ['3.0.3', '3.0.4']" in console_messages
    assert (
        "The following old Task SDK releases should be removed: ['task-sdk/1.0.3', 'task-sdk/1.0.4']"
        in console_messages
    )
    assert run_command_calls == []


def test_remove_old_release_returns_early_when_user_declines(monkeypatch, release_cmd):
    version = "3.0.5"
    task_sdk_version = "1.0.5"
    svn_release_repo = "/svn/release/repo"

    confirm_prompts: list[str] = []

    def fake_confirm_action(prompt: str, **_kwargs) -> bool:
        confirm_prompts.append(prompt)
        return False

    def should_not_be_called(*_args, **_kwargs):
        raise AssertionError("This should not have been called when user declines the initial prompt.")

    monkeypatch.setattr(release_cmd, "confirm_action", fake_confirm_action)
    monkeypatch.setattr(release_cmd.os, "getcwd", should_not_be_called)
    monkeypatch.setattr(release_cmd.os, "chdir", should_not_be_called)
    monkeypatch.setattr(release_cmd.os, "scandir", should_not_be_called)
    monkeypatch.setattr(release_cmd, "console_print", should_not_be_called)
    monkeypatch.setattr(release_cmd, "run_command", should_not_be_called)

    release_cmd.remove_old_release(
        version=version, task_sdk_version=task_sdk_version, svn_release_repo=svn_release_repo
    )

    assert confirm_prompts == ["Do you want to look for old releases to remove?"]


def test_remove_old_release_removes_confirmed_old_releases(monkeypatch, release_cmd):
    version = "3.1.5"
    task_sdk_version = "1.1.5"
    svn_release_repo = "/svn/release/repo"

    entries = [
        FakeDirEntry("3.1.4", is_dir=True),
        FakeDirEntry(version, is_dir=True),
        FakeDirEntry("3.1.0", is_dir=True),
    ]

    task_sdk_entries = [
        FakeDirEntry("1.1.4", is_dir=True),
        FakeDirEntry(task_sdk_version, is_dir=True),
        FakeDirEntry("1.1.0", is_dir=True),
    ]

    chdir_calls: list[str] = []
    console_messages: list[str] = []
    run_command_calls: list[tuple[list[str], dict]] = []
    confirm_prompts: list[str] = []
    getcwd_calls: list[int] = []

    def fake_confirm_action(prompt: str, **_kwargs) -> bool:
        confirm_prompts.append(prompt)
        if prompt == "Do you want to look for old releases to remove?":
            return True
        if prompt == "Remove old release 3.1.0?":
            return True
        if prompt == "Remove old release 3.1.4?":
            return False
        if prompt.startswith("Remove old release task-sdk/"):
            return False
        raise AssertionError(f"Unexpected confirm prompt: {prompt}")

    def fake_getcwd() -> str:
        getcwd_calls.append(1)
        return "/original/dir"

    def fake_path_exists(path: str) -> bool:
        return path == "/svn/release/repo/task-sdk"

    def fake_scandir(path=None):
        if path == "/svn/release/repo/task-sdk":
            return iter(task_sdk_entries)
        return iter(entries)

    monkeypatch.setattr(release_cmd.os, "getcwd", fake_getcwd)
    monkeypatch.setattr(release_cmd.os, "chdir", lambda path: chdir_calls.append(path))
    monkeypatch.setattr(release_cmd.os, "scandir", fake_scandir)
    monkeypatch.setattr(release_cmd.os.path, "exists", fake_path_exists)
    monkeypatch.setattr(release_cmd.os.path, "join", lambda *args: "/".join(args))
    monkeypatch.setattr(release_cmd, "confirm_action", fake_confirm_action)
    monkeypatch.setattr(release_cmd, "console_print", lambda msg="": console_messages.append(str(msg)))

    def fake_run_command(cmd: list[str], **kwargs):
        run_command_calls.append((cmd, kwargs))

    monkeypatch.setattr(release_cmd, "run_command", fake_run_command)

    release_cmd.remove_old_release(
        version=version, task_sdk_version=task_sdk_version, svn_release_repo=svn_release_repo
    )

    assert chdir_calls == [svn_release_repo, "/original/dir"]
    assert confirm_prompts == [
        "Do you want to look for old releases to remove?",
        "Remove old release 3.1.0?",
        "Remove old release 3.1.4?",
        "Remove old release task-sdk/1.1.0?",
        "Remove old release task-sdk/1.1.4?",
    ]
    assert "The following old Airflow releases should be removed: ['3.1.0', '3.1.4']" in console_messages
    assert (
        "The following old Task SDK releases should be removed: ['task-sdk/1.1.0', 'task-sdk/1.1.4']"
        in console_messages
    )
    assert "Removing old release 3.1.0" in console_messages
    assert "Removing old release 3.1.4" in console_messages
    assert "[success]Old releases removed" in console_messages

    # Only 3.1.0 was confirmed, so we should run rm+commit for 3.1.0 only.
    assert run_command_calls == [
        (["svn", "rm", "3.1.0"], {"check": True}),
        (["svn", "commit", "-m", "Remove old release: 3.1.0"], {"check": True}),
    ]


def test_remove_old_release_no_old_releases(monkeypatch, release_cmd):
    version = "3.0.5"
    task_sdk_version = "1.0.5"
    svn_release_repo = "/svn/release/repo"

    # Only current release exists
    entries = [
        FakeDirEntry(version, is_dir=True),
        FakeDirEntry("task-sdk", is_dir=True),  # task-sdk directory exists
    ]

    task_sdk_entries = [
        FakeDirEntry(task_sdk_version, is_dir=True),  # Only current task-sdk release
    ]

    chdir_calls: list[str] = []
    console_messages: list[str] = []
    run_command_calls: list[list[str]] = []

    def fake_confirm_action(prompt: str, **_kwargs) -> bool:
        if prompt == "Do you want to look for old releases to remove?":
            return True
        raise AssertionError(f"Unexpected confirm prompt: {prompt}")

    def fake_getcwd() -> str:
        return "/original/dir"

    def fake_path_exists(path: str) -> bool:
        return path == "/svn/release/repo/task-sdk"

    def fake_scandir(path=None):
        if path == "/svn/release/repo/task-sdk":
            return iter(task_sdk_entries)
        return iter(entries)

    monkeypatch.setattr(release_cmd.os, "getcwd", fake_getcwd)
    monkeypatch.setattr(release_cmd.os, "chdir", lambda path: chdir_calls.append(path))
    monkeypatch.setattr(release_cmd.os, "scandir", fake_scandir)
    monkeypatch.setattr(release_cmd.os.path, "exists", fake_path_exists)
    monkeypatch.setattr(release_cmd.os.path, "join", lambda *args: "/".join(args))
    monkeypatch.setattr(release_cmd, "confirm_action", fake_confirm_action)
    monkeypatch.setattr(release_cmd, "console_print", lambda msg="": console_messages.append(str(msg)))
    monkeypatch.setattr(release_cmd, "run_command", lambda cmd, **_kwargs: run_command_calls.append(cmd))

    release_cmd.remove_old_release(
        version=version, task_sdk_version=task_sdk_version, svn_release_repo=svn_release_repo
    )

    assert "No old releases to remove." in console_messages
    assert run_command_calls == []
    assert chdir_calls == [svn_release_repo, "/original/dir"]


def test_remove_old_release_task_sdk_only(monkeypatch, release_cmd):
    version = "3.0.5"
    task_sdk_version = "1.0.5"
    svn_release_repo = "/svn/release/repo"

    # Only current Airflow release exists, but old Task SDK releases exist
    entries = [
        FakeDirEntry(version, is_dir=True),
    ]

    task_sdk_entries = [
        FakeDirEntry(task_sdk_version, is_dir=True),  # current task-sdk release
        FakeDirEntry("1.0.4", is_dir=True),  # old task-sdk release
        FakeDirEntry("1.0.3", is_dir=True),  # old task-sdk release
    ]

    chdir_calls: list[str] = []
    console_messages: list[str] = []
    run_command_calls: list[tuple[list[str], dict]] = []
    confirm_prompts: list[str] = []

    def fake_confirm_action(prompt: str, **_kwargs) -> bool:
        confirm_prompts.append(prompt)
        if prompt == "Do you want to look for old releases to remove?":
            return True
        if prompt == "Remove old release task-sdk/1.0.3?":
            return True
        if prompt == "Remove old release task-sdk/1.0.4?":
            return False
        raise AssertionError(f"Unexpected confirm prompt: {prompt}")

    def fake_getcwd() -> str:
        return "/original/dir"

    def fake_path_exists(path: str) -> bool:
        return path == "/svn/release/repo/task-sdk"

    def fake_scandir(path=None):
        if path == "/svn/release/repo/task-sdk":
            return iter(task_sdk_entries)
        return iter(entries)

    monkeypatch.setattr(release_cmd.os, "getcwd", fake_getcwd)
    monkeypatch.setattr(release_cmd.os, "chdir", lambda path: chdir_calls.append(path))
    monkeypatch.setattr(release_cmd.os, "scandir", fake_scandir)
    monkeypatch.setattr(release_cmd.os.path, "exists", fake_path_exists)
    monkeypatch.setattr(release_cmd.os.path, "join", lambda *args: "/".join(args))
    monkeypatch.setattr(release_cmd, "confirm_action", fake_confirm_action)
    monkeypatch.setattr(release_cmd, "console_print", lambda msg="": console_messages.append(str(msg)))

    def fake_run_command(cmd: list[str], **kwargs):
        run_command_calls.append((cmd, kwargs))

    monkeypatch.setattr(release_cmd, "run_command", fake_run_command)

    release_cmd.remove_old_release(
        version=version, task_sdk_version=task_sdk_version, svn_release_repo=svn_release_repo
    )

    assert chdir_calls == [svn_release_repo, "/original/dir"]
    assert (
        "The following old Task SDK releases should be removed: ['task-sdk/1.0.3', 'task-sdk/1.0.4']"
        in console_messages
    )
    assert "Removing old release task-sdk/1.0.3" in console_messages
    assert "Removing old release task-sdk/1.0.4" in console_messages
    assert "[success]Old releases removed" in console_messages
    assert run_command_calls == [
        (["svn", "rm", "task-sdk/1.0.3"], {"check": True}),
        (["svn", "commit", "-m", "Remove old release: task-sdk/1.0.3"], {"check": True}),
    ]


def test_remove_old_release_no_task_sdk_version(monkeypatch, release_cmd):
    version = "3.0.5"
    task_sdk_version = None
    svn_release_repo = "/svn/release/repo"

    entries = [
        FakeDirEntry(version, is_dir=True),
        FakeDirEntry("3.0.4", is_dir=True),  # old release
    ]

    chdir_calls: list[str] = []
    console_messages: list[str] = []
    run_command_calls: list[list[str]] = []

    def fake_confirm_action(prompt: str, **_kwargs) -> bool:
        if prompt == "Do you want to look for old releases to remove?":
            return True
        if prompt.startswith("Remove old release "):
            return False
        raise AssertionError(f"Unexpected confirm prompt: {prompt}")

    def fake_getcwd() -> str:
        return "/original/dir"

    monkeypatch.setattr(release_cmd.os, "getcwd", fake_getcwd)
    monkeypatch.setattr(release_cmd.os, "chdir", lambda path: chdir_calls.append(path))
    monkeypatch.setattr(release_cmd.os, "scandir", lambda: iter(entries))
    monkeypatch.setattr(release_cmd, "confirm_action", fake_confirm_action)
    monkeypatch.setattr(release_cmd, "console_print", lambda msg="": console_messages.append(str(msg)))
    monkeypatch.setattr(release_cmd, "run_command", lambda cmd, **_kwargs: run_command_calls.append(cmd))

    release_cmd.remove_old_release(
        version=version, task_sdk_version=task_sdk_version, svn_release_repo=svn_release_repo
    )

    assert "The following old Airflow releases should be removed: ['3.0.4']" in console_messages
    assert "task-sdk" not in " ".join(console_messages).lower()
    assert run_command_calls == []
    assert chdir_calls == [svn_release_repo, "/original/dir"]
