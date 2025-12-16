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


class FakeDirEntry:
    def __init__(self, name: str, *, is_dir: bool):
        self.name = name
        self._is_dir = is_dir

    def is_dir(self) -> bool:
        return self._is_dir


@pytest.fixture
def rc_cmd():
    """Lazy import the rc command module."""
    import airflow_breeze.commands.release_candidate_command as module

    return module


def test_remove_old_releases_only_collects_rc_directories(monkeypatch, rc_cmd):
    version = "2.10.0rc3"
    task_sdk_version = "1.0.6rc3"
    repo_root = "/repo/root"

    # Arrange: entries include current RC, old RC directories, a matching "file", and non-RC directory.
    entries = [
        FakeDirEntry(version, is_dir=True),  # current RC: should be skipped
        FakeDirEntry("2.10.0rc2", is_dir=True),  # old RC dir: should be included
        FakeDirEntry("2.10.0rc1", is_dir=True),  # old RC dir: should be included
        FakeDirEntry("2.10.0rc0", is_dir=False),  # matches pattern but not a directory: excluded
        FakeDirEntry("not-a-rc", is_dir=True),  # directory but not matching pattern: excluded
    ]

    chdir_calls: list[str] = []
    console_messages: list[str] = []
    run_command_calls: list[list[str]] = []

    def fake_confirm_action(prompt: str, **_kwargs) -> bool:
        # First prompt decides whether we scan. We want to.
        if prompt == "Do you want to look for old RCs to remove?":
            return True
        # For each candidate, we decline removal to avoid running svn commands.
        if prompt.startswith("Remove old RC ") or prompt.startswith("Remove old Task SDK RC "):
            return False
        raise AssertionError(f"Unexpected confirm prompt: {prompt}")

    def fake_path_exists(path: str) -> bool:
        # Task SDK path doesn't exist in this test
        return False

    monkeypatch.setattr(rc_cmd.os, "chdir", lambda path: chdir_calls.append(path))
    monkeypatch.setattr(rc_cmd.os, "scandir", lambda: iter(entries))
    monkeypatch.setattr(rc_cmd.os.path, "exists", fake_path_exists)
    monkeypatch.setattr(rc_cmd, "confirm_action", fake_confirm_action)
    monkeypatch.setattr(rc_cmd, "console_print", lambda msg="": console_messages.append(str(msg)))
    monkeypatch.setattr(rc_cmd, "run_command", lambda cmd, **_kwargs: run_command_calls.append(cmd))

    # Act
    rc_cmd.remove_old_releases(version=version, task_sdk_version=task_sdk_version, repo_root=repo_root)

    # Assert: only directory entries matching RC_PATTERN, excluding current version, and sorted.
    assert f"{repo_root}/asf-dist/dev/airflow" in chdir_calls
    assert repo_root in chdir_calls
    assert (
        "The following old Airflow releases should be removed: ['2.10.0rc1', '2.10.0rc2']" in console_messages
    )
    assert run_command_calls == []


def test_remove_old_releases_returns_early_when_user_declines(monkeypatch, rc_cmd):
    version = "2.10.0rc3"
    task_sdk_version = "1.0.6rc3"
    repo_root = "/repo/root"

    confirm_prompts: list[str] = []

    def fake_confirm_action(prompt: str, **_kwargs) -> bool:
        confirm_prompts.append(prompt)
        return False

    def should_not_be_called(*_args, **_kwargs):
        raise AssertionError("This should not have been called when user declines the initial prompt.")

    monkeypatch.setattr(rc_cmd, "confirm_action", fake_confirm_action)
    monkeypatch.setattr(rc_cmd.os, "chdir", should_not_be_called)
    monkeypatch.setattr(rc_cmd.os, "scandir", should_not_be_called)
    monkeypatch.setattr(rc_cmd, "console_print", should_not_be_called)
    monkeypatch.setattr(rc_cmd, "run_command", should_not_be_called)

    rc_cmd.remove_old_releases(version=version, task_sdk_version=task_sdk_version, repo_root=repo_root)

    assert confirm_prompts == ["Do you want to look for old RCs to remove?"]


def test_remove_old_releases_removes_confirmed_old_releases(monkeypatch, rc_cmd):
    version = "3.1.5rc3"
    task_sdk_version = "1.0.6rc3"
    repo_root = "/repo/root"

    # Unsorted on purpose to verify sorting before prompting/removing.
    entries = [
        FakeDirEntry("3.1.5rc2", is_dir=True),
        FakeDirEntry(version, is_dir=True),
        FakeDirEntry("3.1.0rc1", is_dir=True),
    ]

    chdir_calls: list[str] = []
    console_messages: list[str] = []
    run_command_calls: list[tuple[list[str], dict]] = []
    confirm_prompts: list[str] = []

    def fake_confirm_action(prompt: str, **_kwargs) -> bool:
        confirm_prompts.append(prompt)
        if prompt == "Do you want to look for old RCs to remove?":
            return True
        if prompt == "Remove old RC 3.1.0rc1?":
            return True
        if prompt == "Remove old RC 3.1.5rc2?":
            return False
        raise AssertionError(f"Unexpected confirm prompt: {prompt}")

    def fake_path_exists(path: str) -> bool:
        # Task SDK path doesn't exist in this test
        return False

    monkeypatch.setattr(rc_cmd.os, "chdir", lambda path: chdir_calls.append(path))
    monkeypatch.setattr(rc_cmd.os, "scandir", lambda: iter(entries))
    monkeypatch.setattr(rc_cmd.os.path, "exists", fake_path_exists)
    monkeypatch.setattr(rc_cmd, "confirm_action", fake_confirm_action)
    monkeypatch.setattr(rc_cmd, "console_print", lambda msg="": console_messages.append(str(msg)))

    def fake_run_command(cmd: list[str], **kwargs):
        run_command_calls.append((cmd, kwargs))

    monkeypatch.setattr(rc_cmd, "run_command", fake_run_command)

    rc_cmd.remove_old_releases(version=version, task_sdk_version=task_sdk_version, repo_root=repo_root)

    assert chdir_calls == [f"{repo_root}/asf-dist/dev/airflow", repo_root]
    assert confirm_prompts == [
        "Do you want to look for old RCs to remove?",
        "Remove old RC 3.1.0rc1?",
        "Remove old RC 3.1.5rc2?",
    ]
    assert (
        "The following old Airflow releases should be removed: ['3.1.0rc1', '3.1.5rc2']" in console_messages
    )
    assert "Removing old Airflow release 3.1.0rc1" in console_messages
    assert "Removing old Airflow release 3.1.5rc2" in console_messages
    assert "[success]Old releases removed" in console_messages

    # Only rc1 was confirmed, so we should run rm+commit for rc1 only.
    assert run_command_calls == [
        (["svn", "rm", "3.1.0rc1"], {"check": True}),
        (["svn", "commit", "-m", "Remove old release: 3.1.0rc1"], {"check": True}),
    ]


def test_remove_old_releases_removes_task_sdk_releases(monkeypatch, rc_cmd):
    version = "3.1.5rc3"
    task_sdk_version = "1.0.6rc3"
    repo_root = "/repo/root"

    # Airflow entries
    airflow_entries = [
        FakeDirEntry(version, is_dir=True),
        FakeDirEntry("3.1.5rc2", is_dir=True),
    ]

    # Task SDK entries
    task_sdk_entries = [
        FakeDirEntry(task_sdk_version, is_dir=True),  # current RC: should be skipped
        FakeDirEntry("1.0.6rc2", is_dir=True),  # old RC dir: should be included
        FakeDirEntry("1.0.6rc1", is_dir=True),  # old RC dir: should be included
    ]

    chdir_calls: list[str] = []
    console_messages: list[str] = []
    run_command_calls: list[tuple[list[str], dict]] = []
    confirm_prompts: list[str] = []
    scandir_call_count = 0

    def fake_confirm_action(prompt: str, **_kwargs) -> bool:
        confirm_prompts.append(prompt)
        if prompt == "Do you want to look for old RCs to remove?":
            return True
        # Decline all removals to avoid running svn commands
        if prompt.startswith("Remove old RC ") or prompt.startswith("Remove old Task SDK RC "):
            return False
        raise AssertionError(f"Unexpected confirm prompt: {prompt}")

    def fake_path_exists(path: str) -> bool:
        # Task SDK path exists in this test
        return path == f"{repo_root}/asf-dist/dev/airflow/task-sdk"

    def fake_scandir():
        nonlocal scandir_call_count
        scandir_call_count += 1
        # First call is for Airflow, second is for Task SDK
        if scandir_call_count == 1:
            return iter(airflow_entries)
        if scandir_call_count == 2:
            return iter(task_sdk_entries)
        raise AssertionError("Unexpected scandir call")

    monkeypatch.setattr(rc_cmd.os, "chdir", lambda path: chdir_calls.append(path))
    monkeypatch.setattr(rc_cmd.os, "scandir", fake_scandir)
    monkeypatch.setattr(rc_cmd.os.path, "exists", fake_path_exists)
    monkeypatch.setattr(rc_cmd, "confirm_action", fake_confirm_action)
    monkeypatch.setattr(rc_cmd, "console_print", lambda msg="": console_messages.append(str(msg)))
    monkeypatch.setattr(rc_cmd, "run_command", lambda cmd, **_kwargs: run_command_calls.append((cmd, {})))

    rc_cmd.remove_old_releases(version=version, task_sdk_version=task_sdk_version, repo_root=repo_root)

    assert f"{repo_root}/asf-dist/dev/airflow" in chdir_calls
    assert f"{repo_root}/asf-dist/dev/airflow/task-sdk" in chdir_calls
    assert repo_root in chdir_calls
    assert "The following old Airflow releases should be removed: ['3.1.5rc2']" in console_messages
    assert (
        "The following old Task SDK releases should be removed: ['1.0.6rc1', '1.0.6rc2']" in console_messages
    )
    assert "[success]Old releases removed" in console_messages
    # No removals were confirmed, so no svn commands should be run
    assert run_command_calls == []


def test_remove_old_releases_removes_both_airflow_and_task_sdk_releases(monkeypatch, rc_cmd):
    version = "3.1.5rc3"
    task_sdk_version = "1.0.6rc3"
    repo_root = "/repo/root"

    # Airflow entries
    airflow_entries = [
        FakeDirEntry(version, is_dir=True),
        FakeDirEntry("3.1.5rc2", is_dir=True),
    ]

    # Task SDK entries
    task_sdk_entries = [
        FakeDirEntry(task_sdk_version, is_dir=True),
        FakeDirEntry("1.0.6rc2", is_dir=True),
        FakeDirEntry("1.0.6rc1", is_dir=True),
    ]

    chdir_calls: list[str] = []
    console_messages: list[str] = []
    run_command_calls: list[tuple[list[str], dict]] = []
    confirm_prompts: list[str] = []
    scandir_call_count = 0

    def fake_confirm_action(prompt: str, **_kwargs) -> bool:
        confirm_prompts.append(prompt)
        if prompt == "Do you want to look for old RCs to remove?":
            return True
        # Confirm removal of one Airflow and one Task SDK release
        if prompt == "Remove old RC 3.1.5rc2?":
            return True
        if prompt == "Remove old Task SDK RC 1.0.6rc1?":
            return True
        # Decline others
        if prompt.startswith("Remove old RC ") or prompt.startswith("Remove old Task SDK RC "):
            return False
        raise AssertionError(f"Unexpected confirm prompt: {prompt}")

    def fake_path_exists(path: str) -> bool:
        return path == f"{repo_root}/asf-dist/dev/airflow/task-sdk"

    def fake_scandir():
        nonlocal scandir_call_count
        scandir_call_count += 1
        if scandir_call_count == 1:
            return iter(airflow_entries)
        if scandir_call_count == 2:
            return iter(task_sdk_entries)
        raise AssertionError("Unexpected scandir call")

    monkeypatch.setattr(rc_cmd.os, "chdir", lambda path: chdir_calls.append(path))
    monkeypatch.setattr(rc_cmd.os, "scandir", fake_scandir)
    monkeypatch.setattr(rc_cmd.os.path, "exists", fake_path_exists)
    monkeypatch.setattr(rc_cmd, "confirm_action", fake_confirm_action)
    monkeypatch.setattr(rc_cmd, "console_print", lambda msg="": console_messages.append(str(msg)))

    def fake_run_command(cmd: list[str], **kwargs):
        run_command_calls.append((cmd, kwargs))

    monkeypatch.setattr(rc_cmd, "run_command", fake_run_command)

    rc_cmd.remove_old_releases(version=version, task_sdk_version=task_sdk_version, repo_root=repo_root)

    assert chdir_calls == [
        f"{repo_root}/asf-dist/dev/airflow",
        f"{repo_root}/asf-dist/dev/airflow/task-sdk",
        repo_root,
    ]
    assert "The following old Airflow releases should be removed: ['3.1.5rc2']" in console_messages
    assert (
        "The following old Task SDK releases should be removed: ['1.0.6rc1', '1.0.6rc2']" in console_messages
    )
    assert "Removing old Airflow release 3.1.5rc2" in console_messages
    assert "Removing old Task SDK release 1.0.6rc1" in console_messages
    assert "Removing old Task SDK release 1.0.6rc2" in console_messages
    assert "[success]Old releases removed" in console_messages

    # Both Airflow and Task SDK removals were confirmed
    assert run_command_calls == [
        (["svn", "rm", "3.1.5rc2"], {"check": True}),
        (["svn", "commit", "-m", "Remove old release: 3.1.5rc2"], {"check": True}),
        (["svn", "rm", "1.0.6rc1"], {"check": True}),
        (["svn", "commit", "-m", "Remove old Task SDK release: 1.0.6rc1"], {"check": True}),
    ]
