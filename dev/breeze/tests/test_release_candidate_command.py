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
        if prompt.startswith("Remove old RC "):
            return False
        raise AssertionError(f"Unexpected confirm prompt: {prompt}")

    monkeypatch.setattr(rc_cmd.os, "chdir", lambda path: chdir_calls.append(path))
    monkeypatch.setattr(rc_cmd.os, "scandir", lambda: iter(entries))
    monkeypatch.setattr(rc_cmd, "confirm_action", fake_confirm_action)
    monkeypatch.setattr(rc_cmd, "console_print", lambda msg="": console_messages.append(str(msg)))
    monkeypatch.setattr(rc_cmd, "run_command", lambda cmd, **_kwargs: run_command_calls.append(cmd))

    # Act
    rc_cmd.remove_old_releases(version=version, repo_root=repo_root)

    # Assert: only directory entries matching RC_PATTERN, excluding current version, and sorted.
    assert f"{repo_root}/asf-dist/dev/airflow" in chdir_calls
    assert repo_root in chdir_calls
    assert "The following old releases should be removed: ['2.10.0rc1', '2.10.0rc2']" in console_messages
    assert run_command_calls == []


def test_remove_old_releases_returns_early_when_user_declines(monkeypatch, rc_cmd):
    version = "2.10.0rc3"
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

    rc_cmd.remove_old_releases(version=version, repo_root=repo_root)

    assert confirm_prompts == ["Do you want to look for old RCs to remove?"]


def test_remove_old_releases_removes_confirmed_old_releases(monkeypatch, rc_cmd):
    version = "2.10.0rc3"
    repo_root = "/repo/root"

    # Unsorted on purpose to verify sorting before prompting/removing.
    entries = [
        FakeDirEntry("2.10.0rc2", is_dir=True),
        FakeDirEntry(version, is_dir=True),
        FakeDirEntry("2.10.0rc1", is_dir=True),
    ]

    chdir_calls: list[str] = []
    console_messages: list[str] = []
    run_command_calls: list[tuple[list[str], dict]] = []
    confirm_prompts: list[str] = []

    def fake_confirm_action(prompt: str, **_kwargs) -> bool:
        confirm_prompts.append(prompt)
        if prompt == "Do you want to look for old RCs to remove?":
            return True
        if prompt == "Remove old RC 2.10.0rc1?":
            return True
        if prompt == "Remove old RC 2.10.0rc2?":
            return False
        raise AssertionError(f"Unexpected confirm prompt: {prompt}")

    monkeypatch.setattr(rc_cmd.os, "chdir", lambda path: chdir_calls.append(path))
    monkeypatch.setattr(rc_cmd.os, "scandir", lambda: iter(entries))
    monkeypatch.setattr(rc_cmd, "confirm_action", fake_confirm_action)
    monkeypatch.setattr(rc_cmd, "console_print", lambda msg="": console_messages.append(str(msg)))

    def fake_run_command(cmd: list[str], **kwargs):
        run_command_calls.append((cmd, kwargs))

    monkeypatch.setattr(rc_cmd, "run_command", fake_run_command)

    rc_cmd.remove_old_releases(version=version, repo_root=repo_root)

    assert chdir_calls == [f"{repo_root}/asf-dist/dev/airflow", repo_root]
    assert confirm_prompts == [
        "Do you want to look for old RCs to remove?",
        "Remove old RC 2.10.0rc1?",
        "Remove old RC 2.10.0rc2?",
    ]
    assert "The following old releases should be removed: ['2.10.0rc1', '2.10.0rc2']" in console_messages
    assert "Removing old release 2.10.0rc1" in console_messages
    assert "Removing old release 2.10.0rc2" in console_messages
    assert "[success]Old releases removed" in console_messages

    # Only rc1 was confirmed, so we should run rm+commit for rc1 only.
    assert run_command_calls == [
        (["svn", "rm", "2.10.0rc1"], {"check": True}),
        (["svn", "commit", "-m", "Remove old release: 2.10.0rc1"], {"check": True}),
    ]
