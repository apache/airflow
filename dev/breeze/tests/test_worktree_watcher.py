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

import os
import subprocess
import sys
import textwrap
from unittest import mock

import pytest

from airflow_breeze.utils import worktree_watcher


def test_lock_allows_a_single_watcher(tmp_path):
    lock_file = tmp_path / ".build" / "worktree-watcher.lock"
    assert worktree_watcher.is_watcher_running(lock_file) is False

    with worktree_watcher.try_lock(lock_file):
        assert worktree_watcher.try_lock(lock_file) is None
        assert worktree_watcher.is_watcher_running(lock_file) is True

    assert worktree_watcher.is_watcher_running(lock_file) is False


@pytest.fixture
def docker():
    """Fake ``docker`` that reports one resource of each kind until it is asked to remove it."""
    commands = []

    def run(cmd, **kwargs):
        commands.append(cmd)
        stdout = f"{cmd[1]}-id\n" if cmd[2] == "ls" else ""
        return subprocess.CompletedProcess(cmd, 0, stdout=stdout, stderr="")

    with mock.patch.object(worktree_watcher.subprocess, "run", autospec=True, side_effect=run):
        yield commands


@mock.patch.object(worktree_watcher.time, "sleep", autospec=True)
def test_watch_removes_resources_once_the_worktree_is_deleted(sleep, docker, tmp_path):
    worktree = tmp_path / "worktree"
    worktree.mkdir()
    sleep.side_effect = lambda _: worktree.rmdir()

    worktree_watcher.watch(worktree, "this-host", poll_interval=30, idle_timeout=3600)

    sleep.assert_called_once_with(30)
    listings = [cmd for cmd in docker if cmd[2] == "ls"]
    assert all(
        {
            f"label=org.apache.airflow.breeze.worktree={worktree}",
            "label=org.apache.airflow.breeze.host=this-host",
        }
        <= set(cmd)
        for cmd in listings
    )
    assert [cmd for cmd in docker if cmd[2] == "rm"] == [
        ["docker", "container", "rm", "--force", "--volumes", "container-id"],
        ["docker", "volume", "rm", "volume-id"],
        ["docker", "network", "rm", "network-id"],
    ]


@mock.patch.object(worktree_watcher.time, "sleep", autospec=True)
@mock.patch.object(worktree_watcher, "list_resources", autospec=True, return_value=[])
def test_watch_stops_when_the_worktree_stays_idle(list_resources, sleep, tmp_path):
    worktree_watcher.watch(tmp_path, "this-host", poll_interval=30, idle_timeout=0)

    list_resources.assert_called_once_with("container", tmp_path, "this-host")


@mock.patch.object(worktree_watcher, "watch", autospec=True)
def test_main_exits_when_another_watcher_holds_the_lock(watch, tmp_path):
    lock_file = tmp_path / "worktree-watcher.lock"
    args = ["--worktree", str(tmp_path), "--host", "this-host", "--lock-file", str(lock_file)]
    with worktree_watcher.try_lock(lock_file):
        worktree_watcher.main(args)
    watch.assert_not_called()

    worktree_watcher.main(args)
    watch.assert_called_once_with(tmp_path, "this-host", 30, 3600)


def test_watcher_runs_standalone_without_breeze_on_the_path(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "docker.log"
    fake_docker = bin_dir / "docker"
    fake_docker.write_text(
        textwrap.dedent(
            f"""\
            #!/bin/sh
            echo "$@" >> {log}
            if [ "$2" = "ls" ]; then echo "$1-id"; fi
            """
        )
    )
    fake_docker.chmod(0o755)
    deleted = tmp_path / "deleted-worktree"

    subprocess.run(
        [
            sys.executable,
            "-I",
            worktree_watcher.__file__,
            "--worktree",
            str(deleted),
            "--host",
            "this-host",
            "--lock-file",
            str(tmp_path / "worktree-watcher.lock"),
        ],
        env={**os.environ, "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}"},
        cwd="/",
        check=True,
        timeout=60,
    )

    removals = [line for line in log.read_text().splitlines() if " rm " in line]
    assert removals == [
        "container rm --force --volumes container-id",
        "volume rm volume-id",
        "network rm network-id",
    ]
