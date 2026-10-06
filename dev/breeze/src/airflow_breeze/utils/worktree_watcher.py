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
"""
Detached watcher that removes the Docker resources of a deleted Breeze worktree.

Breeze starts it as a separate process with ``python -I <this file>``. The watcher must keep working
after the worktree is deleted, and that deletion can take Breeze's own virtualenv with it, so this
module only uses the standard library, imports everything up front and never imports
``airflow_breeze``.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import subprocess
import time
from pathlib import Path
from typing import IO

OWNER_LABEL = "org.apache.airflow.breeze=true"
WORKTREE_LABEL = "org.apache.airflow.breeze.worktree"
HOST_LABEL = "org.apache.airflow.breeze.host"
DOCKER_TIMEOUT_SECONDS = 120


def try_lock(lock_file: Path) -> IO[str] | None:
    """Take the watcher lock, or return ``None`` when another process holds it."""
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_file.open("a")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        return None
    return handle


def is_watcher_running(lock_file: Path) -> bool:
    handle = try_lock(lock_file)
    if handle is None:
        return True
    handle.close()
    return False


def is_worktree_missing(worktree: Path) -> bool:
    try:
        worktree.stat()
    except FileNotFoundError:
        return True
    except OSError:
        return False
    return False


def list_resources(kind: str, worktree: Path, host: str) -> list[str] | None:
    """Return IDs of this worktree's resources of ``kind``, or ``None`` when Docker cannot be queried."""
    cmd = [
        "docker",
        kind,
        "ls",
        "--quiet",
        "--filter",
        f"label={OWNER_LABEL}",
        "--filter",
        f"label={WORKTREE_LABEL}={worktree}",
        "--filter",
        f"label={HOST_LABEL}={host}",
    ]
    if kind == "container":
        cmd.append("--all")
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, check=False, timeout=DOCKER_TIMEOUT_SECONDS
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    return result.stdout.split() if result.returncode == 0 else None


def remove_resources(worktree: Path, host: str) -> None:
    # Containers go first: Docker refuses to remove volumes and networks that containers still use.
    for kind, remove in (
        ("container", ["docker", "container", "rm", "--force", "--volumes"]),
        ("volume", ["docker", "volume", "rm"]),
        ("network", ["docker", "network", "rm"]),
    ):
        identifiers = list_resources(kind, worktree, host)
        if identifiers:
            with contextlib.suppress(OSError, subprocess.TimeoutExpired):
                subprocess.run(
                    [*remove, *identifiers], capture_output=True, check=False, timeout=DOCKER_TIMEOUT_SECONDS
                )


def watch(worktree: Path, host: str, poll_interval: float, idle_timeout: float) -> None:
    """
    Remove the worktree's resources once its directory disappears.

    Returns early when the worktree has had no containers for ``idle_timeout`` seconds. Breeze starts
    a new watcher on its next Docker command, and resources left without containers are removed by
    the stale-worktree sweep that Breeze runs before Docker commands.
    """
    last_seen_containers = time.monotonic()
    while True:
        if is_worktree_missing(worktree):
            remove_resources(worktree, host)
            return
        if list_resources("container", worktree, host):
            last_seen_containers = time.monotonic()
        elif time.monotonic() - last_seen_containers > idle_timeout:
            return
        time.sleep(poll_interval)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worktree", type=Path, required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--lock-file", type=Path, required=True)
    parser.add_argument("--poll-interval", type=float, default=30)
    parser.add_argument("--idle-timeout", type=float, default=3600)
    args = parser.parse_args(argv)
    lock = try_lock(args.lock_file)
    if lock is None:
        return
    with lock:
        watch(args.worktree, args.host, args.poll_interval, args.idle_timeout)


if __name__ == "__main__":
    main()
