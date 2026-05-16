#
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
"""PID file utilities replacing the deprecated ``lockfile`` library."""

from __future__ import annotations

import errno
import os
import time


class PIDLockFile:
    """
    Lockfile implemented as a Unix PID file.

    The lock file is a normal file named by the attribute ``path``.
    A lock's PID file contains a single line of text, containing
    the process ID (PID) of the process that acquired the lock.

    This is a minimal replacement for ``lockfile.pidlockfile.PIDLockFile``.
    """

    def __init__(self, path: str):
        self.path = path

    def read_pid(self) -> int | None:
        """Read the PID recorded in the PID file."""
        return read_pid_from_pidfile(self.path)

    def is_locked(self) -> bool:
        """Return ``True`` if the PID file exists."""
        return os.path.exists(self.path)

    def i_am_locking(self) -> bool:
        """Return ``True`` if the current process ID matches the PID in the file."""
        return self.is_locked() and os.getpid() == self.read_pid()

    def break_lock(self) -> None:
        """Remove the PID file if it exists."""
        remove_existing_pidfile(self.path)

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *_exc):
        self.release()

    def acquire(self, timeout=None):
        """
        Acquire the lock by creating the PID file.

        If *timeout* > 0 wait up to that many seconds; if 0 or negative
        raise immediately on conflict; if ``None`` wait forever.
        """
        end_time = time.monotonic()
        if timeout is not None and timeout > 0:
            end_time += timeout

        while True:
            try:
                write_pid_to_pidfile(self.path)
            except OSError as exc:
                if exc.errno == errno.EEXIST:
                    if timeout is not None and timeout > 0 and time.monotonic() > end_time:
                        raise TimeoutError(f"Timeout waiting to acquire lock for {self.path}") from exc
                    if timeout is not None and timeout <= 0:
                        raise FileExistsError(f"{self.path} is already locked") from exc
                    time.sleep(timeout / 10 if timeout else 0.1)
                else:
                    raise
            else:
                return

    def release(self):
        """Release the lock by removing the PID file."""
        if not self.is_locked():
            return
        if self.i_am_locking():
            remove_existing_pidfile(self.path)


class TimeoutPIDLockFile(PIDLockFile):
    """
    PIDLockFile with a default acquire timeout.

    Drop-in replacement for ``daemon.pidfile.TimeoutPIDLockFile``.
    """

    def __init__(self, path: str, acquire_timeout: float | None = None):
        super().__init__(path)
        self.acquire_timeout = acquire_timeout

    def acquire(self, timeout=None):
        if timeout is None:
            timeout = self.acquire_timeout
        super().acquire(timeout)


def read_pid_from_pidfile(pidfile_path: str) -> int | None:
    """
    Read and return the numeric PID recorded in the named PID file.

    If the PID file cannot be read, or if the content is not a valid PID,
    return ``None``.
    """
    try:
        with open(pidfile_path) as pidfile:
            line = pidfile.readline().strip()
            return int(line)
    except (OSError, ValueError):
        return None


def write_pid_to_pidfile(pidfile_path: str) -> None:
    """
    Write the current process PID to the named PID file.

    Uses ``O_CREAT | O_EXCL`` to fail atomically if the file already exists.
    """
    open_flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    open_mode = 0o644
    pidfile_fd = os.open(pidfile_path, open_flags, open_mode)
    try:
        os.write(pidfile_fd, f"{os.getpid()}\n".encode())
    finally:
        os.close(pidfile_fd)


def remove_existing_pidfile(pidfile_path: str) -> None:
    """
    Remove the named PID file if it exists.

    Silently ignores the case where the file does not exist.
    """
    try:
        os.remove(pidfile_path)
    except OSError as exc:
        if exc.errno != errno.ENOENT:
            raise
