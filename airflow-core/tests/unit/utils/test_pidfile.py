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

import pytest

from airflow.utils.pidfile import (
    PIDLockFile,
    TimeoutPIDLockFile,
    read_pid_from_pidfile,
    remove_existing_pidfile,
    write_pid_to_pidfile,
)


class TestReadPidFromPidfile:
    def test_reads_valid_pid(self, tmp_path):
        pidfile = tmp_path / "test.pid"
        pidfile.write_text("12345\n")
        assert read_pid_from_pidfile(str(pidfile)) == 12345

    def test_reads_pid_without_trailing_newline(self, tmp_path):
        pidfile = tmp_path / "test.pid"
        pidfile.write_text("99")
        assert read_pid_from_pidfile(str(pidfile)) == 99

    def test_ignores_leading_whitespace(self, tmp_path):
        pidfile = tmp_path / "test.pid"
        pidfile.write_text("  42  \n")
        assert read_pid_from_pidfile(str(pidfile)) == 42

    def test_returns_none_for_missing_file(self, tmp_path):
        assert read_pid_from_pidfile(str(tmp_path / "nonexistent.pid")) is None

    def test_returns_none_for_non_numeric_content(self, tmp_path):
        pidfile = tmp_path / "test.pid"
        pidfile.write_text("not-a-number\n")
        assert read_pid_from_pidfile(str(pidfile)) is None

    def test_returns_none_for_empty_file(self, tmp_path):
        pidfile = tmp_path / "test.pid"
        pidfile.write_text("")
        assert read_pid_from_pidfile(str(pidfile)) is None


class TestWritePidToPidfile:
    def test_writes_current_pid(self, tmp_path):
        pidfile = str(tmp_path / "test.pid")
        write_pid_to_pidfile(pidfile)
        with open(pidfile) as file:
            content = file.read()
        assert content.strip() == str(os.getpid())

    def test_raises_on_existing_file(self, tmp_path):
        pidfile = str(tmp_path / "test.pid")
        write_pid_to_pidfile(pidfile)
        with pytest.raises(OSError, match="File exists"):
            write_pid_to_pidfile(pidfile)

    def test_creates_file_with_correct_permissions(self, tmp_path):
        pidfile = str(tmp_path / "test.pid")
        previous_umask = os.umask(0o022)
        try:
            write_pid_to_pidfile(pidfile)
        finally:
            os.umask(previous_umask)
        mode = os.stat(pidfile).st_mode & 0o777
        assert mode == 0o644


class TestRemoveExistingPidfile:
    def test_removes_existing_file(self, tmp_path):
        pidfile = tmp_path / "test.pid"
        pidfile.write_text("123\n")
        remove_existing_pidfile(str(pidfile))
        assert not pidfile.exists()

    def test_no_error_on_missing_file(self, tmp_path):
        remove_existing_pidfile(str(tmp_path / "nonexistent.pid"))


class TestPIDLockFile:
    def test_not_locked_initially(self, tmp_path):
        lock = PIDLockFile(str(tmp_path / "test.pid"))
        assert not lock.is_locked()

    def test_read_pid_returns_none_when_not_locked(self, tmp_path):
        lock = PIDLockFile(str(tmp_path / "test.pid"))
        assert lock.read_pid() is None

    def test_is_locked_after_acquire(self, tmp_path):
        lock = PIDLockFile(str(tmp_path / "test.pid"))
        lock.acquire()
        try:
            assert lock.is_locked()
            assert lock.read_pid() == os.getpid()
        finally:
            lock.release()

    def test_i_am_locking(self, tmp_path):
        lock = PIDLockFile(str(tmp_path / "test.pid"))
        lock.acquire()
        try:
            assert lock.i_am_locking()
        finally:
            lock.release()

    def test_i_am_not_locking_other_pid(self, tmp_path):
        pidfile = tmp_path / "test.pid"
        pidfile.write_text("99999\n")
        lock = PIDLockFile(str(pidfile))
        assert lock.is_locked()
        assert not lock.i_am_locking()

    def test_release_removes_file(self, tmp_path):
        pidfile_path = str(tmp_path / "test.pid")
        lock = PIDLockFile(pidfile_path)
        lock.acquire()
        lock.release()
        assert not os.path.exists(pidfile_path)

    def test_release_noop_when_not_locked(self, tmp_path):
        lock = PIDLockFile(str(tmp_path / "test.pid"))
        lock.release()

    def test_break_lock(self, tmp_path):
        pidfile = tmp_path / "test.pid"
        pidfile.write_text("99999\n")
        lock = PIDLockFile(str(pidfile))
        lock.break_lock()
        assert not pidfile.exists()

    def test_context_manager(self, tmp_path):
        pidfile_path = str(tmp_path / "test.pid")
        with PIDLockFile(pidfile_path) as lock:
            assert lock.is_locked()
            assert lock.read_pid() == os.getpid()
        assert not os.path.exists(pidfile_path)

    def test_acquire_raises_immediately_with_zero_timeout(self, tmp_path):
        pidfile = tmp_path / "test.pid"
        pidfile.write_text("99999\n")
        lock = PIDLockFile(str(pidfile))
        with pytest.raises(FileExistsError):
            lock.acquire(timeout=0)

    def test_acquire_raises_immediately_with_negative_timeout(self, tmp_path):
        pidfile = tmp_path / "test.pid"
        pidfile.write_text("99999\n")
        lock = PIDLockFile(str(pidfile))
        with pytest.raises(FileExistsError):
            lock.acquire(timeout=-1)


class TestTimeoutPIDLockFile:
    def test_uses_acquire_timeout_as_default(self, tmp_path):
        pidfile = tmp_path / "test.pid"
        pidfile.write_text("99999\n")
        lock = TimeoutPIDLockFile(str(pidfile), acquire_timeout=-1)
        with pytest.raises(FileExistsError):
            lock.acquire()

    def test_explicit_timeout_overrides_default(self, tmp_path):
        pidfile = tmp_path / "test.pid"
        pidfile.write_text("99999\n")
        lock = TimeoutPIDLockFile(str(pidfile), acquire_timeout=None)
        with pytest.raises(FileExistsError):
            lock.acquire(timeout=0)

    def test_context_manager_acquires_and_releases(self, tmp_path):
        pidfile_path = str(tmp_path / "test.pid")
        lock = TimeoutPIDLockFile(pidfile_path, acquire_timeout=None)
        with lock:
            assert os.path.exists(pidfile_path)
        assert not os.path.exists(pidfile_path)

    def test_timeout_expires_raises(self, tmp_path):
        pidfile = tmp_path / "test.pid"
        pidfile.write_text("99999\n")
        lock = TimeoutPIDLockFile(str(pidfile), acquire_timeout=0.01)
        with pytest.raises(TimeoutError):
            lock.acquire()
