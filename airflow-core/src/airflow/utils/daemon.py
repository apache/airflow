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
"""
Minimal daemon context replacing the ``python-daemon`` distribution.

Implements the subset of :pep:`3143` that Airflow actually uses:
double-fork detach, file-descriptor cleanup, stream redirection,
signal-handler installation, umask/chroot/cwd changes, UID/GID
switching, PID-file management and core-dump prevention.
"""

from __future__ import annotations

import atexit
import os
import signal
import socket
import sys

try:
    import pwd
except ModuleNotFoundError:  # pragma: no cover
    pwd = None

try:
    import resource
except ModuleNotFoundError:  # pragma: no cover
    resource = None


class DaemonContext:
    """Context manager that turns the current process into a Unix daemon."""

    def __init__(
        self,
        *,
        chroot_directory: str | None = None,
        working_directory: str = "/",
        umask: int = 0,
        uid: int | None = None,
        gid: int | None = None,
        initgroups: bool = False,
        prevent_core: bool = True,
        detach_process: bool | None = None,
        files_preserve: list | None = None,
        pidfile=None,
        stdin=None,
        stdout=None,
        stderr=None,
        signal_map: dict | None = None,
    ):
        self.chroot_directory = chroot_directory
        self.working_directory = working_directory
        self.umask = umask
        self.prevent_core = prevent_core
        self.files_preserve = files_preserve
        self.pidfile = pidfile
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr

        self.uid = uid if uid is not None else os.getuid()
        self.gid = gid if gid is not None else os.getgid()
        self.initgroups = initgroups

        if detach_process is None:
            detach_process = _is_detach_process_context_required()
        self.detach_process = detach_process

        if signal_map is None:
            signal_map = _make_default_signal_map()
        self.signal_map = signal_map

        self._is_open = False

    @property
    def is_open(self) -> bool:
        return self._is_open

    # -- context-manager protocol ------------------------------------------

    def open(self) -> None:
        if self.is_open:
            return

        if self.chroot_directory is not None:
            _change_root_directory(self.chroot_directory)

        if self.prevent_core:
            _prevent_core_dump()

        _change_file_creation_mask(self.umask)
        _change_working_directory(self.working_directory)
        _change_process_owner(self.uid, self.gid, self.initgroups)

        if self.detach_process:
            _detach_process_context()

        signal_handler_map = self._make_signal_handler_map()
        _set_signal_handlers(signal_handler_map)

        exclude_fds = self._get_exclude_file_descriptors()
        _close_all_open_files(exclude=exclude_fds)

        _redirect_stream(sys.stdin, self.stdin)
        _redirect_stream(sys.stdout, self.stdout)
        _redirect_stream(sys.stderr, self.stderr)

        if self.pidfile is not None:
            self.pidfile.__enter__()

        self._is_open = True
        atexit.register(self.close)

    def close(self) -> None:
        if not self.is_open:
            return
        if self.pidfile is not None:
            self.pidfile.__exit__(None, None, None)
        self._is_open = False

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    # -- signal handling ---------------------------------------------------

    def terminate(self, signal_number, stack_frame):
        raise SystemExit(f"Terminating on signal {signal_number!r}")

    def _make_signal_handler(self, target):
        if target is None:
            return signal.SIG_IGN
        if isinstance(target, str):
            return getattr(self, target)
        return target

    def _make_signal_handler_map(self) -> dict:
        return {
            signal_number: self._make_signal_handler(target)
            for signal_number, target in self.signal_map.items()
        }

    # -- file-descriptor helpers -------------------------------------------

    def _get_exclude_file_descriptors(self) -> set[int]:
        files_preserve = list(self.files_preserve or [])
        files_preserve.extend(
            item for item in (self.stdin, self.stdout, self.stderr) if hasattr(item, "fileno")
        )

        exclude: set[int] = set()
        for item in files_preserve:
            if item is None:
                continue
            fd = _get_file_descriptor(item)
            if fd is not None:
                exclude.add(fd)
            else:
                exclude.add(item)
        return exclude


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def _get_file_descriptor(obj) -> int | None:
    if hasattr(obj, "fileno"):
        try:
            return obj.fileno()
        except ValueError:
            pass
    return None


def _change_working_directory(directory: str) -> None:
    try:
        os.chdir(directory)
    except Exception as exc:
        raise OSError(f"Unable to change working directory ({exc})") from exc


def _change_root_directory(directory: str) -> None:
    try:
        os.chdir(directory)
        os.chroot(directory)
    except Exception as exc:
        raise OSError(f"Unable to change root directory ({exc})") from exc


def _change_file_creation_mask(mask: int) -> None:
    try:
        os.umask(mask)
    except Exception as exc:
        raise OSError(f"Unable to change file creation mask ({exc})") from exc


def _change_process_owner(uid: int, gid: int, initgroups: bool = False) -> None:
    if pwd is None:
        raise OSError("Unable to change process owner (pwd module is not available on this platform)")

    try:
        username = pwd.getpwuid(uid).pw_name
    except KeyError:
        initgroups = False
        username = None

    try:
        if initgroups and username is not None:
            os.initgroups(username, gid)
        else:
            os.setgid(gid)
        os.setuid(uid)
    except Exception as exc:
        raise OSError(f"Unable to change process owner ({exc})") from exc


def _prevent_core_dump() -> None:
    if resource is None:
        raise OSError("System does not support RLIMIT_CORE resource limit (resource module unavailable)")

    core_resource = resource.RLIMIT_CORE
    try:
        resource.getrlimit(core_resource)
    except ValueError as exc:
        raise OSError(f"System does not support RLIMIT_CORE resource limit ({exc})") from exc
    resource.setrlimit(core_resource, (0, 0))


def _detach_process_context() -> None:
    def _fork_then_exit_parent(error_message: str) -> None:
        try:
            pid = os.fork()
            if pid > 0:
                os._exit(0)
        except OSError as exc:
            raise OSError(f"{error_message}: [{exc.errno}] {exc.strerror}") from exc

    _fork_then_exit_parent("Failed first fork")
    os.setsid()
    _fork_then_exit_parent("Failed second fork")


def _is_detach_process_context_required() -> bool:
    if os.getppid() == 1:
        return False
    try:
        if sys.__stdin__ is not None and hasattr(sys.__stdin__, "fileno"):
            fd = sys.__stdin__.fileno()
            with socket.fromfd(fd, socket.AF_INET, socket.SOCK_RAW) as sock:
                sock.getsockopt(socket.SOL_SOCKET, socket.SO_TYPE)
                return False
    except (OSError, ValueError):
        pass
    return True


_MAXFD = 2048


def _get_maximum_file_descriptors() -> int:
    if resource is None:
        return _MAXFD

    __, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
    if hard_limit == resource.RLIM_INFINITY:
        return _MAXFD
    return hard_limit


def _close_all_open_files(exclude: set[int] | None = None) -> None:
    if exclude is None:
        exclude = set()
    maxfd = _get_maximum_file_descriptors()
    ranges: list[tuple[int, int]] = []
    remaining = range(0, maxfd)
    for fd in sorted(exclude):
        if fd > remaining.stop:
            break
        if fd < remaining.start:
            continue
        if fd != remaining.start:
            ranges.append((remaining.start, fd))
        remaining = range(fd + 1, remaining.stop)
    if remaining.start < remaining.stop:
        ranges.append((remaining.start, remaining.stop))
    for low, high in ranges:
        os.closerange(low, high)


def _redirect_stream(system_stream, target_stream) -> None:
    system_fd = system_stream.fileno()
    if target_stream is None:
        target_fd = os.open(os.devnull, os.O_RDWR)
        try:
            if target_fd != system_fd:
                os.dup2(target_fd, system_fd)
        finally:
            if target_fd != system_fd:
                os.close(target_fd)
        return

    os.dup2(target_stream.fileno(), system_fd)


def _make_default_signal_map() -> dict:
    name_map = {
        "SIGTSTP": None,
        "SIGTTIN": None,
        "SIGTTOU": None,
        "SIGTERM": "terminate",
    }
    return {getattr(signal, name): target for name, target in name_map.items() if hasattr(signal, name)}


def _set_signal_handlers(signal_handler_map: dict) -> None:
    for signal_number, handler in signal_handler_map.items():
        signal.signal(signal_number, handler)
