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
import resource
import signal
import sys
from unittest import mock

import pytest

from airflow.utils.daemon import (
    DaemonContext,
    _change_file_creation_mask,
    _change_working_directory,
    _close_all_open_files,
    _get_file_descriptor,
    _get_maximum_file_descriptors,
    _is_detach_process_context_required,
    _make_default_signal_map,
    _prevent_core_dump,
    _redirect_stream,
    _set_signal_handlers,
)


class TestGetFileDescriptor:
    def test_returns_fd_for_file_like_object(self, tmp_path):
        path = tmp_path / "test.txt"
        path.write_text("hello")
        with open(str(path)) as f:
            assert _get_file_descriptor(f) == f.fileno()

    def test_returns_none_for_non_file_object(self):
        assert _get_file_descriptor("not-a-file") is None

    def test_returns_none_for_closed_file(self, tmp_path):
        path = tmp_path / "test.txt"
        path.write_text("hello")
        f = open(str(path))
        f.close()
        assert _get_file_descriptor(f) is None


class TestChangeWorkingDirectory:
    def test_changes_cwd(self, tmp_path):
        original = os.getcwd()
        try:
            _change_working_directory(str(tmp_path))
            assert os.getcwd() == str(tmp_path)
        finally:
            os.chdir(original)

    def test_raises_on_invalid_directory(self):
        with pytest.raises(OSError, match="Unable to change working directory"):
            _change_working_directory("/nonexistent_dir_abc123")


class TestChangeFileCreationMask:
    def test_sets_umask(self):
        old = os.umask(0o022)
        os.umask(old)
        try:
            _change_file_creation_mask(0o077)
            assert os.umask(0o077) == 0o077
        finally:
            os.umask(old)


class TestPreventCoreDump:
    def test_sets_core_limit_to_zero(self):
        original = resource.getrlimit(resource.RLIMIT_CORE)
        try:
            _prevent_core_dump()
            soft, hard = resource.getrlimit(resource.RLIMIT_CORE)
            assert soft == 0
            assert hard == 0
        finally:
            try:
                resource.setrlimit(resource.RLIMIT_CORE, original)
            except ValueError:
                pass


class TestGetMaximumFileDescriptors:
    def test_returns_hard_limit(self):
        __, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        result = _get_maximum_file_descriptors()
        if hard == resource.RLIM_INFINITY:
            assert result == 2048
        else:
            assert result == hard


class TestCloseAllOpenFiles:
    def test_closes_file_descriptors_except_excluded(self):
        r_fd, w_fd = os.pipe()
        try:
            _close_all_open_files(exclude={r_fd, w_fd, 0, 1, 2})
            os.write(w_fd, b"alive")
            assert os.read(r_fd, 5) == b"alive"
        finally:
            os.close(r_fd)
            os.close(w_fd)

    def test_empty_exclude_does_not_raise(self):
        _close_all_open_files(exclude={0, 1, 2})


class TestRedirectStream:
    def test_redirects_to_target(self, tmp_path):
        target_path = tmp_path / "output.txt"
        with open(str(target_path), "w") as target:
            original_fd = os.dup(1)
            try:
                _redirect_stream(sys.stdout, target)
                os.write(1, b"redirected\n")
            finally:
                os.dup2(original_fd, 1)
                os.close(original_fd)
        assert "redirected" in target_path.read_text()

    def test_redirects_to_devnull_when_none(self):
        original_fd = os.dup(sys.stdout.fileno())
        try:
            _redirect_stream(sys.stdout, None)
        finally:
            os.dup2(original_fd, sys.stdout.fileno())
            os.close(original_fd)


class TestMakeDefaultSignalMap:
    def test_contains_sigterm(self):
        signal_map = _make_default_signal_map()
        assert signal.SIGTERM in signal_map
        assert signal_map[signal.SIGTERM] == "terminate"

    def test_contains_sigtstp_as_none(self):
        signal_map = _make_default_signal_map()
        if hasattr(signal, "SIGTSTP"):
            assert signal_map[signal.SIGTSTP] is None


class TestSetSignalHandlers:
    @mock.patch("signal.signal")
    def test_installs_handlers(self, mock_signal):
        handler = mock.MagicMock()
        _set_signal_handlers({signal.SIGUSR1: handler})
        mock_signal.assert_called_once_with(signal.SIGUSR1, handler)


class TestIsDetachProcessContextRequired:
    def test_returns_true_in_normal_process(self):
        assert _is_detach_process_context_required() is True

    @mock.patch("os.getppid", return_value=1)
    def test_returns_false_when_parent_is_init(self, _mock_getppid):
        assert _is_detach_process_context_required() is False


class TestDaemonContext:
    def test_defaults(self):
        ctx = DaemonContext(detach_process=False)
        assert ctx.working_directory == "/"
        assert ctx.umask == 0
        assert ctx.prevent_core is True
        assert ctx.pidfile is None
        assert ctx.stdin is None
        assert ctx.stdout is None
        assert ctx.stderr is None
        assert ctx.uid == os.getuid()
        assert ctx.gid == os.getgid()
        assert not ctx.is_open

    def test_is_open_initially_false(self):
        ctx = DaemonContext(detach_process=False)
        assert ctx.is_open is False

    def test_close_when_not_open_is_noop(self):
        ctx = DaemonContext(detach_process=False)
        ctx.close()
        assert ctx.is_open is False

    def test_open_twice_is_idempotent(self):
        with mock.patch.multiple(
            "airflow.utils.daemon",
            _prevent_core_dump=mock.DEFAULT,
            _change_file_creation_mask=mock.DEFAULT,
            _change_working_directory=mock.DEFAULT,
            _change_process_owner=mock.DEFAULT,
            _close_all_open_files=mock.DEFAULT,
            _redirect_stream=mock.DEFAULT,
            _set_signal_handlers=mock.DEFAULT,
        ) as mocks:
            ctx = DaemonContext(detach_process=False)
            ctx.open()
            call_count = mocks["_prevent_core_dump"].call_count
            ctx.open()
            assert mocks["_prevent_core_dump"].call_count == call_count
            ctx.close()

    def test_context_manager_opens_and_closes(self):
        with mock.patch.multiple(
            "airflow.utils.daemon",
            _prevent_core_dump=mock.DEFAULT,
            _change_file_creation_mask=mock.DEFAULT,
            _change_working_directory=mock.DEFAULT,
            _change_process_owner=mock.DEFAULT,
            _close_all_open_files=mock.DEFAULT,
            _redirect_stream=mock.DEFAULT,
            _set_signal_handlers=mock.DEFAULT,
        ):
            ctx = DaemonContext(detach_process=False)
            with ctx:
                assert ctx.is_open is True
            assert ctx.is_open is False

    def test_pidfile_entered_and_exited(self):
        mock_pidfile = mock.MagicMock()
        with mock.patch.multiple(
            "airflow.utils.daemon",
            _prevent_core_dump=mock.DEFAULT,
            _change_file_creation_mask=mock.DEFAULT,
            _change_working_directory=mock.DEFAULT,
            _change_process_owner=mock.DEFAULT,
            _close_all_open_files=mock.DEFAULT,
            _redirect_stream=mock.DEFAULT,
            _set_signal_handlers=mock.DEFAULT,
        ):
            ctx = DaemonContext(detach_process=False, pidfile=mock_pidfile)
            with ctx:
                mock_pidfile.__enter__.assert_called_once()
            mock_pidfile.__exit__.assert_called_once_with(None, None, None)

    def test_terminate_raises_system_exit(self):
        ctx = DaemonContext(detach_process=False)
        with pytest.raises(SystemExit, match="Terminating on signal"):
            ctx.terminate(signal.SIGTERM, None)

    def test_signal_map_none_becomes_sig_ign(self):
        ctx = DaemonContext(
            detach_process=False,
            signal_map={signal.SIGUSR1: None},
        )
        handler_map = ctx._make_signal_handler_map()
        assert handler_map[signal.SIGUSR1] is signal.SIG_IGN

    def test_signal_map_string_becomes_method(self):
        ctx = DaemonContext(
            detach_process=False,
            signal_map={signal.SIGTERM: "terminate"},
        )
        handler_map = ctx._make_signal_handler_map()
        assert handler_map[signal.SIGTERM] == ctx.terminate

    def test_signal_map_callable_passed_through(self):
        handler = mock.MagicMock()
        ctx = DaemonContext(
            detach_process=False,
            signal_map={signal.SIGUSR1: handler},
        )
        handler_map = ctx._make_signal_handler_map()
        assert handler_map[signal.SIGUSR1] is handler

    def test_exclude_fds_includes_stdio_streams(self):
        stdout_mock = mock.MagicMock()
        stdout_mock.fileno.return_value = 42
        stderr_mock = mock.MagicMock()
        stderr_mock.fileno.return_value = 43
        ctx = DaemonContext(
            detach_process=False,
            stdout=stdout_mock,
            stderr=stderr_mock,
        )
        fds = ctx._get_exclude_file_descriptors()
        assert 42 in fds
        assert 43 in fds

    def test_exclude_fds_includes_files_preserve(self):
        f = mock.MagicMock()
        f.fileno.return_value = 10
        ctx = DaemonContext(
            detach_process=False,
            files_preserve=[f],
        )
        fds = ctx._get_exclude_file_descriptors()
        assert 10 in fds

    def test_exclude_fds_handles_raw_int_items(self):
        ctx = DaemonContext(
            detach_process=False,
            files_preserve=[7],
        )
        fds = ctx._get_exclude_file_descriptors()
        assert 7 in fds

    def test_open_calls_expected_steps(self):
        with mock.patch.multiple(
            "airflow.utils.daemon",
            _prevent_core_dump=mock.DEFAULT,
            _change_file_creation_mask=mock.DEFAULT,
            _change_working_directory=mock.DEFAULT,
            _change_process_owner=mock.DEFAULT,
            _close_all_open_files=mock.DEFAULT,
            _redirect_stream=mock.DEFAULT,
            _set_signal_handlers=mock.DEFAULT,
        ) as mocks:
            ctx = DaemonContext(detach_process=False, umask=0o077, working_directory="/tmp")
            ctx.open()
            try:
                mocks["_prevent_core_dump"].assert_called_once()
                mocks["_change_file_creation_mask"].assert_called_once_with(0o077)
                mocks["_change_working_directory"].assert_called_once_with("/tmp")
                mocks["_change_process_owner"].assert_called_once()
                mocks["_close_all_open_files"].assert_called_once()
                assert mocks["_redirect_stream"].call_count == 3
                mocks["_set_signal_handlers"].assert_called_once()
            finally:
                ctx.close()

    def test_detach_process_called_when_enabled(self):
        with mock.patch.multiple(
            "airflow.utils.daemon",
            _prevent_core_dump=mock.DEFAULT,
            _change_file_creation_mask=mock.DEFAULT,
            _change_working_directory=mock.DEFAULT,
            _change_process_owner=mock.DEFAULT,
            _detach_process_context=mock.DEFAULT,
            _close_all_open_files=mock.DEFAULT,
            _redirect_stream=mock.DEFAULT,
            _set_signal_handlers=mock.DEFAULT,
        ) as mocks:
            ctx = DaemonContext(detach_process=True)
            ctx.open()
            try:
                mocks["_detach_process_context"].assert_called_once()
            finally:
                ctx.close()

    def test_prevent_core_skipped_when_disabled(self):
        with mock.patch.multiple(
            "airflow.utils.daemon",
            _prevent_core_dump=mock.DEFAULT,
            _change_file_creation_mask=mock.DEFAULT,
            _change_working_directory=mock.DEFAULT,
            _change_process_owner=mock.DEFAULT,
            _close_all_open_files=mock.DEFAULT,
            _redirect_stream=mock.DEFAULT,
            _set_signal_handlers=mock.DEFAULT,
        ) as mocks:
            ctx = DaemonContext(detach_process=False, prevent_core=False)
            ctx.open()
            try:
                mocks["_prevent_core_dump"].assert_not_called()
            finally:
                ctx.close()
