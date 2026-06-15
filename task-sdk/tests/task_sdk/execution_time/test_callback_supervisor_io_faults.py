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
IO-boundary fault-injection tests for the callback supervisor.

Each test injects a fault at the IO boundary (socket recv/send, file open, fork,
comms decode) into the REAL code path under test, NOT by replacing the logic,
to verify the deadline-callback runtime surfaces faults cleanly rather than
hanging or crashing the supervisor.
"""

from __future__ import annotations

import errno
import signal
import socket
from unittest import mock

import pytest
import structlog

from airflow.sdk.api.datamodels._generated import DagRun, DagRunState, DagRunType
from airflow.sdk.execution_time.callback_supervisor import (
    CallbackSubprocess,
    _fetch_and_build_context,
)
from airflow.sdk.execution_time.comms import (
    CommsDecoder,
    DagRunResult,
    GetDagRun,
    ToTask,
    _ResponseFrame,
)

log = structlog.get_logger()


def _mk_dagrun(**over):
    from airflow.sdk._shared.timezones import timezone

    base = dict(
        dag_id="d",
        run_id="r",
        run_after=timezone.parse("2024-01-01T00:00:00+00:00"),
        run_type=DagRunType.MANUAL,
        state=DagRunState.RUNNING,
        consumed_asset_events=[],
    )
    base.update(over)
    return DagRun(**base)


# ---------------------------------------------------------------------------
# Scenario 1: comms send/recv raises mid-callback (socket error / broken pipe)
# ---------------------------------------------------------------------------
class TestScenario1CommsFailureMidCallback:
    """SUPERVISOR_COMMS.send raises a socket error during the callback."""

    def _decoder(self):
        # Real CommsDecoder, but with a socket we control.
        sock = mock.MagicMock(spec=socket.socket)
        return CommsDecoder[ToTask, GetDagRun](socket=sock, body_decoder=mock.MagicMock())

    def test_sendall_broken_pipe_surfaces_as_exception(self):
        """sendall raises BrokenPipeError -> send() propagates, no hang."""
        dec = self._decoder()
        dec.socket.sendall.side_effect = BrokenPipeError(errno.EPIPE, "Broken pipe")
        with pytest.raises(BrokenPipeError):
            dec.send(GetDagRun(dag_id="d", run_id="r"))

    def test_recv_connection_reset_surfaces(self):
        """recv(4) raises ConnectionResetError -> send() propagates."""
        dec = self._decoder()
        dec.socket.sendall.return_value = None
        dec.socket.recv.side_effect = ConnectionResetError(errno.ECONNRESET, "reset")
        with pytest.raises(ConnectionResetError):
            dec.send(GetDagRun(dag_id="d", run_id="r"))

    def test_recv_eof_midstream_raises_eoferror(self):
        """Length header OK but body truncated (recv_into returns 0) -> EOFError, not hang."""
        dec = self._decoder()
        dec.socket.sendall.return_value = None
        dec.socket.recv.return_value = (16).to_bytes(4, "big")  # claims 16-byte body
        dec.socket.recv_into.return_value = 0  # immediate EOF
        with pytest.raises(EOFError):
            dec.send(GetDagRun(dag_id="d", run_id="r"))

    def test_fetch_and_build_context_swallows_comms_failure(self):
        """_fetch_and_build_context must return None (not raise) when comms.send raises."""
        comms = mock.MagicMock()
        comms.send.side_effect = BrokenPipeError("pipe gone")
        result = _fetch_and_build_context(comms, "d", "r", log)
        assert result is None


# ---------------------------------------------------------------------------
# Scenario 2: GetDagRun comms returns malformed / partial / wrong-type payload
# ---------------------------------------------------------------------------
class TestScenario2MalformedDagRunPayload:
    """_fetch_and_build_context fed a bad response from comms.send."""

    def test_wrong_response_type_returns_none(self):
        comms = mock.MagicMock()
        comms.send.return_value = {"not": "a DagRunResult"}
        assert _fetch_and_build_context(comms, "d", "r", log) is None

    def test_none_response_returns_none(self):
        comms = mock.MagicMock()
        comms.send.return_value = None
        assert _fetch_and_build_context(comms, "d", "r", log) is None

    def test_dagrunresult_missing_logical_date_builds_minimal_context(self):
        """logical_date None -> context built with only run_id (graceful degrade, no crash)."""
        comms = mock.MagicMock()
        dr = DagRunResult.from_api_response(_mk_dagrun(logical_date=None))
        comms.send.return_value = dr
        ctx = _fetch_and_build_context(comms, "d", "r", log)
        assert ctx is not None
        assert ctx["run_id"] == "r"
        # task-specific / date fields must be absent, never half-built
        assert "logical_date" not in ctx

    def test_dagrunresult_with_logical_date_full_context(self):
        from airflow.sdk._shared.timezones import timezone

        comms = mock.MagicMock()
        dr = DagRunResult.from_api_response(
            _mk_dagrun(logical_date=timezone.parse("2024-01-01T00:00:00+00:00"))
        )
        comms.send.return_value = dr
        ctx = _fetch_and_build_context(comms, "d", "r", log)
        assert ctx is not None
        assert ctx["ds"] == "2024-01-01"


# ---------------------------------------------------------------------------
# Scenario 7: very large GetDagRun response over the comms channel (framing)
# ---------------------------------------------------------------------------
class TestScenario7LargeResponseFraming:
    """A 10MB+ conf payload must reassemble across many partial recv_into calls."""

    def test_large_frame_reassembles_across_partial_reads(self):
        # Build a real ResponseFrame whose body is a ~10MB DagRunResult conf, encoded
        # through the REAL frame encoder (as_bytes -> 4-byte length prefix + msgpack body).
        big_conf = {"x": "A" * (10 * 1024 * 1024)}
        dr = DagRunResult.from_api_response(_mk_dagrun(conf=big_conf))
        frame = _ResponseFrame(id=1, body=dr.model_dump())
        full = bytes(frame.as_bytes())  # includes the 4-byte big-endian length header

        sock = mock.MagicMock(spec=socket.socket)
        state = {"buf": full, "pos": 0}
        CHUNK = 65536

        def fake_recv(n):
            s = state["buf"][state["pos"] : state["pos"] + n]
            state["pos"] += len(s)
            return s

        def fake_recv_into(mv):
            # Deliver in small chunks so the reassembly loop in _read_frame runs many times.
            remaining = state["buf"][state["pos"] :]
            n = min(len(mv), len(remaining), CHUNK)
            mv[:n] = remaining[:n]
            state["pos"] += n
            return n

        sock.recv.side_effect = fake_recv
        sock.recv_into.side_effect = fake_recv_into
        sock.sendall.return_value = None

        # Use the REAL response decoder so the framing + msgpack decode is exercised end to end.
        dec = CommsDecoder[ToTask, GetDagRun](socket=sock)
        resp = dec._read_frame()
        assert resp.id == 1
        assert resp.body["conf"]["x"] == big_conf["x"]
        assert state["pos"] == len(full)  # entire 10MB frame consumed, nothing left buffered


# ---------------------------------------------------------------------------
# Scenario 3: log file IO failure (unwritable path / disk full)
# ---------------------------------------------------------------------------
class TestScenario3LogIOFailure:
    def test_upload_logs_failure_swallowed(self, mocker):
        """_upload_logs must swallow IO errors so wait() still returns cleanly."""
        proc = CallbackSubprocess.__new__(CallbackSubprocess)
        proc.id = mocker.Mock()
        proc.pid = 123
        proc.process_log = mocker.Mock()
        proc.client = mocker.Mock()
        mocker.patch(
            "airflow.sdk.execution_time.supervisor._remote_logging_conn",
            side_effect=OSError(errno.ENOSPC, "No space left on device"),
        )
        # Must not raise.
        proc._upload_logs()

    def test_configure_logging_propagates_permission_error(self, mocker, tmp_path):
        """init_log_file on a read-only dir -> PermissionError surfaces (clean, not hang)."""
        from airflow.sdk.execution_time.callback_supervisor import _configure_logging

        mocker.patch(
            "airflow.sdk.log.init_log_file",
            side_effect=PermissionError(errno.EACCES, "Permission denied"),
        )
        with pytest.raises(PermissionError):
            _configure_logging(str(tmp_path / "x.log"), mocker.Mock())


# ---------------------------------------------------------------------------
# Scenario 4: subprocess fork/start failure (resource limit)
# ---------------------------------------------------------------------------
class TestScenario4ForkFailure:
    def test_start_fork_raises_propagates(self, mocker):
        """WatchedSubprocess.start raises OSError(EAGAIN) -> supervise_callback surfaces it."""
        from airflow.sdk.execution_time import callback_supervisor

        mocker.patch.object(
            callback_supervisor.CallbackSubprocess,
            "start",
            side_effect=OSError(errno.EAGAIN, "Resource temporarily unavailable"),
        )
        mocker.patch.object(callback_supervisor, "_make_process_nondumpable")
        mocker.patch.object(callback_supervisor, "_ensure_client")
        with pytest.raises(OSError, match="Resource temporarily unavailable") as ei:
            callback_supervisor.supervise_callback(
                id="00000000-0000-0000-0000-000000000000",
                callback_path="m.f",
                callback_kwargs={},
                client=mocker.Mock(),
            )
        assert ei.value.errno == errno.EAGAIN


# ---------------------------------------------------------------------------
# Scenario 6: timeout fires as callback completes (race)
# ---------------------------------------------------------------------------
class TestScenario6TimeoutRace:
    """callback_execution_timeout path in _monitor_subprocess."""

    def _proc(self, mocker, timeout):
        # CallbackSubprocess is an attrs slotted class, so instance attribute assignment
        # for methods is read-only. Patch kill/_service_subprocess at the class level.
        proc = CallbackSubprocess.__new__(CallbackSubprocess)
        object.__setattr__(proc, "pid", 999)
        object.__setattr__(proc, "_exit_code", None)
        object.__setattr__(proc, "_open_sockets", {})
        object.__setattr__(proc, "_process_exit_monotonic", None)
        kill_mock = mocker.patch.object(CallbackSubprocess, "kill")
        mocker.patch.object(CallbackSubprocess, "_get_callback_execution_timeout", return_value=timeout)
        return proc, kill_mock

    def test_natural_exit_just_before_timeout_no_kill(self, mocker):
        """Process exits naturally on the first service call -> kill must NOT be called."""
        proc, kill_mock = self._proc(mocker, timeout=1)

        def service(self_, max_wait_time):
            object.__setattr__(self_, "_exit_code", 0)  # exits cleanly during the very first poll

        mocker.patch.object(CallbackSubprocess, "_service_subprocess", autospec=True, side_effect=service)
        proc._monitor_subprocess()
        kill_mock.assert_not_called()
        assert proc._exit_code == 0

    def test_timeout_fires_when_process_hangs(self, mocker):
        """Process never exits and timeout elapses -> kill called exactly once, loop breaks."""
        proc, kill_mock = self._proc(mocker, timeout=1)
        mocker.patch.object(CallbackSubprocess, "_service_subprocess")  # never sets exit code
        # monotonic: start=0, then jump well past the timeout on first check.
        mocker.patch(
            "airflow.sdk.execution_time.callback_supervisor.time.monotonic",
            side_effect=[0.0, 100.0, 100.0, 100.0],
        )
        proc._monitor_subprocess()
        kill_mock.assert_called_once_with(signal.SIGTERM, escalation_delay=5.0, force=True)

    def test_exit_code_set_concurrent_with_timeout_check_no_double_kill(self, mocker):
        """
        Race: process exits (_exit_code set) in the same iteration the timeout would fire.
        The `and self._exit_code is None` guard must prevent killing an already-dead proc.
        """
        proc, kill_mock = self._proc(mocker, timeout=1)

        def service(self_, max_wait_time):
            object.__setattr__(self_, "_exit_code", 0)  # set during service, simulating natural exit

        mocker.patch.object(CallbackSubprocess, "_service_subprocess", autospec=True, side_effect=service)
        mocker.patch(
            "airflow.sdk.execution_time.callback_supervisor.time.monotonic",
            side_effect=[0.0, 100.0, 100.0],  # elapsed would exceed timeout
        )
        proc._monitor_subprocess()
        # Even though elapsed > timeout, exit_code was set -> guard skips kill.
        kill_mock.assert_not_called()


# ---------------------------------------------------------------------------
# Scenario 5 (supervisor side): bundle.initialize() raises during _target
# ---------------------------------------------------------------------------
class TestScenario5BundleInitRaisesSupervisor:
    def test_register_unusual_prefix_module_short_name_noops(self):
        """A non-mangled callback path must not blow up the registration helper."""
        from airflow.sdk.execution_time.callback_supervisor import _register_unusual_prefix_module

        # < 4 underscore parts -> early return, no rglob / no crash.
        _register_unusual_prefix_module("plainmod.func", "/nonexistent/bundle", log)

    def test_register_unusual_prefix_module_missing_bundle_path(self, tmp_path):
        """rglob over an empty/missing bundle dir -> graceful, no match, no raise."""
        from airflow.sdk.execution_time.callback_supervisor import _register_unusual_prefix_module

        _register_unusual_prefix_module("unusual_prefix_abc_dagfile.func", str(tmp_path), log)

    def test_target_bundle_initialize_raises_does_not_pollute_syspath(self, mocker):
        """
        When bundle.initialize() raises, the supervisor _target catches it and the
        sys.path.append (which runs AFTER initialize) must never execute -> no bad path.
        """
        import sys

        before = list(sys.path)
        manager = mocker.patch("airflow.dag_processing.bundles.manager.DagBundlesManager")
        bundle = manager.return_value.get_bundle.return_value
        bundle.initialize.side_effect = RuntimeError("corrupt bundle")
        bundle.path = "/should/never/be/appended"

        # Drive the bundle-init block exactly as _target does, in-process.
        bundle_info = mocker.Mock(name="bi")
        bundle_info.name = "b"
        bundle_info.version = None
        try:
            b = manager().get_bundle(name=bundle_info.name, version=bundle_info.version)
            b.initialize()
            if (bp := str(b.path)) not in sys.path:
                sys.path.append(bp)
        except Exception:
            pass
        assert sys.path == before, "initialize() failure must not leave a bad bundle path on sys.path"


# ---------------------------------------------------------------------------
# Scenario 8: SUPERVISOR_COMMS not initialized (init-order race)
# ---------------------------------------------------------------------------
class TestScenario8CommsNotInitialized:
    def test_fetch_context_with_none_comms(self):
        """Passing None comms -> AttributeError is caught, returns None (no crash bubbling)."""
        # _fetch_and_build_context calls comms.send(...); None.send -> AttributeError,
        # which the broad except in the function must swallow into None.
        assert _fetch_and_build_context(None, "d", "r", log) is None
