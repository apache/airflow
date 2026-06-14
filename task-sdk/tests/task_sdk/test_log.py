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
from __future__ import annotations

from unittest import mock

import structlog.testing

from airflow.sdk import log as sdk_log


class TestMaskSecretSupervisorIPC:
    """``mask_secret`` must surface a *safe* warning when it cannot register a secret with the supervisor.

    When the task forwards logs to the supervisor it drops its own ``mask_logs`` processor (see the
    ``sending_to_supervisor=True`` branches in ``log.py``) and relies on the supervisor to do the
    masking. If that registration is lost, silently swallowing the error would leave the secret
    unmasked in supervisor-level logs, so a dedicated warning is emitted instead.

    The warning records only the exception *type* and the secret's *name* -- never the value, the
    exception message, or a traceback -- so the warning itself can never leak the secret it reports
    on. The tests below pin that property down, since the failure path is exactly the one where local
    masking is already disabled.
    """

    def test_warning_never_leaks_the_secret_value(self):
        """If building ``MaskSecret`` raises -- its validation error text can ``repr`` the offending
        value -- the warning must record the failure WITHOUT the value, the message, or a traceback.

        This is the leak Ash flagged: ``exc_info`` / the exception message could carry the secret into
        the task log on the very path where the local ``mask_logs`` processor is already off.
        """
        from airflow.sdk.execution_time import task_runner

        comms = mock.MagicMock()
        canary = "s3cr3t-leak-canary"

        class _Unserializable:
            """Not a ``str``/``dict``/``Iterable`` JSON value, so ``MaskSecret(value=...)`` rejects it.

            Its ``repr`` embeds the secret so that *any* code reprs/serializes it into the log would
            surface the canary -- letting the assertion below catch a regression.
            """

            def __repr__(self) -> str:
                return f"<Unserializable secret={canary!r}>"

        with (
            mock.patch.object(task_runner, "SUPERVISOR_COMMS", comms, create=True),
            structlog.testing.capture_logs() as captured,
        ):
            sdk_log.mask_secret(_Unserializable(), name="password")  # type: ignore[arg-type]

        events = [e for e in captured if e["event"] == "supervisor_mask_secret_failed"]
        assert len(events) == 1
        ev = events[0]
        assert ev["log_level"] == "warning"
        assert ev["secret_name"] == "password"
        assert ev["error_type"]  # the exception class name, e.g. "ValidationError"
        assert "exc_info" not in ev  # no traceback that could carry the value
        # The crucial property: the secret value never appears anywhere in the emitted warning.
        assert canary not in repr(ev)
        # The build failed before the socket was touched -- the supervisor is never contacted.
        comms.send.assert_not_called()

    def test_warns_when_supervisor_send_fails(self):
        """A broken IPC send (e.g. ``BrokenPipeError``) is reported as a grep-able warning -- and,
        again, only the exception *type* is recorded, never the value."""
        from airflow.sdk.execution_time import task_runner

        comms = mock.MagicMock()
        comms.send.side_effect = BrokenPipeError("supervisor connection lost")

        with (
            mock.patch.object(task_runner, "SUPERVISOR_COMMS", comms, create=True),
            structlog.testing.capture_logs() as captured,
        ):
            sdk_log.mask_secret("hunter2", name="password")

        events = [e for e in captured if e["event"] == "supervisor_mask_secret_failed"]
        assert len(events) == 1
        ev = events[0]
        assert ev["log_level"] == "warning"
        assert ev["secret_name"] == "password"
        assert ev["error_type"] == "BrokenPipeError"
        assert "exc_info" not in ev
        assert "hunter2" not in repr(ev)
        comms.send.assert_called_once()

    def test_silent_when_supervisor_send_succeeds(self):
        from airflow.sdk.execution_time import task_runner

        comms = mock.MagicMock()

        with (
            mock.patch.object(task_runner, "SUPERVISOR_COMMS", comms, create=True),
            structlog.testing.capture_logs() as captured,
        ):
            sdk_log.mask_secret("hunter2", name="password")

        events = [e for e in captured if e["event"] == "supervisor_mask_secret_failed"]
        assert events == []
        comms.send.assert_called_once()

    def test_silent_when_not_running_under_a_supervisor(self):
        """With no ``SUPERVISOR_COMMS`` (the task is not running under a supervisor) the mirror is a
        no-op -- there is nothing to notify and nothing to warn about."""
        from airflow.sdk.execution_time import task_runner

        with (
            mock.patch.object(task_runner, "SUPERVISOR_COMMS", None, create=True),
            structlog.testing.capture_logs() as captured,
        ):
            sdk_log.mask_secret("hunter2", name="password")

        events = [e for e in captured if e["event"] == "supervisor_mask_secret_failed"]
        assert events == []
