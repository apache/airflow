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
    """``mask_secret`` must surface a warning when it cannot register a secret with the supervisor.

    When the task forwards logs to the supervisor it drops its own ``mask_logs`` processor (see the
    ``sending_to_supervisor=True`` branches in ``log.py``) and relies on the supervisor to do the
    masking. So if the registration is lost -- whether the ``MaskSecret`` message cannot be built /
    serialized, or the IPC socket is broken -- silently swallowing the error would leave the secret
    unmasked in supervisor-level logs, so a dedicated warning is emitted instead.
    """

    def test_warns_when_mask_message_cannot_be_serialized(self):
        """Realistic, supervisor-alive failure: the ``MaskSecret`` message cannot even be built.

        A value that is not JSON-serializable fails ``MaskSecret`` validation *before* the socket is
        touched -- the supervisor is alive and reachable, ``send`` is never reached, and the warning
        is the only signal that the secret was never registered. (This is the realistic counterpart
        to a full supervisor crash, where the warning could not be delivered anyway.)
        """
        from airflow.sdk.execution_time import task_runner

        comms = mock.MagicMock()

        class _Unserializable:
            """Not a ``str``/``dict``/``Iterable``, so ``MaskSecret(value=...)`` rejects it."""

        with (
            mock.patch.object(task_runner, "SUPERVISOR_COMMS", comms, create=True),
            structlog.testing.capture_logs() as captured,
        ):
            sdk_log.mask_secret(_Unserializable(), name="password")

        events = [e for e in captured if e["event"] == "supervisor_mask_secret_failed"]
        assert len(events) == 1
        assert events[0]["log_level"] == "warning"
        assert events[0]["secret_name"] == "password"
        assert events[0]["exc_info"]  # structlog renders exc_info=True as True in capture_logs
        # The failure is in building the message, not the socket -- the supervisor is never contacted.
        comms.send.assert_not_called()

    def test_warns_when_supervisor_socket_is_broken(self):
        """The IPC socket itself fails (e.g. a broken connection) -- still warn rather than swallow."""
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
        assert events[0]["log_level"] == "warning"
        assert events[0]["secret_name"] == "password"
        assert events[0]["exc_info"]
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

    def test_silent_when_no_supervisor_context(self):
        """Outside a task-execution context (no SUPERVISOR_COMMS) the function is a no-op."""
        from airflow.sdk.execution_time import task_runner

        with (
            mock.patch.object(task_runner, "SUPERVISOR_COMMS", None, create=True),
            structlog.testing.capture_logs() as captured,
        ):
            sdk_log.mask_secret("hunter2", name="password")

        events = [e for e in captured if e["event"] == "supervisor_mask_secret_failed"]
        assert events == []
