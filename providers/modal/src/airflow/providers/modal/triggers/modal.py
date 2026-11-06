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
"""Trigger that waits for a ``@task.modal`` sandbox to exit."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from airflow.providers.modal.hooks.modal import ModalHook
from airflow.providers.modal.utils.python_sandbox import await_exit
from airflow.triggers.base import BaseTrigger, TriggerEvent

if TYPE_CHECKING:
    import modal


class ModalSandboxTrigger(BaseTrigger):
    """
    Wait for a ``@task.modal`` sandbox to exit, writing its output to the task log.

    The trigger logs the function's output (the sandbox's stderr) line by line, waits for the
    sandbox to exit, and then fires one event with the exit code. The operator reads the return
    value in ``execute_complete``. Losing the output stream only stops the logging.

    If the triggerer restarts, the trigger that takes over reads the output again from the
    oldest part Modal still holds, so the task log can repeat lines logged before the restart.

    :param sandbox_id: The ``sb-...`` id of the sandbox to wait for.
    :param modal_conn_id: :ref:`Modal connection id <howto/connection:modal>` the sandbox was
        created through.
    """

    def __init__(self, sandbox_id: str, modal_conn_id: str | None = ModalHook.default_conn_name) -> None:
        super().__init__()
        self.sandbox_id = sandbox_id
        self.modal_conn_id = modal_conn_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            f"{type(self).__module__}.{type(self).__qualname__}",
            {"sandbox_id": self.sandbox_id, "modal_conn_id": self.modal_conn_id},
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        try:
            sandbox = await ModalHook(modal_conn_id=self.modal_conn_id).aget_sandbox(self.sandbox_id)
            await self._log_output(sandbox)
            await await_exit(sandbox)
        except Exception:
            # The task fails with this error and a retry starts a new sandbox, so stop this one.
            # Cancellation is not an Exception and passes through. On triggerer shutdown or
            # reassignment another triggerer takes the sandbox over; on the trigger's timeout,
            # the sandbox's Modal timeout, never longer than execution_timeout, stops it.
            await self._terminate()
            raise
        yield TriggerEvent({"sandbox_id": self.sandbox_id, "returncode": sandbox.returncode})

    async def _log_output(self, sandbox: modal.Sandbox) -> None:
        try:
            async for line in sandbox.stderr:
                self.log.info("%s", line.rstrip("\n"))
        except Exception as e:
            # Modal's reader raises once its retries for stream errors run out. Losing the
            # output is no reason to stop a function that is still running.
            self.log.warning(
                "Lost the output of Modal sandbox %s (%s); waiting for it to exit.", self.sandbox_id, e
            )

    async def on_kill(self) -> None:
        """
        Terminate the sandbox when a user kills the deferred task.

        Runs on Airflow 3.3+, which calls it when a deferred task is marked failed, marked
        success or cleared; older versions never call it. Without it, a sandbox whose task was
        killed keeps running, and billing, until its own ``timeout``.
        """
        await self._terminate()

    async def _terminate(self) -> None:
        try:
            sandbox = await ModalHook(modal_conn_id=self.modal_conn_id).aget_sandbox(self.sandbox_id)
            await sandbox.terminate.aio()
            self.log.info("Terminated Modal sandbox %s", self.sandbox_id)
        except Exception as e:
            self.log.warning("Failed to terminate Modal sandbox %s: %s", self.sandbox_id, e)
