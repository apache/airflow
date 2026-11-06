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

import asyncio
from unittest import mock

import modal
import pytest

from airflow.providers.modal.hooks.modal import ModalHook
from airflow.providers.modal.triggers.modal import ModalSandboxTrigger
from airflow.triggers.base import TriggerEvent


async def _lines(*lines: str):
    for line in lines:
        yield line


class _AioMethod:
    """A Modal SDK method as async code calls it, through ``.aio``; records the calls."""

    def __init__(self, raises: BaseException | None = None) -> None:
        self.calls: list[tuple[tuple, dict]] = []
        self.raises = raises

    async def aio(self, *args, **kwargs) -> None:
        self.calls.append((args, kwargs))
        if self.raises is not None:
            raise self.raises


class _AsyncSandbox:
    """The parts of a ``modal.Sandbox`` handle the trigger uses from the event loop."""

    def __init__(self, *, stderr_lines: list[str], returncode: int | None) -> None:
        self.stderr = _lines(*stderr_lines)
        self.wait = _AioMethod()
        self.terminate = _AioMethod()
        self.returncode = returncode


def test_serialize_round_trips():
    trigger = ModalSandboxTrigger(sandbox_id="sb-1", modal_conn_id="modal_gpu")

    classpath, kwargs = trigger.serialize()

    assert classpath == "airflow.providers.modal.triggers.modal.ModalSandboxTrigger"
    assert kwargs == {"sandbox_id": "sb-1", "modal_conn_id": "modal_gpu"}
    assert ModalSandboxTrigger(**kwargs).serialize() == (classpath, kwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize("returncode", [0, 1, 124])
async def test_run_logs_output_and_fires_once_with_the_exit_code(returncode):
    sandbox = _AsyncSandbox(stderr_lines=["epoch 1\n", "epoch 2\n"], returncode=returncode)
    trigger = ModalSandboxTrigger(sandbox_id="sb-1", modal_conn_id="modal_gpu")

    with (
        mock.patch.object(ModalHook, "aget_sandbox", autospec=True, return_value=sandbox) as aget_sandbox,
        mock.patch.object(type(trigger), "log", new_callable=mock.PropertyMock) as log,
    ):
        events = [event async for event in trigger.run()]

    assert events == [TriggerEvent({"sandbox_id": "sb-1", "returncode": returncode})]
    assert aget_sandbox.call_args.args[1:] == ("sb-1",)
    assert aget_sandbox.call_args.args[0].modal_conn_id == "modal_gpu"
    assert [call.args for call in log.return_value.info.call_args_list] == [
        ("%s", "epoch 1"),
        ("%s", "epoch 2"),
    ]
    assert sandbox.wait.calls == [((), {"raise_on_termination": False})]


@pytest.mark.asyncio
async def test_on_kill_terminates_the_sandbox():
    sandbox = _AsyncSandbox(stderr_lines=[], returncode=None)
    trigger = ModalSandboxTrigger(sandbox_id="sb-1")

    with mock.patch.object(ModalHook, "aget_sandbox", autospec=True, return_value=sandbox):
        await trigger.on_kill()

    assert sandbox.terminate.calls == [((), {})]


@pytest.mark.asyncio
async def test_on_kill_swallows_errors_so_the_triggerer_keeps_going():
    trigger = ModalSandboxTrigger(sandbox_id="sb-gone")

    with mock.patch.object(
        ModalHook, "aget_sandbox", autospec=True, side_effect=modal.exception.NotFoundError("gone")
    ):
        await trigger.on_kill()


@pytest.mark.asyncio
async def test_timeout_fires_an_event_with_exit_code_124():
    # Modal's wait raises SandboxTimeoutError for a timed-out sandbox even with
    # raise_on_termination=False, after recording exit code 124 on the handle.
    sandbox = _AsyncSandbox(stderr_lines=["epoch 1\n"], returncode=124)
    sandbox.wait = _AioMethod(raises=modal.exception.SandboxTimeoutError())
    trigger = ModalSandboxTrigger(sandbox_id="sb-1")

    with mock.patch.object(ModalHook, "aget_sandbox", autospec=True, return_value=sandbox):
        events = [event async for event in trigger.run()]

    assert events == [TriggerEvent({"sandbox_id": "sb-1", "returncode": 124})]
    assert sandbox.terminate.calls == []


async def _stream_ending_with(error: BaseException):
    yield "epoch 1\n"
    raise error


@pytest.mark.asyncio
async def test_losing_the_output_stream_keeps_waiting_for_the_sandbox():
    sandbox = _AsyncSandbox(stderr_lines=[], returncode=0)
    sandbox.stderr = _stream_ending_with(modal.exception.ConnectionError("stream reset"))
    trigger = ModalSandboxTrigger(sandbox_id="sb-1")

    with mock.patch.object(ModalHook, "aget_sandbox", autospec=True, return_value=sandbox):
        events = [event async for event in trigger.run()]

    assert events == [TriggerEvent({"sandbox_id": "sb-1", "returncode": 0})]
    assert sandbox.terminate.calls == []


@pytest.mark.asyncio
async def test_losing_track_of_the_sandbox_terminates_it_and_fails():
    sandbox = _AsyncSandbox(stderr_lines=[], returncode=None)
    sandbox.wait = _AioMethod(raises=modal.exception.ConnectionError("connection lost"))
    trigger = ModalSandboxTrigger(sandbox_id="sb-1")

    with (
        mock.patch.object(ModalHook, "aget_sandbox", autospec=True, return_value=sandbox),
        pytest.raises(modal.exception.ConnectionError),
    ):
        [event async for event in trigger.run()]

    assert sandbox.terminate.calls == [((), {})]


@pytest.mark.asyncio
async def test_cancellation_leaves_the_sandbox_to_the_next_triggerer():
    sandbox = _AsyncSandbox(stderr_lines=[], returncode=None)
    sandbox.stderr = _stream_ending_with(asyncio.CancelledError())
    trigger = ModalSandboxTrigger(sandbox_id="sb-1")

    with (
        mock.patch.object(ModalHook, "aget_sandbox", autospec=True, return_value=sandbox),
        pytest.raises(asyncio.CancelledError),
    ):
        [event async for event in trigger.run()]

    assert sandbox.terminate.calls == []
