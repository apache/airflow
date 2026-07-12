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
import signal
from unittest import mock

import pytest

from airflow.providers.docker.sandbox.cli import AsyncSbxCli, parse_sandbox_list
from airflow.providers.docker.sandbox.exceptions import (
    DockerSandboxCommandError,
    DockerSandboxProtocolError,
)


@pytest.mark.parametrize(
    "raw",
    [
        '[{"id":"stable-id","name":"airflow-id","status":"RUNNING","workspace":"/tmp"}]',
        '{"sandboxes":[{"ID":"stable-id","Name":"airflow-id","Status":"running"}]}',
    ],
)
def test_parse_sandbox_list_accepts_documented_compatible_shapes(raw: str) -> None:
    assert parse_sandbox_list(raw)[0].sandbox_id == "stable-id"
    assert parse_sandbox_list(raw)[0].status == "running"


@pytest.mark.parametrize(
    "raw",
    [
        "not json",
        '{"items":[]}',
        '[{"name":"missing-id","status":"running"}]',
        ('[{"id":"same","name":"one","status":"running"},{"id":"same","name":"two","status":"running"}]'),
    ],
)
def test_parse_sandbox_list_rejects_ambiguous_or_malformed_data(raw: str) -> None:
    with pytest.raises(DockerSandboxProtocolError):
        parse_sandbox_list(raw)


@pytest.mark.asyncio
@mock.patch.object(AsyncSbxCli, "_run", new_callable=mock.AsyncMock)
async def test_health_check_validates_every_reported_component(run: mock.AsyncMock) -> None:
    run.side_effect = ["client v0.36.0\nserver v0.34.9", "[]"]
    cli = AsyncSbxCli(binary="sbx", timeout_seconds=1)

    with pytest.raises(DockerSandboxCommandError, match="0.34.9"):
        await cli.health_check()


@pytest.mark.asyncio
@mock.patch.object(AsyncSbxCli, "_run", new_callable=mock.AsyncMock)
async def test_health_check_requires_semantic_version(run: mock.AsyncMock) -> None:
    run.return_value = "development build"

    with pytest.raises(DockerSandboxProtocolError, match="semantic version"):
        await AsyncSbxCli(binary="sbx", timeout_seconds=1).health_check()


class BlockingProcess:
    def __init__(self) -> None:
        self.stdout = asyncio.StreamReader()
        self.stderr = asyncio.StreamReader()
        self.returncode: int | None = None
        self.pid = 42
        self.stopped = asyncio.Event()

    async def wait(self) -> int:
        await self.stopped.wait()
        return -signal.SIGTERM


@pytest.mark.asyncio
@mock.patch("airflow.providers.docker.sandbox.cli.os.killpg", autospec=True)
@mock.patch("airflow.providers.docker.sandbox.cli.asyncio.create_subprocess_exec", autospec=True)
async def test_cancelled_command_stops_and_reaps_process_group(
    create_subprocess: mock.AsyncMock,
    killpg: mock.MagicMock,
) -> None:
    process = BlockingProcess()
    create_subprocess.return_value = process

    def stop_process_group(pid: int, sig: signal.Signals) -> None:
        assert pid == process.pid
        assert sig is signal.SIGTERM
        process.returncode = -signal.SIGTERM
        process.stopped.set()

    killpg.side_effect = stop_process_group
    task = asyncio.create_task(AsyncSbxCli(binary="sbx", timeout_seconds=60)._run("ls", "--json"))
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    killpg.assert_called_once_with(42, signal.SIGTERM)
    await_args = create_subprocess.await_args
    assert await_args is not None
    assert await_args.kwargs["start_new_session"] is True
