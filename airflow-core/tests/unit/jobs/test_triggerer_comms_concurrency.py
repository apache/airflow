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
import contextlib
import itertools
import queue
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock

import pytest

from airflow.jobs.triggerer_job_runner import TriggerCommsDecoder
from airflow.sdk.execution_time.comms import GetVariable, OKResponse, _new_encoder, _RequestFrame


class MockReader:
    def __init__(self):
        self._queue = queue.Queue()

    async def readexactly(self, n):
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(None, self._queue.get, True, 2)
        except queue.Empty:
            raise EOFError("Mock queue timeout - prevents hanging during test failure")


class SimpleMockWriter:
    """Manual mock of StreamWriter to avoid OSError 88 (Socket operation on non-socket)."""

    def __init__(self):
        self.write_count = 0

    def write(self, data: bytes):
        self.write_count += 1

    async def drain(self):
        await asyncio.sleep(0.01)

    def get_extra_info(self, name, default=None):
        return None

    def close(self):
        pass

    async def wait_closed(self):
        pass


@pytest.mark.asyncio
async def test_trigger_comms_decoder_concurrency():
    """Test that TriggerCommsDecoder.asend is thread-safe when called from multiple threads."""
    mock_reader = MockReader()
    mock_writer = SimpleMockWriter()

    decoder = TriggerCommsDecoder(async_reader=mock_reader, async_writer=mock_writer, socket=MagicMock())
    decoder.id_counter = iter(range(1, 1000))

    responses_sent = 0

    def provide_response(msg_id):
        nonlocal responses_sent
        # We must explicitly set type="OKResponse" because the comms encoder
        # uses exclude_unset=True, and Pydantic requires the discriminator "type".
        resp = OKResponse(ok=True, type="OKResponse")
        frame = _RequestFrame(id=msg_id, body=resp)
        encoder = _new_encoder()
        body = encoder.encode(frame)
        length = len(body).to_bytes(4, byteorder="big")

        mock_reader._queue.put(length)
        mock_reader._queue.put(body)
        responses_sent += 1

    msg = GetVariable(key="test")

    def thread_task():
        return decoder.send(msg)

    async def supervisor_sim():
        for i in range(20):
            while mock_writer.write_count <= i:
                await asyncio.sleep(0.001)
            provide_response(i + 1)

    loop = asyncio.get_running_loop()
    # Increase the default executor's thread limit to prevent deadlock from thread exhaustion
    # during high-concurrency testing.
    loop.set_default_executor(ThreadPoolExecutor(max_workers=50))

    decoder.id_counter = itertools.count(1)
    sim_task = asyncio.create_task(supervisor_sim())

    try:
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [loop.run_in_executor(executor, thread_task) for _ in range(20)]
            results = await asyncio.gather(*futures)
    finally:
        sim_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await sim_task

    assert len(results) == 20
    assert all(isinstance(r, OKResponse) for r in results)
