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
Micro-benchmarks of the task's channel to its supervisor.

Every SDK call a task makes (a Variable, a Connection, an XCom, a state store key) is one round
trip through ``CommsDecoder``. Async tasks make them concurrently on one event loop, and an
iterated task makes them for every item, so the cost of one round trip multiplies with the
number of calls. The supervisor side is a thread on the other end of a socket pair that answers
each request at once, so what is measured is the decoder: framing, locking and the thread hops.
"""

from __future__ import annotations

import asyncio
import socket
import threading
from collections.abc import Iterator

import msgspec
import pytest
import structlog

from airflow.sdk.execution_time.comms import CommsDecoder, GetVariable, _RequestFrame, _ResponseFrame


def _recv_exactly(sock: socket.socket, size: int) -> bytes:
    buffer = bytearray()
    while len(buffer) < size:
        chunk = sock.recv(size - len(buffer))
        if not chunk:
            raise EOFError
        buffer.extend(chunk)
    return bytes(buffer)


def _answer_every_request(sock: socket.socket) -> None:
    """Act as the supervisor: answer each ``GetVariable`` with its key, until the socket closes."""
    try:
        while True:
            length = int.from_bytes(_recv_exactly(sock, 4), "big")
            request = msgspec.msgpack.decode(_recv_exactly(sock, length), type=_RequestFrame)
            assert request.body is not None
            response = {"type": "VariableResult", "key": request.body["key"], "value": "v"}
            encoded = msgspec.msgpack.encode(_ResponseFrame(request.id, response, None))
            sock.sendall(len(encoded).to_bytes(4, "big") + encoded)
    except (EOFError, OSError):
        pass
    finally:
        # Whatever ends this loop, the decoder gets EOF and the benchmark fails instead of waiting forever.
        sock.close()


@pytest.fixture
def decoder() -> Iterator[CommsDecoder]:
    task_side, supervisor_side = socket.socketpair()
    supervisor = threading.Thread(target=_answer_every_request, args=(supervisor_side,), daemon=True)
    supervisor.start()
    yield CommsDecoder(socket=task_side, log=structlog.get_logger())
    task_side.close()
    supervisor_side.close()
    supervisor.join(timeout=5)


@pytest.fixture
def loop() -> Iterator[asyncio.AbstractEventLoop]:
    event_loop = asyncio.new_event_loop()
    yield event_loop
    event_loop.close()


def test_send_round_trip(benchmark, decoder):
    """One synchronous request and its response, as a sync task's SDK call makes it."""
    result = benchmark(lambda: decoder.send(GetVariable(key="key")))
    assert result.key == "key"


def test_asend_round_trip(benchmark, decoder, loop):
    """One asynchronous request and its response, as an async task's SDK call makes it."""
    result = benchmark(lambda: loop.run_until_complete(decoder.asend(GetVariable(key="key"))))
    assert result.key == "key"


def test_asend_round_trips_from_concurrent_coroutines(benchmark, decoder, loop):
    """Fifty coroutines each making one request on the same loop, as iterated async items do."""

    async def fifty_concurrent_requests():
        return await asyncio.gather(*(decoder.asend(GetVariable(key=f"key{i}")) for i in range(50)))

    results = benchmark(lambda: loop.run_until_complete(fifty_concurrent_requests()))
    assert [result.key for result in results] == [f"key{i}" for i in range(50)]
