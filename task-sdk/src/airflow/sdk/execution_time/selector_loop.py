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
Selector-based I/O loop utilities shared across subprocess monitors.

Both :class:`~airflow.sdk.execution_time.supervisor.WatchedSubprocess`
(supervisor-side) and provider-registered bridges such as the Locale DagFileProcessor (child-side) use these building blocks to multiplex
socket I/O without threads.

The common contract for every callback registered with the selector:

* The selector stores a ``(handler, on_close)`` tuple as ``key.data``.
* ``handler(fileobj) -> bool`` — read available data and return
  ``True`` to keep listening, ``False`` on EOF / error.
* ``on_close(fileobj)`` — called when the handler returns ``False``;
  must unregister the fileobj from the selector.
* :func:`service_selector` drives one iteration of this protocol.
"""

from __future__ import annotations

import selectors
from contextlib import suppress
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Generator
    from socket import socket

    # (handler, on_close) — stored as ``selector.register(..., data=cb)``
    SelectorCallback = tuple[Callable[[socket], bool], Callable[[socket], None]]


# Sockets, even the `.makefile()` function don't correctly do line buffering on reading. If a chunk is read
# and it doesn't contain a new line character, `.readline()` will just return the chunk as is.
#
# This returns a callback suitable for attaching to a `selector` that reads in to a buffer, and yields lines
# to a (sync) generator
def make_buffered_socket_reader(
    gen: Generator[None, bytes | bytearray, None],
    on_close: Callable[[socket], None],
    buffer_size: int = 4096,
) -> SelectorCallback:
    """
    Create a selector callback that line-buffers socket data into a generator.

    Bytes are accumulated until a newline is found; each
    complete line is sent to *gen* via ``gen.send(line)``.  On EOF the
    remainder of the buffer (if any) is flushed.

    Returns a ``(handler, on_close)`` tuple suitable for
    ``selector.register(..., data=...)``.
    """
    buffer = bytearray()  # This will hold our accumulated binary data
    read_buffer = bytearray(buffer_size)  # Temporary buffer for each read

    # We need to start up the generator to get it to the point it's at waiting on the yield
    next(gen)

    def cb(sock: socket):
        nonlocal buffer, read_buffer
        # Read up to `buffer_size` bytes of data from the socket
        n_received = sock.recv_into(read_buffer)

        if not n_received:
            # If no data is returned, the connection is closed. Return whatever is left in the buffer
            if len(buffer):
                with suppress(StopIteration):
                    gen.send(buffer)
            return False

        buffer.extend(read_buffer[:n_received])

        # We could have read multiple lines in one go, yield them all
        while (newline_pos := buffer.find(b"\n")) != -1:
            line = buffer[: newline_pos + 1]
            try:
                gen.send(line)
            except StopIteration:
                return False
            buffer = buffer[newline_pos + 1 :]  # Update the buffer with remaining data

        return True

    return cb, on_close


def make_raw_forwarder(
    dest: socket,
    on_close: Callable[[socket], None],
) -> SelectorCallback:
    """
    Create a selector callback that forwards raw bytes to *dest*.

    Used for transparent protocol bridges where bytes must be shuttled
    between two sockets without interpretation (e.g. length-prefixed
    msgpack frames between a supervisor and a Java subprocess).
    """

    def cb(sock: socket) -> bool:
        data = sock.recv(65536)
        if not data:
            return False
        try:
            dest.sendall(data)
        except (BrokenPipeError, ConnectionResetError, OSError):
            return False
        return True

    return cb, on_close


def service_selector(selector: selectors.BaseSelector, timeout: float = 1.0) -> None:
    """
    Process one round of selector events.

    For each ready socket whose handler returns ``False`` (EOF / error),
    the socket's *on_close* callback is invoked and the socket is closed.
    """
    # Ensure minimum timeout to prevent CPU spike with tight loop when timeout is 0 or negative
    timeout = max(0.01, timeout)
    events = selector.select(timeout=timeout)
    for key, _ in events:
        # Retrieve the handler responsible for processing this file object (e.g., stdout, stderr)
        socket_handler, on_close = key.data

        # Example of handler behavior:
        # If the subprocess writes "Hello, World!" to stdout:
        # - `socket_handler` reads and processes the message.
        # - If EOF is reached, the handler returns False to signal no more reads are expected.
        # - BrokenPipeError should be caught and treated as if the handler returned false, similar
        # to EOF case
        try:
            need_more = socket_handler(key.fileobj)
        except (BrokenPipeError, ConnectionResetError):
            need_more = False

        # If the handler signals that the file object is no longer needed (EOF, closed, etc.)
        # unregister it from the selector to stop monitoring; `wait()` blocks until all selectors
        # are removed.
        if not need_more:
            sock: socket = key.fileobj  # type: ignore[assignment]
            on_close(sock)
            sock.close()
