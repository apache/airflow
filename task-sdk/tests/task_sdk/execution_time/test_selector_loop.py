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

import selectors
import socket
from unittest.mock import MagicMock

import pytest

from airflow.sdk.execution_time.selector_loop import (
    make_buffered_socket_reader,
    make_raw_forwarder,
    service_selector,
)


def _make_generator():
    """Return a generator that collects sent lines into a list."""
    received: list[bytes | bytearray] = []

    def gen():
        while True:
            line = yield
            received.append(bytes(line))

    g = gen()
    return g, received


def _make_socket_pair():
    """Create a connected TCP socket pair on localhost."""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    addr = server.getsockname()

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(addr)
    conn, _ = server.accept()
    server.close()
    return client, conn


class TestMakeBufferedSocketReader:
    def test_single_complete_line(self):
        gen, received = _make_generator()
        on_close = MagicMock()
        handler, returned_on_close = make_buffered_socket_reader(gen, on_close)

        sock = MagicMock(spec=socket.socket)
        # recv_into writes data and returns count
        data = b"hello world\n"
        sock.recv_into.side_effect = lambda buf: _fill_buffer(buf, data)

        result = handler(sock)

        assert result is True
        assert received == [b"hello world\n"]
        assert returned_on_close is on_close

    def test_multiple_lines_in_single_recv(self):
        gen, received = _make_generator()
        on_close = MagicMock()
        handler, _ = make_buffered_socket_reader(gen, on_close)

        sock = MagicMock(spec=socket.socket)
        data = b"line1\nline2\nline3\n"
        sock.recv_into.side_effect = lambda buf: _fill_buffer(buf, data)

        result = handler(sock)

        assert result is True
        assert received == [b"line1\n", b"line2\n", b"line3\n"]

    def test_partial_line_accumulated_across_calls(self):
        gen, received = _make_generator()
        on_close = MagicMock()
        handler, _ = make_buffered_socket_reader(gen, on_close)

        sock = MagicMock(spec=socket.socket)

        # First call: partial line (no newline)
        sock.recv_into.side_effect = lambda buf: _fill_buffer(buf, b"hell")
        result = handler(sock)
        assert result is True
        assert received == []

        # Second call: rest of the line
        sock.recv_into.side_effect = lambda buf: _fill_buffer(buf, b"o\n")
        result = handler(sock)
        assert result is True
        assert received == [b"hello\n"]

    def test_eof_flushes_remaining_buffer(self):
        gen, received = _make_generator()
        on_close = MagicMock()
        handler, _ = make_buffered_socket_reader(gen, on_close)

        sock = MagicMock(spec=socket.socket)

        # Send partial data (no newline)
        sock.recv_into.side_effect = lambda buf: _fill_buffer(buf, b"leftover")
        handler(sock)
        assert received == []

        # EOF (recv_into returns 0) — clear side_effect so return_value takes effect
        sock.recv_into.side_effect = None
        sock.recv_into.return_value = 0
        result = handler(sock)

        assert result is False
        assert received == [b"leftover"]

    def test_eof_with_empty_buffer(self):
        gen, received = _make_generator()
        on_close = MagicMock()
        handler, _ = make_buffered_socket_reader(gen, on_close)

        sock = MagicMock(spec=socket.socket)
        sock.recv_into.return_value = 0

        result = handler(sock)

        assert result is False
        assert received == []

    def test_generator_stop_iteration_returns_false(self):
        """If the generator is exhausted, handler returns False."""

        def limited_gen():
            yield  # startup
            yield  # receive one line, then stop

        gen = limited_gen()
        on_close = MagicMock()
        handler, _ = make_buffered_socket_reader(gen, on_close)

        sock = MagicMock(spec=socket.socket)
        # First line succeeds
        sock.recv_into.side_effect = lambda buf: _fill_buffer(buf, b"line1\n")
        result = handler(sock)
        assert result is True

        # Second line triggers StopIteration in the generator
        sock.recv_into.side_effect = lambda buf: _fill_buffer(buf, b"line2\n")
        result = handler(sock)
        assert result is False

    def test_mixed_complete_and_partial_lines(self):
        gen, received = _make_generator()
        on_close = MagicMock()
        handler, _ = make_buffered_socket_reader(gen, on_close)

        sock = MagicMock(spec=socket.socket)
        # Data contains one complete line and a partial line
        sock.recv_into.side_effect = lambda buf: _fill_buffer(buf, b"complete\npart")
        handler(sock)
        assert received == [b"complete\n"]

        # Finish the partial line
        sock.recv_into.side_effect = lambda buf: _fill_buffer(buf, b"ial\n")
        handler(sock)
        assert received == [b"complete\n", b"partial\n"]

    def test_custom_buffer_size(self):
        gen, received = _make_generator()
        on_close = MagicMock()
        handler, _ = make_buffered_socket_reader(gen, on_close, buffer_size=8)

        sock = MagicMock(spec=socket.socket)
        # Data larger than buffer_size — recv_into only reads buffer_size bytes
        full_data = b"abcdefghijklmnop\n"
        # Simulate chunked reads
        sock.recv_into.side_effect = lambda buf: _fill_buffer(buf, full_data[: len(buf)])
        handler(sock)
        # Only first 8 bytes read, no newline yet
        assert received == []

        sock.recv_into.side_effect = lambda buf: _fill_buffer(buf, full_data[8:16])
        handler(sock)
        assert received == []

        sock.recv_into.side_effect = lambda buf: _fill_buffer(buf, full_data[16:])
        handler(sock)
        assert received == [b"abcdefghijklmnop\n"]


def _fill_buffer(buf: bytearray, data: bytes) -> int:
    """Helper to simulate socket.recv_into by filling the buffer."""
    n = min(len(data), len(buf))
    buf[:n] = data[:n]
    return n


class TestMakeRawForwarder:
    def test_forwards_data_to_dest(self):
        on_close = MagicMock()
        dest = MagicMock(spec=socket.socket)
        handler, returned_on_close = make_raw_forwarder(dest, on_close)

        src = MagicMock(spec=socket.socket)
        src.recv.return_value = b"hello"

        result = handler(src)

        assert result is True
        dest.sendall.assert_called_once_with(b"hello")
        assert returned_on_close is on_close

    def test_eof_returns_false(self):
        on_close = MagicMock()
        dest = MagicMock(spec=socket.socket)
        handler, _ = make_raw_forwarder(dest, on_close)

        src = MagicMock(spec=socket.socket)
        src.recv.return_value = b""

        result = handler(src)

        assert result is False
        dest.sendall.assert_not_called()

    @pytest.mark.parametrize(
        "exception",
        [BrokenPipeError, ConnectionResetError, OSError],
        ids=["broken_pipe", "connection_reset", "os_error"],
    )
    def test_sendall_exception_returns_false(self, exception):
        on_close = MagicMock()
        dest = MagicMock(spec=socket.socket)
        dest.sendall.side_effect = exception
        handler, _ = make_raw_forwarder(dest, on_close)

        src = MagicMock(spec=socket.socket)
        src.recv.return_value = b"data"

        result = handler(src)

        assert result is False

    def test_multiple_forwards(self):
        on_close = MagicMock()
        dest = MagicMock(spec=socket.socket)
        handler, _ = make_raw_forwarder(dest, on_close)

        src = MagicMock(spec=socket.socket)

        for chunk in [b"chunk1", b"chunk2", b"chunk3"]:
            src.recv.return_value = chunk
            assert handler(src) is True

        assert dest.sendall.call_count == 3


class TestServiceSelector:
    def test_calls_handler_for_ready_sockets(self):
        sel = MagicMock(spec=selectors.DefaultSelector)
        handler = MagicMock(return_value=True)
        on_close = MagicMock()
        sock = MagicMock(spec=socket.socket)

        key = MagicMock()
        key.data = (handler, on_close)
        key.fileobj = sock

        sel.select.return_value = [(key, selectors.EVENT_READ)]

        service_selector(sel, timeout=1.0)

        handler.assert_called_once_with(sock)
        on_close.assert_not_called()
        sock.close.assert_not_called()

    def test_on_close_and_sock_close_when_handler_returns_false(self):
        sel = MagicMock(spec=selectors.DefaultSelector)
        handler = MagicMock(return_value=False)
        on_close = MagicMock()
        sock = MagicMock(spec=socket.socket)

        key = MagicMock()
        key.data = (handler, on_close)
        key.fileobj = sock

        sel.select.return_value = [(key, selectors.EVENT_READ)]

        service_selector(sel, timeout=1.0)

        handler.assert_called_once_with(sock)
        on_close.assert_called_once_with(sock)
        sock.close.assert_called_once()

    @pytest.mark.parametrize(
        "exception",
        [BrokenPipeError, ConnectionResetError],
        ids=["broken_pipe", "connection_reset"],
    )
    def test_pipe_errors_treated_as_eof(self, exception):
        sel = MagicMock(spec=selectors.DefaultSelector)
        handler = MagicMock(side_effect=exception)
        on_close = MagicMock()
        sock = MagicMock(spec=socket.socket)

        key = MagicMock()
        key.data = (handler, on_close)
        key.fileobj = sock

        sel.select.return_value = [(key, selectors.EVENT_READ)]

        service_selector(sel, timeout=1.0)

        on_close.assert_called_once_with(sock)
        sock.close.assert_called_once()

    def test_empty_selector_no_events(self):
        sel = MagicMock(spec=selectors.DefaultSelector)
        sel.select.return_value = []

        # Should not raise
        service_selector(sel, timeout=1.0)

    @pytest.mark.parametrize(
        ("input_timeout", "expected_min"),
        [
            (0.0, 0.01),
            (-1.0, 0.01),
            (-100.0, 0.01),
            (0.5, 0.5),
            (2.0, 2.0),
        ],
        ids=["zero", "negative", "very_negative", "positive_half", "positive_two"],
    )
    def test_timeout_clamped_to_minimum(self, input_timeout, expected_min):
        sel = MagicMock(spec=selectors.DefaultSelector)
        sel.select.return_value = []

        service_selector(sel, timeout=input_timeout)

        sel.select.assert_called_once()
        actual_timeout = sel.select.call_args[1].get("timeout") or sel.select.call_args[0][0]
        assert actual_timeout == pytest.approx(expected_min)

    def test_multiple_ready_sockets(self):
        sel = MagicMock(spec=selectors.DefaultSelector)

        handler1 = MagicMock(return_value=True)
        on_close1 = MagicMock()
        sock1 = MagicMock(spec=socket.socket)
        key1 = MagicMock()
        key1.data = (handler1, on_close1)
        key1.fileobj = sock1

        handler2 = MagicMock(return_value=False)
        on_close2 = MagicMock()
        sock2 = MagicMock(spec=socket.socket)
        key2 = MagicMock()
        key2.data = (handler2, on_close2)
        key2.fileobj = sock2

        sel.select.return_value = [(key1, selectors.EVENT_READ), (key2, selectors.EVENT_READ)]

        service_selector(sel, timeout=1.0)

        # First socket: handler returns True, stays open
        handler1.assert_called_once_with(sock1)
        on_close1.assert_not_called()
        sock1.close.assert_not_called()

        # Second socket: handler returns False, closed
        handler2.assert_called_once_with(sock2)
        on_close2.assert_called_once_with(sock2)
        sock2.close.assert_called_once()


class TestSelectorLoopIntegration:
    def test_buffered_reader_with_real_sockets(self):
        """End-to-end: send lines through real sockets and verify buffered reading."""
        gen, received = _make_generator()
        sender, reader = _make_socket_pair()
        try:
            sel = selectors.DefaultSelector()

            def on_close(sock):
                sel.unregister(sock)

            sel.register(reader, selectors.EVENT_READ, make_buffered_socket_reader(gen, on_close))

            sender.sendall(b"first line\nsecond line\n")

            service_selector(sel, timeout=1.0)

            assert b"first line\n" in received
            assert b"second line\n" in received

            # Close sender, then drain
            sender.close()
            sender = None

            service_selector(sel, timeout=0.5)

            sel.close()
        finally:
            if sender:
                sender.close()
            reader.close()

    def test_raw_forwarder_with_real_sockets(self):
        """End-to-end: forward raw bytes between real socket pairs."""
        src_send, src_recv = _make_socket_pair()
        # Use socketpair for the destination so reads/writes are symmetric
        dst_write, dst_read = socket.socketpair()
        try:
            sel = selectors.DefaultSelector()

            def on_close(sock):
                sel.unregister(sock)

            sel.register(src_recv, selectors.EVENT_READ, make_raw_forwarder(dst_write, on_close))

            src_send.sendall(b"raw data payload")

            service_selector(sel, timeout=1.0)

            dst_read.setblocking(False)
            forwarded = dst_read.recv(4096)

            assert forwarded == b"raw data payload"

            sel.close()
        finally:
            for s in (src_send, src_recv, dst_write, dst_read):
                s.close()

    def test_eof_triggers_on_close_with_real_sockets(self):
        """When the sender closes, the selector callback chain fires on_close."""
        gen, received = _make_generator()
        sender, reader = _make_socket_pair()
        closed_sockets: list[socket.socket] = []
        try:
            sel = selectors.DefaultSelector()

            def on_close(sock):
                sel.unregister(sock)
                closed_sockets.append(sock)

            sel.register(reader, selectors.EVENT_READ, make_buffered_socket_reader(gen, on_close))

            # Send data then close
            sender.sendall(b"final\n")
            service_selector(sel, timeout=1.0)
            assert received == [b"final\n"]

            sender.close()
            sender = None
            service_selector(sel, timeout=0.5)

            # on_close should have been called, and socket closed by service_selector
            assert len(closed_sockets) == 1

            sel.close()
        finally:
            if sender:
                sender.close()
            reader.close()
