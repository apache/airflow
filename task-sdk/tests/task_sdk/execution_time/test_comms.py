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
import gc
import signal
import threading
import uuid
import weakref
from unittest import mock

import msgspec
import pytest
import structlog

from airflow.sdk import timezone
from airflow.sdk.execution_time.comms import (
    BundleInfo,
    CommsDecoder,
    DeadlockImminentError,
    GetVariable,
    MaskSecret,
    StartupDetails,
    VariableResult,
    _RequestFrame,
    _ResponseFrame,
)


def _is_taken(lock) -> bool:
    """Whether a thread holds ``lock``, asked from a thread of its own: an RLock lets its owner in again."""
    free: list[bool] = []

    def probe():
        free.append(lock.acquire(blocking=False))
        if free[0]:
            lock.release()

    prober = threading.Thread(target=probe)
    prober.start()
    prober.join()
    return not free[0]


class TestCommsModels:
    """Test Pydantic models used in task communication for proper validation."""

    @pytest.mark.parametrize(
        "object_to_mask",
        [
            {
                "key_path": "/files/airflow-breeze-config/keys2/keys.json",
                "scope": "https://www.googleapis.com/auth/cloud-platform",
                "project": "project_id",
                "num_retries": 6,
            },
            ["iter1", "iter2", {"key": "value"}],
            "string",
            {
                "key1": "value1",
            },
        ],
    )
    def test_mask_secret_with_objects(self, object_to_mask):
        mask_secret_object = MaskSecret(value=object_to_mask, name="test_secret")
        assert mask_secret_object.value == object_to_mask


class TestCommsDecoder:
    """Test the communication between the subprocess and the "supervisor"."""

    @pytest.mark.usefixtures("disable_capturing")
    @pytest.mark.parametrize(
        "coordinates",
        [
            {},
            {"region_id": uuid.UUID(int=0), "region_index": -1},
            {"region_id": uuid.uuid4(), "region_index": 7},
        ],
    )
    def test_recv_StartupDetails(self, socket_pair, coordinates):
        r, w = socket_pair

        msg = {
            "type": "StartupDetails",
            "ti": {
                "id": uuid.UUID("4d828a62-a417-4936-a7a6-2b3fabacecab"),
                "task_id": "a",
                "try_number": 1,
                "run_id": "b",
                "dag_id": "c",
                "dag_version_id": uuid.UUID("4d828a62-a417-4936-a7a6-2b3fabacecab"),
                "pool_slots": 1,
                "map_index": 3,
                **coordinates,
                "queue": "default",
                "priority_weight": 1,
            },
            "ti_context": {
                "dag_run": {
                    "dag_id": "c",
                    "run_id": "b",
                    "logical_date": "2024-12-01T01:00:00Z",
                    "data_interval_start": "2024-12-01T00:00:00Z",
                    "data_interval_end": "2024-12-01T01:00:00Z",
                    "start_date": "2024-12-01T01:00:00Z",
                    "run_after": "2024-12-01T01:00:00Z",
                    "end_date": None,
                    "run_type": "manual",
                    "state": "success",
                    "conf": None,
                    "consumed_asset_events": [],
                    "partition_key": None,
                },
                "max_tries": 0,
                "should_retry": False,
                "variables": None,
                "connections": None,
            },
            "file": "/dev/null",
            "start_date": "2024-12-01T01:00:00Z",
            "dag_rel_path": "/dev/null",
            "bundle_info": {"name": "any-name", "version": "any-version"},
            "sentry_integration": "",
        }
        bytes = msgspec.msgpack.encode(_ResponseFrame(0, msg, None))
        w.sendall(len(bytes).to_bytes(4, byteorder="big") + bytes)

        decoder = CommsDecoder(socket=r, log=None)

        msg = decoder._get_response()
        assert isinstance(msg, StartupDetails)
        assert msg.ti.id == uuid.UUID("4d828a62-a417-4936-a7a6-2b3fabacecab")
        assert msg.ti.task_id == "a"
        assert msg.ti.region_id == coordinates.get("region_id")
        assert msg.ti.region_index == coordinates.get("region_index")
        assert msg.ti.map_index == 3
        assert ("region_index" in msg.ti.model_fields_set) == ("region_index" in coordinates)
        assert msg.ti.dag_id == "c"
        assert msg.dag_rel_path == "/dev/null"
        assert msg.bundle_info == BundleInfo(name="any-name", version="any-version")
        assert msg.start_date == timezone.datetime(2024, 12, 1, 1)

    def test_huge_payload(self, socket_pair):
        r, w = socket_pair

        msg = {
            "type": "XComResult",
            "key": "a",
            "value": ("a" * 10 * 1024 * 1024) + "b",  # A 10mb xcom value
        }

        w.settimeout(1.0)
        bytes = msgspec.msgpack.encode(_ResponseFrame(0, msg, None))

        # Since `sendall` blocks, we need to do the send in another thread, so we can perform the read here
        t = threading.Thread(target=w.sendall, args=(len(bytes).to_bytes(4, byteorder="big") + bytes,))
        t.start()

        decoder = CommsDecoder(socket=r, log=None)

        try:
            msg = decoder._get_response()
        finally:
            t.join(2)

        assert msg is not None

        # It actually failed to read at all for large values, but lets just make sure we get it all
        assert len(msg.value) == 10 * 1024 * 1024 + 1
        assert msg.value[-1] == "b"

    def test_send_thread_safety(self, socket_pair):
        r, w = socket_pair
        decoder = CommsDecoder(socket=r, log=structlog.get_logger())
        num_threads = 5
        results = [None] * num_threads
        errors = [None] * num_threads

        def send_and_store(idx):
            try:
                msg = VariableResult(key=f"key{idx}", value=f"value{idx}", type="VariableResult")
                results[idx] = decoder.send(msg)
            except Exception as e:
                errors[idx] = e

        threads = [threading.Thread(target=send_and_store, args=(i,)) for i in range(num_threads)]
        for t in threads:
            t.start()

        def _recv_exactly(sock, n):
            buffer = bytearray()
            while len(buffer) < n:
                chunk = sock.recv(n - len(buffer))
                if not chunk:
                    raise EOFError("socket closed before a full frame was received")
                buffer.extend(chunk)
            return bytes(buffer)

        # The order in which the concurrent ``send`` calls reach the socket is not
        # deterministic, so the parent must not assume requests arrive in thread-index
        # order. Read each request as it arrives and echo a response built from that
        # request's own body, so every thread reliably gets the response to its own
        # message regardless of thread scheduling.
        for _ in range(num_threads):
            length = int.from_bytes(_recv_exactly(w, 4), byteorder="big")
            request = msgspec.msgpack.decode(_recv_exactly(w, length), type=_RequestFrame)
            resp = {"type": "VariableResult", "key": request.body["key"], "value": request.body["value"]}
            data = msgspec.msgpack.encode(_ResponseFrame(request.id, resp, None))
            w.sendall(len(data).to_bytes(4, byteorder="big") + data)

        for t in threads:
            t.join(timeout=5)
        for idx, t in enumerate(threads):
            assert not t.is_alive(), f"Thread {idx} did not finish (possible deadlock or hang in send method)"

        for idx in range(num_threads):
            assert errors[idx] is None, f"Thread {idx} error: {errors[idx]}"
            assert results[idx].key == f"key{idx}", f"Out-of-order or missing response for thread {idx}"
            assert results[idx].value == f"value{idx}", f"Incorrect value for thread {idx}"

    @staticmethod
    def _answer_in_order(sock, first_read: threading.Event, answer_first: threading.Event) -> None:
        """
        Act as the supervisor for two requests: signal once the first is read, answer it when
        told to, then read and answer the second. Gives up after a few seconds by closing the
        socket, so a send() that would wait forever fails instead of hanging the test.
        """

        def read_request() -> _RequestFrame:
            length = int.from_bytes(sock.recv(4), "big")
            body = b""
            while len(body) < length:
                body += sock.recv(length - len(body))
            return msgspec.msgpack.decode(body, type=_RequestFrame)

        def respond(request: _RequestFrame) -> None:
            assert request.body is not None
            resp = {"type": "VariableResult", "key": request.body["key"], "value": "v"}
            encoded = msgspec.msgpack.encode(_ResponseFrame(request.id, resp, None))
            sock.sendall(len(encoded).to_bytes(4, "big") + encoded)

        sock.settimeout(5)
        try:
            first = read_request()
            first_read.set()
            if not answer_first.wait(timeout=5):
                raise TimeoutError
            respond(first)
            respond(read_request())
        except (OSError, TimeoutError):
            sock.close()

    @pytest.mark.asyncio
    async def test_send_from_the_running_loop_waits_for_an_asend_in_flight(self, socket_pair):
        """
        A sync call on the loop thread while an asend() waits for its response no longer
        deadlocks: asend() holds the channel on a worker thread that needs no loop to finish, so
        send() waits for that round trip and then gets its own response.
        """
        r, w = socket_pair
        decoder = CommsDecoder(socket=r, log=structlog.get_logger())
        first_read, answer_first = threading.Event(), threading.Event()
        supervisor = threading.Thread(
            target=self._answer_in_order, args=(w, first_read, answer_first), daemon=True
        )
        supervisor.start()

        in_flight = asyncio.ensure_future(decoder.asend(GetVariable(key="first")))
        assert await asyncio.to_thread(first_read.wait, 5)
        # The first response is held back until send() has had time to block on the channel.
        threading.Timer(0.1, answer_first.set).start()

        result = decoder.send(GetVariable(key="second"))

        assert result.key == "second"
        assert (await asyncio.wait_for(in_flight, timeout=5)).key == "first"
        supervisor.join(timeout=5)

    @pytest.mark.asyncio
    async def test_asend_cancelled_while_waiting_for_the_thread_lock_leaves_it_free(self, socket_pair):
        """
        asend() waits for _thread_lock in a worker thread. Cancelling that wait (the executor shutting
        down after a failure) cancels the future, not the thread: once the holder lets go, the thread
        acquires, and the lock must not stay taken with nobody to release it.
        """
        r, _ = socket_pair
        decoder = CommsDecoder(socket=r, log=structlog.get_logger())

        lock_held = threading.Event()
        lock_release = threading.Event()

        def _hold_lock():
            decoder._thread_lock.acquire()
            lock_held.set()
            lock_release.wait()
            decoder._thread_lock.release()

        holder = threading.Thread(target=_hold_lock, daemon=True)
        holder.start()
        assert lock_held.wait(timeout=2), "Background thread never acquired _thread_lock"

        parked = asyncio.ensure_future(decoder.asend(GetVariable(key="parked")))
        await asyncio.sleep(0.05)
        parked.cancel()
        with pytest.raises(asyncio.CancelledError):
            await parked

        lock_release.set()
        holder.join(timeout=2)
        for _ in range(200):
            if decoder._thread_lock.acquire(blocking=False):
                decoder._thread_lock.release()
                break
            await asyncio.sleep(0.01)
        else:
            pytest.fail("_thread_lock stayed taken after the cancelled asend()")

    @pytest.mark.asyncio
    async def test_asend_cancelled_while_reading_keeps_the_channel_until_its_reply_is_drained(
        self, socket_pair
    ):
        """
        Cancelling an asend() parked on the response cancels the future, not the read. The channel
        must stay taken until that response is drained; otherwise the next send() writes its own
        request and may read the cancelled request's response as its own.
        """
        r, w = socket_pair
        decoder = CommsDecoder(socket=r, log=structlog.get_logger())

        def _read_request(sock) -> _RequestFrame:
            length = int.from_bytes(sock.recv(4), "big")
            body = b""
            while len(body) < length:
                body += sock.recv(length - len(body))
            return msgspec.msgpack.decode(body, type=_RequestFrame)

        def _respond(sock, req: _RequestFrame) -> None:
            assert req.body is not None
            resp = {"type": "VariableResult", "key": req.body["key"], "value": "v"}
            encoded = msgspec.msgpack.encode(_ResponseFrame(req.id, resp, None))
            sock.sendall(len(encoded).to_bytes(4, "big") + encoded)

        cancelled = asyncio.ensure_future(decoder.asend(GetVariable(key="cancelled")))
        cancelled_request = await asyncio.to_thread(_read_request, w)
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled

        try:
            assert _is_taken(decoder._thread_lock), "the channel was released before the response was read"

            results: list = []
            sender = threading.Thread(
                target=lambda: results.append(decoder.send(GetVariable(key="next"))), daemon=True
            )
            sender.start()
            await asyncio.to_thread(_respond, w, cancelled_request)
            await asyncio.to_thread(lambda: _respond(w, _read_request(w)))
            await asyncio.to_thread(sender.join, 5)

            assert [result.key for result in results] == ["next"]
            assert not _is_taken(decoder._thread_lock)
        except BaseException:
            # Unblocks a read left in recv, so a failure here ends the test instead of hanging the
            # loop's executor shutdown.
            w.close()
            raise

    @pytest.mark.asyncio
    async def test_send_from_event_loop_succeeds_when_lock_free(self, socket_pair):
        """
        send() called from the event loop thread after an asend() has finished (the lock is
        free) proceeds and returns its result: the sync socket I/O needs no event loop.
        """
        import asyncio

        r, w = socket_pair
        decoder = CommsDecoder(socket=r, log=structlog.get_logger())

        def _serve_one():
            length = int.from_bytes(w.recv(4), "big")
            body = b""
            while len(body) < length:
                body += w.recv(length - len(body))
            req = msgspec.msgpack.decode(body, type=_RequestFrame)
            resp = {"type": "VariableResult", "key": req.body["key"], "value": "v"}
            encoded = msgspec.msgpack.encode(_ResponseFrame(req.id, resp, None))
            w.sendall(len(encoded).to_bytes(4, "big") + encoded)

        # Phase 1: complete one asend() from this loop
        server = threading.Thread(target=_serve_one, daemon=True)
        server.start()
        await asyncio.wait_for(decoder.asend(GetVariable(key="prime")), timeout=5)
        server.join(timeout=2)

        # Phase 2: the lock is free; send() from the event loop thread succeeds.
        server2 = threading.Thread(target=_serve_one, daemon=True)
        server2.start()
        result = decoder.send(GetVariable(key="should_succeed"))
        server2.join(timeout=2)

        assert result is not None
        assert result.key == "should_succeed"

    def test_send_after_event_loop_closes_does_not_raise(self, socket_pair):
        """
        send() called from the thread that ran an event loop, after that loop has finished,
        returns its result.

        Scenario: AsyncOperator.execute() runs aexecute() via loop.run_until_complete(), which
        calls await SUPERVISOR_COMMS.asend(...). After execute() returns the loop is closed, and
        the task_runner calls SUPERVISOR_COMMS.send() to push XCom results.
        """
        import asyncio

        r, w = socket_pair
        decoder = CommsDecoder(socket=r, log=structlog.get_logger())

        # Helper: serve exactly one request from the given socket pair.
        def _serve_one(sock):
            length = int.from_bytes(sock.recv(4), "big")
            body = b""
            while len(body) < length:
                body += sock.recv(length - len(body))
            req = msgspec.msgpack.decode(body, type=_RequestFrame)
            resp = {"type": "VariableResult", "key": req.body["key"], "value": "v"}
            encoded = msgspec.msgpack.encode(_ResponseFrame(req.id, resp, None))
            sock.sendall(len(encoded).to_bytes(4, "big") + encoded)

        # --- Phase 1: run the async part (simulates aexecute()) ---
        async def _async_phase():
            server = threading.Thread(target=_serve_one, args=(w,), daemon=True)
            server.start()
            await asyncio.wait_for(decoder.asend(GetVariable(key="async_key")), timeout=5)
            server.join(timeout=2)

        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(_async_phase())
        finally:
            loop.close()
            asyncio.set_event_loop(None)

        # --- Phase 2: sync send() after the loop is closed (simulates _push_xcom_if_needed) ---
        server2 = threading.Thread(target=_serve_one, args=(w,), daemon=True)
        server2.start()

        result = decoder.send(GetVariable(key="sync_key"))
        server2.join(timeout=2)

        assert result is not None

    @staticmethod
    def _serve(sock, count: int, delay: float = 0.0) -> None:
        """Answer ``count`` requests with their own key, after ``delay`` seconds each; stop on a closed socket."""
        import time

        try:
            for _ in range(count):
                length = int.from_bytes(sock.recv(4), "big")
                body = b""
                while len(body) < length:
                    body += sock.recv(length - len(body))
                request = msgspec.msgpack.decode(body, type=_RequestFrame)
                assert request.body is not None
                time.sleep(delay)
                resp = {"type": "VariableResult", "key": request.body["key"], "value": "v"}
                encoded = msgspec.msgpack.encode(_ResponseFrame(request.id, resp, None))
                sock.sendall(len(encoded).to_bytes(4, "big") + encoded)
        except OSError:
            pass

    def test_send_on_a_thread_already_in_a_request_raises_instead_of_waiting(self, socket_pair):
        """
        A call on a thread that holds the channel for its own request would wait for itself: the
        one real deadlock left, so it raises at once, and the interrupted request still completes.
        """
        r, w = socket_pair
        decoder = CommsDecoder(socket=r, log=structlog.get_logger())
        threading.Thread(target=self._serve, args=(w, 1), daemon=True).start()
        raised: list[BaseException] = []
        original_get_response = CommsDecoder._get_response

        def interrupted(self):
            # What a signal handler would do while this thread waits for the response.
            try:
                self.send(GetVariable(key="from the handler"))
            except DeadlockImminentError as exc:
                raised.append(exc)
            return original_get_response(self)

        with mock.patch.object(CommsDecoder, "_get_response", interrupted):
            result = decoder.send(GetVariable(key="outer"))

        assert result.key == "outer"
        assert len(raised) == 1
        assert "already in the middle of a request" in str(raised[0])
        assert not _is_taken(decoder._thread_lock)

    @pytest.mark.skipif(not hasattr(signal, "setitimer"), reason="needs SIGALRM timers")
    def test_a_signal_handler_calling_send_during_a_send_raises_instead_of_freezing(self, socket_pair):
        """
        The case the check exists for: the runner's SIGTERM handler runs on_kill() on the main
        thread, and an on_kill() making a sync SDK call while the main thread waits for a response
        would wait for its own thread. A real signal lands mid-request here.
        """
        r, w = socket_pair
        decoder = CommsDecoder(socket=r, log=structlog.get_logger())
        # The response comes after the signal has fired.
        threading.Thread(target=self._serve, args=(w, 1, 0.3), daemon=True).start()
        raised: list[BaseException] = []

        def on_alarm(signum, frame):
            try:
                decoder.send(GetVariable(key="from on_kill"))
            except DeadlockImminentError as exc:
                raised.append(exc)

        previous = signal.signal(signal.SIGALRM, on_alarm)
        try:
            signal.setitimer(signal.ITIMER_REAL, 0.05)
            result = decoder.send(GetVariable(key="interrupted"))
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous)

        assert result.key == "interrupted"
        assert len(raised) == 1

    def test_a_failed_request_leaves_no_request_in_flight_behind(self, socket_pair):
        """
        A request that fails mid-exchange releases the channel and clears its in-flight mark, so the
        thread's next call is served instead of being taken for a call from inside a request.
        """
        r, w = socket_pair
        decoder = CommsDecoder(socket=r, log=structlog.get_logger())
        threading.Thread(target=self._serve, args=(w, 2), daemon=True).start()

        with mock.patch.object(CommsDecoder, "_get_response", side_effect=EOFError("supervisor gone")):
            with pytest.raises(EOFError):
                decoder.send(GetVariable(key="lost"))

        assert decoder._exchanging is False
        assert not _is_taken(decoder._thread_lock)
        assert decoder.send(GetVariable(key="next")) is not None

    # Waiting for its own thread would never end: the limit turns a regression into a failure.
    @pytest.mark.execution_timeout(10)
    @pytest.mark.parametrize("where", ["before the request is written", "after the response is read"])
    def test_a_send_interrupting_its_own_thread_outside_a_request_is_served(self, socket_pair, where):
        """
        A signal handler can land after the interrupted send() took the channel but before it wrote
        its request, or after it read its response but before it let the channel go. Nothing of the
        interrupted request is on the wire then, so the handler's call is exchanged whole in between
        instead of waiting for its own thread, and both callers get their own response.
        """
        r, w = socket_pair
        decoder = CommsDecoder(socket=r, log=structlog.get_logger())
        threading.Thread(target=self._serve, args=(w, 2), daemon=True).start()
        from_the_handler: list = []
        original_exchange = CommsDecoder._exchange

        def interrupted(self, msg, frame_bytes):
            # Only the outer request is interrupted: the handler's own send() exchanges unpatched.
            if msg.key != "interrupted":
                return original_exchange(self, msg, frame_bytes)
            if where == "before the request is written":
                from_the_handler.append(self.send(GetVariable(key="from the handler")))
                return original_exchange(self, msg, frame_bytes)
            result = original_exchange(self, msg, frame_bytes)
            from_the_handler.append(self.send(GetVariable(key="from the handler")))
            return result

        with mock.patch.object(CommsDecoder, "_exchange", interrupted):
            result = decoder.send(GetVariable(key="interrupted"))

        assert result.key == "interrupted"
        assert [response.key for response in from_the_handler] == ["from the handler"]
        assert not _is_taken(decoder._thread_lock)

    def test_concurrent_sync_and_async_callers_each_get_their_own_response(self, socket_pair):
        """Twenty threads calling send() and twenty coroutines calling asend() at once never cross responses."""
        r, w = socket_pair
        decoder = CommsDecoder(socket=r, log=structlog.get_logger())
        supervisor = threading.Thread(target=self._serve, args=(w, 40), daemon=True)
        supervisor.start()
        sync_results: dict[str, str] = {}

        def sync_caller(key: str) -> None:
            sync_results[key] = decoder.send(GetVariable(key=key)).key

        async def main() -> list[str]:
            threads = [threading.Thread(target=sync_caller, args=(f"sync-{i}",)) for i in range(20)]
            for thread in threads:
                thread.start()
            results = await asyncio.gather(*(decoder.asend(GetVariable(key=f"async-{i}")) for i in range(20)))
            for thread in threads:
                await asyncio.to_thread(thread.join, 10)
            return [result.key for result in results]

        async_keys = asyncio.run(asyncio.wait_for(main(), timeout=20))

        assert async_keys == [f"async-{i}" for i in range(20)]
        assert sync_results == {f"sync-{i}": f"sync-{i}" for i in range(20)}
        supervisor.join(timeout=5)

    @pytest.mark.execution_timeout(10)
    def test_asend_from_two_event_loops_at_once_each_gets_its_own_response(self, socket_pair):
        """
        A sync indexed task that runs an event loop of its own (``asyncio.run(aget_connection())`` in
        ``execute``) sends while the iteration's loop has an ``asend`` in flight. Each call gets its
        own response, instead of one failing on a lock bound to the other loop.
        """
        r, w = socket_pair
        decoder = CommsDecoder(socket=r, log=structlog.get_logger())
        # Slow answers, so the second loop's call arrives while the first one's is in flight.
        threading.Thread(target=self._serve, args=(w, 4, 0.1), daemon=True).start()
        results: dict[str, list] = {"iteration": [], "item": []}
        errors: list[BaseException] = []

        def run_loop(name):
            async def two_calls():
                return await asyncio.gather(
                    decoder.asend(GetVariable(key=f"{name} 1")), decoder.asend(GetVariable(key=f"{name} 2"))
                )

            try:
                results[name] = [response.key for response in asyncio.run(two_calls())]
            except BaseException as e:
                errors.append(e)

        loops = [threading.Thread(target=run_loop, args=(name,)) for name in results]
        for thread in loops:
            thread.start()
        for thread in loops:
            thread.join(8)

        assert errors == []
        assert results == {"iteration": ["iteration 1", "iteration 2"], "item": ["item 1", "item 2"]}

    @pytest.mark.execution_timeout(10)
    def test_asend_from_a_later_event_loop_is_served(self, socket_pair):
        """
        Calls that had to wait for each other on one event loop do not tie the channel to it: a
        later loop in the same process (another ``asyncio.run()``) has its own calls served too.
        """
        r, w = socket_pair
        decoder = CommsDecoder(socket=r, log=structlog.get_logger())
        threading.Thread(target=self._serve, args=(w, 4, 0.05), daemon=True).start()

        async def two_calls(name):
            responses = await asyncio.gather(
                decoder.asend(GetVariable(key=f"{name} 1")), decoder.asend(GetVariable(key=f"{name} 2"))
            )
            return [response.key for response in responses]

        assert asyncio.run(two_calls("first loop")) == ["first loop 1", "first loop 2"]
        assert asyncio.run(two_calls("second loop")) == ["second loop 1", "second loop 2"]

    def test_a_closed_loop_whose_calls_waited_for_each_other_is_not_kept(self, socket_pair):
        """
        Calls that waited for each other on one event loop leave that loop referenced by its lock. Once
        its last call is answered the decoder lets the loop go, so a sync indexed task running
        ``asyncio.run()`` per index does not keep every closed loop until the task ends.
        """
        r, w = socket_pair
        decoder = CommsDecoder(socket=r, log=structlog.get_logger())
        threading.Thread(target=self._serve, args=(w, 6, 0.01), daemon=True).start()
        loops: list[weakref.ref] = []

        async def two_calls():
            loops.append(weakref.ref(asyncio.get_running_loop()))
            await asyncio.gather(decoder.asend(GetVariable(key="a")), decoder.asend(GetVariable(key="b")))

        for _ in range(3):
            asyncio.run(two_calls())
        gc.collect()

        assert [loop() for loop in loops] == [None, None, None]
        assert len(decoder._loop_queues) == 0

    def test_send_from_the_paused_loop_waits_for_an_asend_in_flight(self, socket_pair):
        """
        The same between two run_until_complete() calls, on the loop's own thread: the asend()
        parked on its response finishes on its worker thread without the loop, so send() waits
        for it instead of freezing the process.
        """
        r, w = socket_pair
        decoder = CommsDecoder(socket=r, log=structlog.get_logger())
        first_read, answer_first = threading.Event(), threading.Event()
        supervisor = threading.Thread(
            target=self._answer_in_order, args=(w, first_read, answer_first), daemon=True
        )
        supervisor.start()

        loop = asyncio.new_event_loop()
        try:
            in_flight = loop.create_task(decoder.asend(GetVariable(key="first")))
            assert loop.run_until_complete(asyncio.to_thread(first_read.wait, 5))
            threading.Timer(0.1, answer_first.set).start()

            # The loop is paused and this thread is its thread.
            result = decoder.send(GetVariable(key="second"))

            assert result.key == "second"
            assert loop.run_until_complete(asyncio.wait_for(in_flight, timeout=5)).key == "first"
        finally:
            loop.close()
        supervisor.join(timeout=5)

    def test_read_frame_recovers_from_short_read_on_header(self):
        msg = VariableResult(key="k", value="v", type="VariableResult")
        payload = msgspec.msgpack.encode(_ResponseFrame(0, msg.model_dump(), None))
        wire = len(payload).to_bytes(4, byteorder="big") + payload

        class ChunkedSocket:
            def __init__(self, data: bytes, chunk_size: int):
                self._data = data
                self._chunk_size = chunk_size
                self._pos = 0

            def setblocking(self, flag):
                pass

            def recv(self, n):
                remaining = self._data[self._pos :]
                if not remaining:
                    return b""
                chunk = remaining[: min(n, self._chunk_size)]
                self._pos += len(chunk)
                return chunk

            def recv_into(self, buf):
                remaining = self._data[self._pos :]
                if not remaining:
                    return 0
                take = min(len(buf), self._chunk_size, len(remaining))
                buf[:take] = remaining[:take]
                self._pos += take
                return take

        sock = ChunkedSocket(wire, chunk_size=2)
        decoder = CommsDecoder(socket=sock, log=None)

        result = decoder._get_response()
        assert isinstance(result, VariableResult)
        assert result.key == "k"
        assert result.value == "v"
