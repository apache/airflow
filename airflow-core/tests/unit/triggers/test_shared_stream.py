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
import time
from collections.abc import Callable
from contextlib import asynccontextmanager, suppress
from unittest import mock
from unittest.mock import MagicMock

import pytest
import structlog

from airflow.triggers.base import BaseEventTrigger, TriggerEvent
from airflow.triggers.shared_stream import (
    AckTimeout,
    AdvanceOutcome,
    SharedStreamManager,
    SharedStreamProducer,
    _PollFailure,
    _SharedStreamGroup,
    _SubscriberOverflow,
    reject_shared_stream_event,
)


class _ProgrammableSharedStreamTrigger(BaseEventTrigger):
    """
    Test helper trigger whose shared poll yields whatever the test class attr says.

    Subclass per test so each scenario gets its own ``open_shared_stream``
    behavior without leaking state between tests.
    """

    queue_url: str = "https://q"

    def __init__(self, queue_url: str = "https://q", region: str | None = None):
        super().__init__()
        self.queue_url = queue_url
        self.region = region

    def serialize(self):
        return (
            f"{type(self).__module__}.{type(self).__qualname__}",
            {"queue_url": self.queue_url, "region": self.region},
        )

    def shared_stream_key(self):
        return ("queue", self.queue_url)

    async def filter_shared_stream(self, shared_stream):
        async for raw in shared_stream:
            if self.region is None or raw["region"] == self.region:
                yield TriggerEvent(raw)

    async def run(self):  # pragma: no cover - replaced by filter_shared_stream
        yield TriggerEvent({})


def _events_then_block(events: list[dict]):
    async def _open_shared_stream(cls, kwargs):
        for event in events:
            yield event
        # Stay alive forever so tests can observe broadcast then tear down.
        await asyncio.Event().wait()

    return classmethod(_open_shared_stream)


def _make_trigger_class(open_shared_stream):
    """Return a fresh subclass with the given open_shared_stream classmethod."""

    class _Trigger(_ProgrammableSharedStreamTrigger):
        pass

    _Trigger.open_shared_stream = open_shared_stream
    return _Trigger


async def _collect(stream, *, n: int, timeout: float = 1.0) -> list:
    """Pull ``n`` items off an async iterator with a per-item timeout."""
    out = []
    it = stream.__aiter__()
    for _ in range(n):
        out.append(await asyncio.wait_for(it.__anext__(), timeout=timeout))
    return out


@pytest.mark.asyncio
async def test_single_subscriber_receives_broadcast_events():
    cls = _make_trigger_class(
        _events_then_block(
            [
                {"region": "us"},
                {"region": "eu"},
            ]
        )
    )
    trigger = cls(region="us")
    manager = SharedStreamManager()
    try:
        stream = manager.subscribe(trigger_id=1, trigger=trigger, key=trigger.shared_stream_key())
        events = await _collect(trigger.filter_shared_stream(stream), n=1)
        assert [e.payload["region"] for e in events] == ["us"]
    finally:
        await manager.unsubscribe(1, trigger.shared_stream_key())


@pytest.mark.asyncio
async def test_two_subscribers_share_one_poll_and_filter_independently():
    cls = _make_trigger_class(
        _events_then_block(
            [
                {"region": "us"},
                {"region": "eu"},
                {"region": "us"},
            ]
        )
    )
    us, eu = cls(region="us"), cls(region="eu")
    key = us.shared_stream_key()
    assert key == eu.shared_stream_key()

    manager = SharedStreamManager()
    try:
        us_stream = manager.subscribe(trigger_id=1, trigger=us, key=key)
        eu_stream = manager.subscribe(trigger_id=2, trigger=eu, key=key)

        # The shared group is created exactly once.
        assert len(manager._groups) == 1

        us_events, eu_events = await asyncio.gather(
            _collect(us.filter_shared_stream(us_stream), n=2),
            _collect(eu.filter_shared_stream(eu_stream), n=1),
        )
        assert [e.payload["region"] for e in us_events] == ["us", "us"]
        assert [e.payload["region"] for e in eu_events] == ["eu"]
    finally:
        await manager.unsubscribe(1, key)
        await manager.unsubscribe(2, key)


@pytest.mark.asyncio
async def test_group_is_torn_down_when_last_subscriber_leaves():
    cls = _make_trigger_class(_events_then_block([{"region": "us"}]))
    trigger = cls(region="us")
    manager = SharedStreamManager()
    key = trigger.shared_stream_key()

    manager.subscribe(trigger_id=1, trigger=trigger, key=key)
    assert key in manager._groups

    await manager.unsubscribe(1, key)
    assert key not in manager._groups


@pytest.mark.asyncio
async def test_independent_keys_use_independent_groups():
    cls = _make_trigger_class(_events_then_block([{"region": "us"}]))
    a = cls(queue_url="https://a")
    b = cls(queue_url="https://b")
    manager = SharedStreamManager()

    manager.subscribe(trigger_id=1, trigger=a, key=a.shared_stream_key())
    manager.subscribe(trigger_id=2, trigger=b, key=b.shared_stream_key())
    try:
        assert set(manager._groups) == {a.shared_stream_key(), b.shared_stream_key()}
    finally:
        await manager.unsubscribe(1, a.shared_stream_key())
        await manager.unsubscribe(2, b.shared_stream_key())


@pytest.mark.asyncio
async def test_poll_failure_propagates_to_subscribers_and_evicts_group():
    async def _open_shared_stream(cls, kwargs):
        raise RuntimeError("boom")
        yield  # pragma: no cover

    cls = _make_trigger_class(classmethod(_open_shared_stream))
    trigger = cls()
    manager = SharedStreamManager()
    key = trigger.shared_stream_key()
    try:
        stream = manager.subscribe(trigger_id=1, trigger=trigger, key=key)
        with pytest.raises(RuntimeError, match="boom"):
            await asyncio.wait_for(_collect(trigger.filter_shared_stream(stream), n=1), timeout=1.0)
        # The failing poll evicts its own group from the manager in _poll's
        # finally, before any subscriber resumes — so by the time the
        # subscriber observes "boom" the manager already has no group for
        # this key. A late subscriber arriving here would create a fresh
        # group rather than attaching to a dead one.
        assert key not in manager._groups
    finally:
        await manager.unsubscribe(1, key)


@pytest.mark.asyncio
async def test_subscribe_rejects_none_key():
    cls = _make_trigger_class(_events_then_block([]))
    trigger = cls()
    manager = SharedStreamManager()
    with pytest.raises(ValueError, match="must not be None"):
        manager.subscribe(trigger_id=1, trigger=trigger, key=None)


@pytest.mark.asyncio
async def test_double_subscribe_same_id_is_rejected():
    cls = _make_trigger_class(_events_then_block([]))
    trigger = cls()
    manager = SharedStreamManager()
    key = trigger.shared_stream_key()
    try:
        manager.subscribe(trigger_id=1, trigger=trigger, key=key)
        with pytest.raises(RuntimeError, match="already subscribed"):
            manager.subscribe(trigger_id=1, trigger=trigger, key=key)
    finally:
        await manager.unsubscribe(1, key)


@pytest.mark.asyncio
async def test_stop_all_clears_every_group():
    cls = _make_trigger_class(_events_then_block([]))
    a = cls(queue_url="https://a")
    b = cls(queue_url="https://b")
    manager = SharedStreamManager()

    manager.subscribe(trigger_id=1, trigger=a, key=a.shared_stream_key())
    manager.subscribe(trigger_id=2, trigger=b, key=b.shared_stream_key())
    assert len(manager._groups) == 2

    await manager.stop_all()
    assert manager._groups == {}


@pytest.mark.asyncio
async def test_late_subscriber_after_poll_failure_gets_fresh_group():
    """The first call's open_shared_stream raises; a subsequent subscribe for the same key should
    start a brand new poll rather than attach to the dead group.
    """
    invocations: list[int] = []

    async def _open_shared_stream(cls, kwargs):
        n = len(invocations)
        invocations.append(n)
        if n == 0:
            raise RuntimeError("first invocation fails")
        yield {"region": "us"}
        await asyncio.Event().wait()

    cls = _make_trigger_class(classmethod(_open_shared_stream))
    trigger = cls()
    manager = SharedStreamManager()
    key = trigger.shared_stream_key()

    stream1 = manager.subscribe(trigger_id=1, trigger=trigger, key=key)
    with pytest.raises(RuntimeError, match="first invocation fails"):
        await asyncio.wait_for(
            _collect(trigger.filter_shared_stream(stream1), n=1),
            timeout=1.0,
        )
    await manager.unsubscribe(1, key)

    stream2 = manager.subscribe(trigger_id=2, trigger=trigger, key=key)
    try:
        events = await asyncio.wait_for(
            _collect(trigger.filter_shared_stream(stream2), n=1),
            timeout=1.0,
        )
        assert [e.payload["region"] for e in events] == ["us"]
    finally:
        await manager.unsubscribe(2, key)

    assert invocations == [0, 1], "open_shared_stream should be called twice (failed, then fresh)"


@pytest.mark.asyncio
async def test_late_subscriber_during_poll_failure_window_does_not_attach_to_dead_group():
    """Reproduce the race the lifecycle rewrite closes: a new subscriber arriving after _poll has
    raised but before the original subscriber has finished propagating the failure must see no
    existing group and create a fresh one — otherwise it would attach to a queue nothing will ever
    put events on.
    """
    invocations: list[int] = []

    async def _open_shared_stream(cls, kwargs):
        n = len(invocations)
        invocations.append(n)
        if n == 0:
            raise RuntimeError("boom")
        yield {"region": "fresh"}
        await asyncio.Event().wait()

    cls = _make_trigger_class(classmethod(_open_shared_stream))
    trigger = cls()
    manager = SharedStreamManager()
    key = trigger.shared_stream_key()

    stream1 = manager.subscribe(trigger_id=1, trigger=trigger, key=key)

    # Wait for the poll task to finish its lifecycle — including the synchronous self-eviction in
    # its finally block — but do NOT consume the _PollFailure from stream1 yet. This simulates the
    # "broadcast done, subscriber not yet unwound" window described in the bug report.
    poll_task = manager._groups[key]._poll_task
    assert poll_task is not None
    with suppress(RuntimeError):
        await poll_task

    assert key not in manager._groups, (
        "the failing poll must evict its group synchronously in _poll's finally, so this window "
        "is closed before any other coroutine can subscribe"
    )

    stream2 = manager.subscribe(trigger_id=2, trigger=trigger, key=key)
    try:
        events = await asyncio.wait_for(
            _collect(trigger.filter_shared_stream(stream2), n=1),
            timeout=1.0,
        )
        assert events[0].payload == {"region": "fresh"}
    finally:
        # Original subscriber still has _PollFailure waiting for it.
        with pytest.raises(RuntimeError, match="boom"):
            await asyncio.wait_for(
                _collect(trigger.filter_shared_stream(stream1), n=1),
                timeout=1.0,
            )
        await manager.unsubscribe(1, key)
        await manager.unsubscribe(2, key)

    assert invocations == [0, 1]


@pytest.mark.asyncio
async def test_resubscribe_during_last_unsubscribe_creates_fresh_group():
    """If the last subscriber leaves and the manager is mid-``await group.stop()``, a concurrent
    subscribe for the same key must build a new group instead of attaching to the dying one.
    """
    invocations: list[int] = []

    async def _open_shared_stream(cls, kwargs):
        n = len(invocations)
        invocations.append(n)
        yield {"n": n}
        await asyncio.Event().wait()

    cls = _make_trigger_class(classmethod(_open_shared_stream))
    trigger = cls()
    manager = SharedStreamManager()
    key = trigger.shared_stream_key()

    stream1 = manager.subscribe(trigger_id=1, trigger=trigger, key=key)
    await asyncio.wait_for(
        _collect(trigger.filter_shared_stream(stream1), n=1),
        timeout=1.0,
    )

    unsub_task = asyncio.create_task(manager.unsubscribe(1, key))
    # One tick: unsubscribe runs synchronously through the pop-from-_groups step, then yields at
    # `await group.stop()`. After this yield returns to us, _groups is already cleared.
    await asyncio.sleep(0)
    assert key not in manager._groups, (
        "manager.unsubscribe must evict the group from _groups before awaiting stop(), so a "
        "racing subscribe sees no group and creates a fresh one"
    )

    stream2 = manager.subscribe(trigger_id=2, trigger=trigger, key=key)
    try:
        events = await asyncio.wait_for(
            _collect(trigger.filter_shared_stream(stream2), n=1),
            timeout=1.0,
        )
        # Second invocation (index 1) — proves stream2 is bound to a fresh poll, not the dying one.
        assert events[0].payload == {"n": 1}
    finally:
        await unsub_task
        await manager.unsubscribe(2, key)

    assert invocations == [0, 1]


@pytest.mark.asyncio
async def test_open_shared_stream_returning_naturally_propagates_as_failure():
    """A shared poll that exhausts its iterator instead of running indefinitely would otherwise
    leave subscribers blocked on queue.get() forever; the manager surfaces it as an error.
    """

    async def _open_shared_stream(cls, kwargs):
        yield {"region": "us"}

    cls = _make_trigger_class(classmethod(_open_shared_stream))
    trigger = cls()
    manager = SharedStreamManager()
    key = trigger.shared_stream_key()

    stream = manager.subscribe(trigger_id=1, trigger=trigger, key=key)
    with pytest.raises(Exception, match="expected to run for the lifetime of the group"):
        await asyncio.wait_for(
            _collect(trigger.filter_shared_stream(stream), n=2),
            timeout=1.0,
        )

    assert key not in manager._groups, "natural exhaustion should evict the group like a failure"
    await manager.unsubscribe(1, key)


@pytest.mark.asyncio
async def test_slow_subscriber_overflow_fails_only_that_subscriber():
    """A subscriber whose ``filter_shared_stream`` lags behind the upstream cadence enough to
    overflow its bounded queue must fail loudly with ``_SubscriberOverflow`` — silent drops are
    unacceptable for Asset event-driven semantics. Sibling subscribers in the same group keep
    receiving events.
    """

    async def _open_shared_stream(cls, kwargs):
        for i in range(5):
            yield {"i": i}
            # Yield to the loop so the fast consumer gets a chance to drain;
            # the slow consumer never runs while sleep(0) ticks pass, so its
            # queue fills up.
            await asyncio.sleep(0)
        await asyncio.Event().wait()

    cls = _make_trigger_class(classmethod(_open_shared_stream))
    slow_trigger = cls()
    fast_trigger = cls()
    manager = SharedStreamManager(max_subscriber_queue=2)
    key = slow_trigger.shared_stream_key()

    slow_stream = manager.subscribe(trigger_id=1, trigger=slow_trigger, key=key)
    fast_stream = manager.subscribe(trigger_id=2, trigger=fast_trigger, key=key)

    async def drain_fast():
        out = []
        async for ev in fast_trigger.filter_shared_stream(fast_stream):
            out.append(ev)
            if len(out) >= 5:
                break
        return out

    # Start fast first so it drains its queue as the producer broadcasts.
    fast_task = asyncio.create_task(drain_fast())

    # Hand control back so the producer can broadcast all 5 events. The fast
    # consumer keeps its queue around 1; the slow consumer has no task yet,
    # so its queue fills past maxsize=2 and the overflow handler swaps the
    # backlog for a failure sentinel.
    fast_events = await asyncio.wait_for(fast_task, timeout=2.0)

    # Slow consumer starts after the overflow; first event should be the failure.
    with pytest.raises(_SubscriberOverflow, match="exceeded maxsize"):
        await asyncio.wait_for(
            _collect(slow_trigger.filter_shared_stream(slow_stream), n=1),
            timeout=2.0,
        )

    assert [e.payload["i"] for e in fast_events] == [0, 1, 2, 3, 4], (
        "fast subscriber must not be affected by the slow subscriber's overflow"
    )
    # The group is still alive — only the slow subscriber was failed; fast is still subscribed.
    assert key in manager._groups
    assert 1 in manager._groups[key]._failed_subscribers

    await manager.unsubscribe(1, key)
    await manager.unsubscribe(2, key)


@pytest.mark.asyncio
async def test_concurrent_unsubscribes_tear_down_group_cleanly():
    """N subscribers leaving at once via concurrent ``unsubscribe`` must end with the group fully
    torn down and the poll task cancelled — mirrors a triggerer cancelling many deferred tasks in
    the same tick.
    """
    cls = _make_trigger_class(_events_then_block([]))
    n_subscribers = 8
    triggers = [cls() for _ in range(n_subscribers)]
    key = triggers[0].shared_stream_key()
    manager = SharedStreamManager()

    for trigger_id, trigger in enumerate(triggers):
        manager.subscribe(trigger_id=trigger_id, trigger=trigger, key=key)
    assert len(manager._groups[key]._subscribers) == n_subscribers
    poll_task = manager._groups[key]._poll_task
    assert poll_task is not None

    await asyncio.gather(*(manager.unsubscribe(i, key) for i in range(n_subscribers)))

    assert manager._groups == {}, "every subscriber gone means the group is gone"
    assert poll_task.done(), "the poll task must exit when the last subscriber leaves"
    assert poll_task.cancelled()


@pytest.mark.asyncio
async def test_stop_all_with_blocked_consumer_does_not_inject_failure_sentinel():
    """A consumer blocked on ``queue.get()`` when ``stop_all`` runs must not be woken with a
    poison sentinel. The poll task's ``CancelledError`` path explicitly skips the terminate
    broadcast, leaving the standard idiom — the trigger's consuming task is cancelled separately
    — as the only exit. Verifies the asymmetry between cancel-driven and failure-driven teardown.
    """
    cls = _make_trigger_class(_events_then_block([]))  # never yields; consumer always blocks
    trigger = cls()
    key = trigger.shared_stream_key()
    manager = SharedStreamManager()

    stream = manager.subscribe(trigger_id=1, trigger=trigger, key=key)

    async def consume():
        async for event in trigger.filter_shared_stream(stream):
            return event
        return None

    consumer = asyncio.create_task(consume())
    # Let the consumer reach ``await queue.get()``.
    await asyncio.sleep(0)
    assert not consumer.done()

    poll_task = manager._groups[key]._poll_task
    assert poll_task is not None

    await manager.stop_all()

    assert manager._groups == {}
    assert poll_task.done()
    assert poll_task.cancelled()
    # No sentinel was injected — the consumer is still parked on queue.get().
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(asyncio.shield(consumer), timeout=0.05)

    consumer.cancel()
    with suppress(asyncio.CancelledError):
        await consumer


@pytest.mark.asyncio
async def test_sibling_non_key_kwargs_diverge_first_subscriber_wins():
    """Two siblings with the same ``shared_stream_key`` but divergent non-key kwargs share the
    group built from the **first** subscriber's kwargs. The second subscriber's non-key kwargs are
    silently ignored — this is the documented contract; the test locks the behavior so any future
    change (e.g. adding a runtime warning) is a deliberate decision rather than a regression.
    """
    captured_kwargs: list[dict] = []

    async def _open_shared_stream(cls, kwargs):
        captured_kwargs.append(kwargs)
        yield {"region": kwargs.get("region")}
        await asyncio.Event().wait()

    cls = _make_trigger_class(classmethod(_open_shared_stream))
    first = cls(region="us")
    second = cls(region="eu")  # same queue_url (key), different region (non-key)
    key = first.shared_stream_key()
    assert key == second.shared_stream_key()

    manager = SharedStreamManager()
    try:
        stream1 = manager.subscribe(trigger_id=1, trigger=first, key=key)
        manager.subscribe(trigger_id=2, trigger=second, key=key)

        # First subscriber accepts (region="us"); second's filter rejects since the raw event
        # carries the first subscriber's region. Verify by consuming from the first subscriber.
        events = await _collect(first.filter_shared_stream(stream1), n=1)
        assert [e.payload for e in events] == [{"region": "us"}]

        assert len(captured_kwargs) == 1, "open_shared_stream must be called exactly once per group"
        assert captured_kwargs[0]["region"] == "us", (
            "first subscriber's non-key kwargs become the group's kwargs"
        )
    finally:
        await manager.unsubscribe(1, key)
        await manager.unsubscribe(2, key)


@pytest.mark.asyncio
async def test_serialize_failure_in_subscribe_leaves_groups_clean():
    """If ``trigger.serialize()`` raises while a fresh group is being built, ``subscribe`` must
    propagate the exception without leaving an orphan entry in ``_groups``. A subsequent subscribe
    for the same key must build a clean group.
    """
    cls = _make_trigger_class(_events_then_block([{"region": "us"}]))

    class _BrokenSerializeTrigger(cls):
        def serialize(self):
            raise RuntimeError("serialize boom")

    broken = _BrokenSerializeTrigger()
    manager = SharedStreamManager()
    key = broken.shared_stream_key()

    with pytest.raises(RuntimeError, match="serialize boom"):
        manager.subscribe(trigger_id=1, trigger=broken, key=key)

    assert key not in manager._groups, "failed subscribe must not leave an orphan group entry"

    clean = cls()
    stream = manager.subscribe(trigger_id=2, trigger=clean, key=key)
    try:
        events = await _collect(clean.filter_shared_stream(stream), n=1)
        assert events[0].payload == {"region": "us"}
        assert key in manager._groups
    finally:
        await manager.unsubscribe(2, key)


@pytest.mark.asyncio
async def test_terminal_failure_reaches_every_subscriber_even_with_full_queues():
    """When the shared poll raises right after a broadcast that filled every subscriber's queue,
    the terminal :class:`_PollFailure` sentinel must still reach all of them. Without draining
    each queue before the terminal ``put_nowait``, the first overflowed subscriber would raise
    ``QueueFull``, abort the broadcast loop, and silently strand the remaining subscribers on
    ``queue.get()`` forever.
    """

    async def _open_shared_stream(cls, kwargs):
        yield {"region": "us"}
        raise RuntimeError("upstream died")

    cls = _make_trigger_class(classmethod(_open_shared_stream))
    first = cls()
    second = cls()
    manager = SharedStreamManager(max_subscriber_queue=1)
    key = first.shared_stream_key()

    first_stream = manager.subscribe(trigger_id=1, trigger=first, key=key)
    second_stream = manager.subscribe(trigger_id=2, trigger=second, key=key)

    # Both queues sit at maxsize=1 with the broadcast event unread when the
    # terminal _PollFailure goes out. The fix must drain each queue so the
    # sentinel lands; both consumers should observe the same RuntimeError.
    with pytest.raises(RuntimeError, match="upstream died"):
        await asyncio.wait_for(_collect(first.filter_shared_stream(first_stream), n=2), timeout=2.0)
    with pytest.raises(RuntimeError, match="upstream died"):
        await asyncio.wait_for(_collect(second.filter_shared_stream(second_stream), n=2), timeout=2.0)

    await manager.unsubscribe(1, key)
    await manager.unsubscribe(2, key)


@pytest.mark.asyncio
async def test_fail_overflowed_subscriber_drains_full_queue_before_putting_sentinel():
    """``_fail_overflowed_subscriber`` must drain the backlog *before* placing the
    failure sentinel, not after.

    White-box invariant: given a queue already at capacity, calling
    ``_fail_overflowed_subscriber`` must leave exactly one item in the queue —
    the :class:`_PollFailure` wrapping a :class:`_SubscriberOverflow` — regardless
    of how many stale events were sitting there beforehand.

    If the drain loop were moved to *after* the ``put_nowait``, the put would
    raise :exc:`asyncio.QueueFull` before any draining occurred and the
    subscriber would never receive its failure sentinel.
    """
    cap = 3
    queue: asyncio.Queue = asyncio.Queue(maxsize=cap)
    # Pre-fill the queue to capacity with stale events.
    for i in range(cap):
        queue.put_nowait({"stale": i})

    assert queue.full(), "pre-condition: queue must be full before the call"

    group = _SharedStreamGroup(
        key="test-key",
        trigger_class=_ProgrammableSharedStreamTrigger,
        kwargs={},
        on_poll_terminate=lambda g: None,
        max_subscriber_queue=cap,
        ack_timeout=300.0,
        log=structlog.get_logger("test"),
    )
    trigger_id = 42
    group._subscribers[trigger_id] = queue

    group._fail_overflowed_subscriber(trigger_id, queue)

    # Post-conditions that pin the drain-before-put ordering:
    assert queue.qsize() == 1, "exactly one item must remain: the failure sentinel"
    sentinel = queue.get_nowait()
    assert isinstance(sentinel, _PollFailure), "sentinel must be a _PollFailure"
    assert isinstance(sentinel.exc, _SubscriberOverflow), "the wrapped exception must be _SubscriberOverflow"
    assert trigger_id in group._failed_subscribers, "trigger_id must be recorded in _failed_subscribers"


class _RecordingProducer(SharedStreamProducer):
    """Producer that records advance/aclose activity onto its trigger class."""

    def __init__(self, trigger_cls, events_with_payloads: list[tuple], *, lane_for: Callable | None = None):
        self._trigger_cls = trigger_cls
        self._events_with_payloads = events_with_payloads
        self._lane_for = lane_for

    async def open_stream(self):
        for event, broker_payload in self._events_with_payloads:
            yield event, broker_payload
        await asyncio.Event().wait()

    def get_advance_lane(self, broker_payload):
        if self._lane_for is None:
            return None
        return self._lane_for(broker_payload)

    async def advance(self, batch):
        self._trigger_cls.batches.append([item.broker_payload for item in batch])
        self._trigger_cls.advanced.extend(item.broker_payload for item in batch)
        self._trigger_cls.outcomes.extend(item.outcome for item in batch)

    async def aclose(self):
        self._trigger_cls.aclose_calls += 1


def _make_ack_required_trigger_class(events_with_payloads: list[tuple], *, lane_for: Callable | None = None):
    """
    Return a fresh trigger class whose ``create_shared_stream_producer``
    returns a :class:`_RecordingProducer` yielding ``(event, broker_payload)``
    tuples and recording each advance call into class-level lists —
    ``batches`` keeps one list per call, ``advanced`` / ``outcomes`` are the
    flattened projections.
    """

    class _AckRequiredTrigger(_ProgrammableSharedStreamTrigger):
        advanced: list = []
        outcomes: list[AdvanceOutcome] = []
        batches: list[list] = []
        aclose_calls: int = 0

        @classmethod
        def create_shared_stream_producer(cls, kwargs):
            return _RecordingProducer(cls, events_with_payloads, lane_for=lane_for)

        async def filter_shared_stream(self, shared_stream):
            async for raw in shared_stream:
                yield TriggerEvent(raw)

    return _AckRequiredTrigger


def _make_reject_trigger_class(events_with_payloads: list[tuple], *, lane_for: Callable | None = None):
    """
    Like :func:`_make_ack_required_trigger_class`, but the filter rejects any
    raw event whose payload is truthy on ``"reject"`` (calling
    :func:`reject_shared_stream_event` instead of yielding) and yields a
    ``TriggerEvent`` for the rest.
    """

    class _RejectTrigger(_ProgrammableSharedStreamTrigger):
        advanced: list = []
        outcomes: list[AdvanceOutcome] = []
        batches: list[list] = []
        aclose_calls: int = 0

        @classmethod
        def create_shared_stream_producer(cls, kwargs):
            return _RecordingProducer(cls, events_with_payloads, lane_for=lane_for)

        async def filter_shared_stream(self, shared_stream):
            async for raw in shared_stream:
                if raw.get("reject"):
                    reject_shared_stream_event()
                    continue
                yield TriggerEvent(raw)

    return _RejectTrigger


async def _collect_ack_stream(trigger, stream, *, n: int) -> list:
    """Collect ``n`` TriggerEvent items from an ack-aware filter_shared_stream."""
    out = []
    async for event in trigger.filter_shared_stream(stream):
        out.append(event)
        if len(out) >= n:
            break
    return out


class _BackgroundCollector:
    """
    Items collected from a stream that is consumed like a real filter loop.

    Use via :func:`_consume_in_background`. The consuming task never breaks
    out of its ``async for``, so after each item the stream generator is
    resumed and the item's binding window closes — exactly what a production
    ``filter_shared_stream`` loop does when it goes back for the next raw
    event. Breaking out of the loop instead would leave the generator
    suspended at its yield with the binding window open, so the event could
    never resolve.
    """

    def __init__(self) -> None:
        self.items: list = []
        # The exception (e.g. an injected AckTimeout) that ended the stream,
        # if any; the consuming task records it here instead of propagating.
        self.error: BaseException | None = None

    async def wait_for(self, n: int, *, timeout: float = 1.0) -> list:
        """Wait until ``n`` items have been collected and return them."""
        deadline = asyncio.get_event_loop().time() + timeout
        while len(self.items) < n:
            if self.error is not None:
                raise self.error
            assert asyncio.get_event_loop().time() < deadline, (
                f"collector got {len(self.items)} of {n} items within {timeout}s"
            )
            await asyncio.sleep(0)
        return self.items[:n]


@asynccontextmanager
async def _consume_in_background(stream):
    """
    Consume ``stream`` in a background task, the way a real subscriber would.

    Yields a :class:`_BackgroundCollector`; the consuming task is cancelled
    on exit. A stream failure is recorded on ``collector.error`` rather than
    propagating out of the task.
    """
    collector = _BackgroundCollector()

    async def consume() -> None:
        try:
            async for item in stream:
                collector.items.append(item)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            collector.error = exc

    task = asyncio.create_task(consume())
    try:
        yield collector
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


@pytest.mark.parametrize(
    ("override_level", "expected"),
    [
        pytest.param("middle", True, id="middle-overrides-grandchild-inherits"),
        pytest.param(None, False, id="no-override-anywhere"),
        pytest.param("grandchild", True, id="grandchild-overrides-itself"),
    ],
)
def test_is_ack_required_multi_level_inheritance(override_level, expected):
    """``_is_ack_required`` walks the MRO down to ``BaseEventTrigger``: any class
    along the way that defines ``create_shared_stream_producer`` — including one
    a subclass merely inherits from — switches the group to ack mode.
    """

    class _Parent(_ProgrammableSharedStreamTrigger):
        pass

    class _Middle(_Parent):
        pass

    class _Grandchild(_Middle):
        pass

    # Detection only — the poll never runs, so returning None is fine.
    def create_shared_stream_producer(cls, kwargs):
        return None

    if override_level == "middle":
        _Middle.create_shared_stream_producer = classmethod(create_shared_stream_producer)
    elif override_level == "grandchild":
        _Grandchild.create_shared_stream_producer = classmethod(create_shared_stream_producer)

    group = _SharedStreamGroup(
        key="test-key",
        trigger_class=_Grandchild,
        kwargs={},
        on_poll_terminate=lambda g: None,
        max_subscriber_queue=8,
        ack_timeout=300.0,
        log=structlog.get_logger("test"),
    )

    assert group._is_ack_required() is expected


@pytest.mark.asyncio
async def test_ack_required_producer_advances_after_all_subscribers_resolve():
    """Producer's advance hook fires exactly once after both subscribers move past the event."""
    cls = _make_ack_required_trigger_class(
        [
            ({"msg": "hello"}, "receipt-1"),
        ]
    )
    cls.advanced.clear()
    cls.outcomes.clear()

    t1, t2 = cls(), cls()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()
    next_task = None
    try:
        s1 = manager.subscribe(trigger_id=1, trigger=t1, key=key)
        s2 = manager.subscribe(trigger_id=2, trigger=t2, key=key)

        # Subscriber 1 pulls the event and stays on it; subscriber 2 consumes
        # it like a real filter loop (and thereby resolves it).
        it1 = s1.__aiter__()
        await _pull_raw(it1)
        async with _consume_in_background(s2) as c2:
            await c2.wait_for(1)
            await asyncio.sleep(0)
            assert len(cls.advanced) == 0, "advance must not fire while one subscriber is still on the event"

            next_task = await _resume_past_yield(it1)
            await asyncio.sleep(0)  # let the advance pump run
            assert cls.advanced == ["receipt-1"], (
                "advance must fire exactly once after all subscribers resolve"
            )
    finally:
        await _cancel_quietly(next_task)
        await manager.unsubscribe(1, key)
        await manager.unsubscribe(2, key)


@pytest.mark.asyncio
async def test_ack_required_producer_does_not_advance_on_partial_resolution():
    """Producer does not advance while one of two subscribers is still on the event."""
    cls = _make_ack_required_trigger_class(
        [
            ({"msg": "partial"}, "receipt-partial"),
        ]
    )
    cls.advanced.clear()
    cls.outcomes.clear()

    t1, t2 = cls(), cls()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()
    try:
        s1 = manager.subscribe(trigger_id=1, trigger=t1, key=key)
        s2 = manager.subscribe(trigger_id=2, trigger=t2, key=key)

        # Subscriber 1 pulls the event but never resumes past it.
        it1 = s1.__aiter__()
        await _pull_raw(it1)
        # Subscriber 2 consumes and resolves it.
        async with _consume_in_background(s2) as c2:
            await c2.wait_for(1)
            await asyncio.sleep(0)
            assert cls.advanced == [], "producer must not advance while a subscriber is still on the event"
    finally:
        await manager.unsubscribe(1, key)
        await manager.unsubscribe(2, key)


@pytest.mark.asyncio
async def test_ack_timeout_force_fails_slow_subscriber_only():
    """A slow subscriber is force-failed after timeout; the fast subscriber and advance are unaffected."""
    cls = _make_ack_required_trigger_class(
        [
            ({"msg": "timeout-test"}, "receipt-timeout"),
        ]
    )
    cls.advanced.clear()
    cls.outcomes.clear()

    t_fast, t_slow = cls(), cls()
    key = t_fast.shared_stream_key()

    # Inject a fake clock so the test controls time without relying on wall-clock
    # or time_machine (which does not affect time.monotonic).
    fake_clock: list[float] = [0.0]

    def now() -> float:
        return fake_clock[0]

    # Very short timeout; cadence = max(0.01, 0.1/10) = 0.01s.
    manager = SharedStreamManager(ack_timeout=0.1, _now=now)
    try:
        s_fast = manager.subscribe(trigger_id=1, trigger=t_fast, key=key)
        s_slow = manager.subscribe(trigger_id=2, trigger=t_slow, key=key)

        async def drain_slow(stream):
            # Pull the raw event off the slow stream and stay on it — the
            # deliberately stalled subscriber the timeout must catch.
            async for _ in stream:
                break

        async with _consume_in_background(s_fast) as fast_collector:
            # The fast subscriber consumes the event and resolves; the slow
            # one stays on it.
            await fast_collector.wait_for(1)
            await asyncio.wait_for(drain_slow(s_slow), timeout=1.0)

            # Advance fake clock past ack_timeout; the timeout loop sees now() - created_at >= 0.1.
            fake_clock[0] = 0.2
            slow_queue = manager._groups[key]._subscribers.get(2)
            # Let _run_ack_timeout_loop tick through a few cadence cycles (cadence=0.01s real).
            await asyncio.sleep(0.05)

            assert slow_queue is not None
            sentinel = slow_queue.get_nowait()
            assert isinstance(sentinel, _PollFailure)
            assert isinstance(sentinel.exc, AckTimeout)

            # Fast subscriber (already resolved) must not have received a spurious AckTimeout.
            fast_queue = manager._groups[key]._subscribers.get(1)
            assert fast_queue is not None
            assert fast_queue.empty(), (
                "fast (already-resolved) subscriber must not receive a spurious AckTimeout"
            )
            assert fast_collector.error is None, (
                "fast (already-resolved) subscriber must not receive a spurious AckTimeout"
            )

            # After timeout the advance should have fired (pending emptied).
            await asyncio.sleep(0)
            assert cls.advanced == ["receipt-timeout"]
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_ack_timeout_at_cap_boundary_does_not_timeout():
    """Subscriber resolving inside the window (< ack_timeout) must NOT be force-failed."""
    cls = _make_ack_required_trigger_class(
        [
            ({"msg": "boundary-test"}, "receipt-boundary"),
        ]
    )
    cls.advanced.clear()
    cls.outcomes.clear()

    t1 = cls()
    key = t1.shared_stream_key()

    # Inject a fake clock: created_at will be recorded at fake_clock[0] = 0.0.
    fake_clock: list[float] = [0.0]

    def now() -> float:
        return fake_clock[0]

    manager = SharedStreamManager(ack_timeout=0.1, _now=now)
    next_task = None
    try:
        stream = manager.subscribe(trigger_id=1, trigger=t1, key=key)

        # Pull the event and stay on it so it remains outstanding.
        it = stream.__aiter__()
        await _pull_raw(it)

        # Advance fake clock to 99 ms — strictly inside the 100 ms ack_timeout window.
        # now() - created_at = 0.099 < 0.1, so the timeout loop must NOT fire.
        fake_clock[0] = 0.099
        # Let the timeout loop tick a few cadence cycles (cadence = max(0.01, 0.1/10) = 0.01s real).
        await asyncio.sleep(0.05)

        subscriber_queue = manager._groups[key]._subscribers.get(1)
        assert subscriber_queue is not None
        assert subscriber_queue.empty(), "subscriber must not have been force-failed at t=99ms"

        # Now move past the event — advance should fire.
        next_task = await _resume_past_yield(it)
        await asyncio.sleep(0)
        assert cls.advanced == ["receipt-boundary"]
    finally:
        await _cancel_quietly(next_task)
        await manager.stop_all()


@pytest.mark.asyncio
async def test_ack_timeout_exactly_at_timeout_force_fails():
    """Subscriber still on an event exactly ``ack_timeout`` old IS force-failed (elapsed >= timeout)."""
    cls = _make_ack_required_trigger_class(
        [
            ({"msg": "exact-boundary"}, "receipt-exact"),
        ]
    )
    cls.advanced.clear()
    cls.outcomes.clear()

    t1 = cls()
    key = t1.shared_stream_key()

    # Inject a fake clock: created_at will be recorded at fake_clock[0] = 0.0.
    fake_clock: list[float] = [0.0]

    def now() -> float:
        return fake_clock[0]

    manager = SharedStreamManager(ack_timeout=0.1, _now=now)
    try:
        stream = manager.subscribe(trigger_id=1, trigger=t1, key=key)

        async def drain_one(s):
            # Pull the raw event and stay on it — never resume past the yield.
            async for _ in s:
                break

        await asyncio.wait_for(drain_one(stream), timeout=1.0)

        # Advance fake clock to exactly the 100 ms ack_timeout.
        # now() - created_at == 0.1 is not < 0.1, so the timeout loop must fire —
        # this pins the boundary at >= rather than >.
        fake_clock[0] = 0.1
        # Let the timeout loop tick a few cadence cycles (cadence = max(0.01, 0.1/10) = 0.01s real).
        await asyncio.sleep(0.05)

        subscriber_queue = manager._groups[key]._subscribers.get(1)
        assert subscriber_queue is not None
        sentinel = subscriber_queue.get_nowait()
        assert isinstance(sentinel, _PollFailure)
        assert isinstance(sentinel.exc, AckTimeout)

        # The pending set emptied, so the producer advance fired.
        await asyncio.sleep(0)
        assert cls.advanced == ["receipt-exact"]
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("ack_timeout", "expected_cadence"),
    [
        (0.05, 0.01),  # 0.05/10 = 0.005 < 0.01  → floor clamps to 0.01
        (0.2, 0.02),  # 0.2/10  = 0.02  >= 0.01 → exact computed value
    ],
    ids=["floor-clamp", "computed"],
)
async def test_ack_timeout_cadence_floor(ack_timeout, expected_cadence):
    """Cadence floor: max(0.01, ack_timeout/10) is floored at 0.01 when ack_timeout/10 < 0.01."""
    cls = _make_ack_required_trigger_class([({"msg": "cadence-test"}, "receipt-cadence")])
    cls.advanced.clear()
    cls.outcomes.clear()

    t1 = cls()
    key = t1.shared_stream_key()

    first_sleep: list[float] = []
    real_sleep = asyncio.sleep

    async def capturing_sleep(delay):
        if not first_sleep:
            first_sleep.append(delay)
            # Stop after capturing so the test doesn't run real time.
            raise asyncio.CancelledError
        await real_sleep(delay)

    manager = SharedStreamManager(ack_timeout=ack_timeout)
    try:
        with mock.patch("airflow.triggers.shared_stream.asyncio.sleep", side_effect=capturing_sleep):
            manager.subscribe(trigger_id=1, trigger=t1, key=key)
            # Give the event loop a turn so the group starts and _run_ack_timeout_loop fires.
            with suppress(asyncio.CancelledError):
                await real_sleep(0.05)
    finally:
        await manager.stop_all()

    assert first_sleep, "capturing_sleep was never called — _run_ack_timeout_loop did not start"
    assert first_sleep[0] == pytest.approx(expected_cadence), (
        f"cadence for ack_timeout={ack_timeout} should be {expected_cadence}, got {first_sleep[0]}"
    )


@pytest.mark.asyncio
async def test_late_subscriber_does_not_block_advance_of_earlier_event():
    """A subscriber joining after event N is broadcast is not in event N's ack set."""
    event_broadcast = asyncio.Event()
    proceed = asyncio.Event()

    class _LateProducer(SharedStreamProducer):
        def __init__(self, trigger_cls):
            self._trigger_cls = trigger_cls

        async def open_stream(self):
            event_broadcast.set()
            await proceed.wait()
            yield {"msg": "late-test"}, "receipt-late"
            await asyncio.Event().wait()

        async def advance(self, batch):
            self._trigger_cls.advanced.extend(item.broker_payload for item in batch)

    class _LateTrigger(_ProgrammableSharedStreamTrigger):
        advanced: list = []

        @classmethod
        def create_shared_stream_producer(cls, kwargs):
            return _LateProducer(cls)

        async def filter_shared_stream(self, shared_stream):
            async for raw in shared_stream:
                yield TriggerEvent(raw)

    _LateTrigger.advanced.clear()

    t_early = _LateTrigger()
    t_late = _LateTrigger()
    key = t_early.shared_stream_key()
    manager = SharedStreamManager()
    try:
        s_early = manager.subscribe(trigger_id=1, trigger=t_early, key=key)

        # Wait for open_shared_stream to reach the proceed gate, then unblock.
        await asyncio.wait_for(event_broadcast.wait(), timeout=1.0)
        proceed.set()

        # Give the poll task a few ticks to run past the yield and put the
        # raw event into the early subscriber's queue.
        deadline = asyncio.get_event_loop().time() + 1.0
        while asyncio.get_event_loop().time() < deadline:
            if manager._groups[key]._outstanding:
                break
            await asyncio.sleep(0)

        # The event is now outstanding with only the early subscriber in
        # its pending set. Subscribe the late subscriber AFTER fan-out.
        manager.subscribe(trigger_id=2, trigger=t_late, key=key)

        # Confirm the late subscriber is NOT in the outstanding entry's pending set.
        entry = next(iter(manager._groups[key]._outstanding.values()), None)
        assert entry is not None
        assert 2 not in entry.pending, "late subscriber must not be in the pending set of earlier event"

        # Collect from early subscriber (the filter loop resolves the event).
        async with _consume_in_background(t_early.filter_shared_stream(s_early)) as collector:
            events = await collector.wait_for(1)
            assert len(events) == 1

            await asyncio.sleep(0)
            # The late subscriber did not participate in the pending set; advance must have fired.
            assert _LateTrigger.advanced == ["receipt-late"]
    finally:
        await manager.unsubscribe(1, key)
        await manager.unsubscribe(2, key)


@pytest.mark.asyncio
async def test_subscriber_unsubscribe_during_outstanding_event():
    """When a subscriber leaves while still on an event, the group advances without it."""
    cls = _make_ack_required_trigger_class(
        [
            ({"msg": "unsub-during-ack"}, "receipt-unsub"),
        ]
    )
    cls.advanced.clear()
    cls.outcomes.clear()

    t1, t2 = cls(), cls()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()
    try:
        s1 = manager.subscribe(trigger_id=1, trigger=t1, key=key)
        s2 = manager.subscribe(trigger_id=2, trigger=t2, key=key)

        # t1 consumes and resolves; t2 pulls the event, stays on it, then leaves.
        it2 = s2.__aiter__()
        async with _consume_in_background(s1) as c1:
            await c1.wait_for(1)
            await _pull_raw(it2)
            await asyncio.sleep(0)
            assert cls.advanced == [], "the event still waits on subscriber 2"

            await manager.unsubscribe(2, key)
            await asyncio.sleep(0)

            assert cls.advanced == ["receipt-unsub"], (
                "unsubscribe must resolve the departing subscriber so the producer can advance"
            )
    finally:
        await manager.unsubscribe(1, key)


@pytest.mark.asyncio
async def test_ack_mode_queue_full_during_fanout_does_not_break_iteration():
    """A full queue at fan-out time must not break the iteration of remaining subscribers."""
    cls = _make_ack_required_trigger_class([({"msg": "burst"}, "receipt-burst")])
    cls.advanced.clear()
    cls.outcomes.clear()

    t_full, t_ok = cls(), cls()
    key = t_full.shared_stream_key()
    # Queue of size 1 — pre-filling it will force QueueFull on fan-out for trigger 1.
    manager = SharedStreamManager(max_subscriber_queue=1)
    try:
        s_full = manager.subscribe(trigger_id=1, trigger=t_full, key=key)
        s_ok = manager.subscribe(trigger_id=2, trigger=t_ok, key=key)

        # Pre-fill trigger 1's queue so put_nowait raises QueueFull at fan-out.
        # The filler mirrors the internal (raw_event, event_id) queue shape.
        group = manager._groups[key]
        group._subscribers[1].put_nowait(("filler", -1))

        # t_full: its queue will be drained + replaced by _PollFailure(_SubscriberOverflow).
        # _drain() raises the inner exception, so we expect _SubscriberOverflow here.
        full_exc: list[BaseException] = []

        async def collect_full():
            try:
                async for _ in s_full:
                    pass
            except Exception as exc:
                full_exc.append(exc)

        # t_ok: drive its ack stream and collect the real event.
        async with _consume_in_background(t_ok.filter_shared_stream(s_ok)) as ok_collector:
            await asyncio.wait_for(collect_full(), timeout=2.0)
            ok_result = await ok_collector.wait_for(1)

            # Give the advance pump a tick to run.
            await asyncio.sleep(0)

            # t_full got an overflow failure (not a RuntimeError from set mutation).
            assert len(full_exc) == 1
            assert isinstance(full_exc[0], _SubscriberOverflow)

            # t_ok got the real event and resolved it → advance was triggered.
            assert len(ok_result) == 1
            assert ok_result[0].payload == {"msg": "burst"}
            assert cls.advanced == ["receipt-burst"]
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("lane_for", "events", "expected_per_lane"),
    [
        pytest.param(
            None,
            [
                ({"msg": "no-sub-1"}, "p1"),
                ({"msg": "no-sub-2"}, "p2"),
                ({"msg": "no-sub-3"}, "p3"),
            ],
            {None: ["p1", "p2", "p3"]},
            id="no-lane-global-order",
        ),
        pytest.param(
            lambda payload: payload[0],
            [
                ({"n": "a1"}, "a1"),
                ({"n": "b1"}, "b1"),
                ({"n": "a2"}, "a2"),
                ({"n": "b2"}, "b2"),
            ],
            {"a": ["a1", "a2"], "b": ["b1", "b2"]},
            id="lanes-per-lane-order",
        ),
    ],
)
async def test_empty_snapshot_advances_in_fanout_order(lane_for, events, expected_per_lane):
    """Events broadcast while no subscribers are online still advance, in fan-out order.

    Empty-snapshot entries are born resolved and go through the same ordered
    pump as everything else; their outcomes carry all-zero counts. Without
    lanes the full global order is preserved; with lanes the per-lane
    projections keep fan-out order.
    """
    cls = _make_ack_required_trigger_class(events, lane_for=lane_for)
    cls.advanced.clear()
    cls.outcomes.clear()

    t1 = cls()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()

    # Subscribe then immediately unsubscribe so the group exists but has zero subscribers
    # when the events arrive.
    s1 = manager.subscribe(trigger_id=1, trigger=t1, key=key)
    manager._groups[key].unsubscribe(1)  # remove without stopping the poll task

    # The poll task is still running; give it a few ticks to broadcast.
    deadline = asyncio.get_event_loop().time() + 1.0
    while asyncio.get_event_loop().time() < deadline:
        if len(cls.advanced) >= len(events):
            break
        await asyncio.sleep(0.01)

    # Per-lane projection of the advance calls; with no lane_for the
    # projection is the full global sequence.
    for lane, expected in expected_per_lane.items():
        projected = cls.advanced if lane_for is None else [p for p in cls.advanced if lane_for(p) == lane]
        assert projected == expected, (
            "advance must be called for every event, in fan-out order, even with no subscribers online"
        )
    assert len(cls.advanced) == len(events)
    assert cls.outcomes == [AdvanceOutcome(0, 0)] * len(events)
    assert all(outcome.is_clean for outcome in cls.outcomes)

    del s1  # silence unused-variable warning
    await manager.stop_all()


@pytest.mark.asyncio
async def test_producer_advance_exception_is_logged():
    """When producer.advance raises, the error is logged and the poll group stays alive."""
    proceed_flag = asyncio.Event()

    class _RaisingAdvanceProducer(SharedStreamProducer):
        async def open_stream(self):
            yield ({"msg": "event-1"}, "payload-1")
            await proceed_flag.wait()
            yield ({"msg": "event-2"}, "payload-2")
            await asyncio.Event().wait()

        async def advance(self, batch):
            raise RuntimeError("simulated broker failure")

    class _RaisingAdvanceTrigger(_ProgrammableSharedStreamTrigger):
        @classmethod
        def create_shared_stream_producer(cls, kwargs):
            return _RaisingAdvanceProducer()

        async def filter_shared_stream(self, shared_stream):
            async for raw in shared_stream:
                yield TriggerEvent(raw)

    t1 = _RaisingAdvanceTrigger()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()

    try:
        s1 = manager.subscribe(trigger_id=1, trigger=t1, key=key)

        # Intercept log.error on the group so we can assert it was called.
        group = manager._groups[key]
        mock_log_error = MagicMock()
        group.log = MagicMock()
        group.log.error = mock_log_error

        async with _consume_in_background(t1.filter_shared_stream(s1)) as collector:
            # Consume the first event — the filter loop resolves it and schedules advance.
            events1 = await collector.wait_for(1)
            assert len(events1) == 1

            # Give the advance pump a tick to run and raise.
            await asyncio.sleep(0.05)

            # log.error must have been called with a message about the failed advance.
            assert mock_log_error.called, "log.error must be called when producer.advance raises"
            call_args = mock_log_error.call_args
            assert "broker advance failed" in call_args[0][0], (
                f"log.error message must mention 'broker advance failed', got: {call_args[0][0]}"
            )

            # The poll group is still alive — the exception must not have killed it.
            assert key in manager._groups, "group must survive a producer.advance exception"

            # A second event can still be produced and consumed (the collector
            # count is cumulative across both events).
            proceed_flag.set()
            events2 = await collector.wait_for(2)
            assert len(events2) == 2
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_stop_drains_in_flight_advance():
    """stop() must await an advance already in flight so graceful shutdown does not drop it."""
    advance_started = asyncio.Event()
    advance_may_finish = asyncio.Event()
    advance_done_at: list[float] = []

    class _SlowAdvanceProducer(SharedStreamProducer):
        def __init__(self, trigger_cls):
            self._trigger_cls = trigger_cls

        async def open_stream(self):
            yield ({"msg": "slow"}, "receipt-slow")
            await asyncio.Event().wait()

        async def advance(self, batch):
            advance_started.set()
            await advance_may_finish.wait()
            await asyncio.sleep(0.05)  # ensure stop() must actually wait for us
            self._trigger_cls.advanced.extend(item.broker_payload for item in batch)
            advance_done_at.append(time.monotonic())

    class _SlowAdvanceTrigger(_ProgrammableSharedStreamTrigger):
        advanced: list = []

        @classmethod
        def create_shared_stream_producer(cls, kwargs):
            return _SlowAdvanceProducer(cls)

        async def filter_shared_stream(self, shared_stream):
            async for raw in shared_stream:
                yield TriggerEvent(raw)

    _SlowAdvanceTrigger.advanced.clear()

    t1 = _SlowAdvanceTrigger()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()

    s1 = manager.subscribe(trigger_id=1, trigger=t1, key=key)

    async with _consume_in_background(t1.filter_shared_stream(s1)) as collector:
        # Drive the one event through so the advance is dispatched.
        events = await collector.wait_for(1)
        assert len(events) == 1

        # Wait for producer.advance to actually start (confirms the advance is in-flight).
        await asyncio.wait_for(advance_started.wait(), timeout=1.0)

        # Unblock the advance hook and stop; advance still has a 50 ms sleep so stop()
        # must genuinely await the task rather than racing past it.
        advance_may_finish.set()
        await manager.stop_all()
        stop_returned_at = time.monotonic()

    # stop_all() must have waited for the in-flight advance to complete.
    assert _SlowAdvanceTrigger.advanced == ["receipt-slow"], (
        "stop() must drain the in-flight advance before returning"
    )
    assert advance_done_at, "the in-flight advance must have completed"
    assert advance_done_at[0] <= stop_returned_at, (
        "stop() must wait for in-flight advance to finish before returning"
    )


@pytest.mark.asyncio
async def test_ack_mode_late_subscriber_during_poll_failure_window_gets_fresh_group():
    """Ack-mode variant of the poll-failure-window race: tearing down the ack-timeout
    task must not introduce a yield point before the group's eviction + sentinel
    broadcast. A subscriber arriving in the same tick the poll died must create a
    fresh group rather than attach to the dead one.
    """
    invocations: list[int] = []

    class _AckWindowProducer(SharedStreamProducer):
        def __init__(self, trigger_cls):
            self._trigger_cls = trigger_cls

        async def open_stream(self):
            n = len(invocations)
            invocations.append(n)
            if n == 0:
                raise RuntimeError("boom")
            yield {"region": "fresh"}, "receipt-fresh"
            await asyncio.Event().wait()

        async def advance(self, batch):
            self._trigger_cls.advanced.extend(item.broker_payload for item in batch)

    class _AckWindowTrigger(_ProgrammableSharedStreamTrigger):
        advanced: list = []

        @classmethod
        def create_shared_stream_producer(cls, kwargs):
            return _AckWindowProducer(cls)

        async def filter_shared_stream(self, shared_stream):
            async for raw in shared_stream:
                yield TriggerEvent(raw)

    _AckWindowTrigger.advanced.clear()
    trigger = _AckWindowTrigger()
    manager = SharedStreamManager()
    key = trigger.shared_stream_key()

    stream1 = manager.subscribe(trigger_id=1, trigger=trigger, key=key)
    old_poll_task = manager._groups[key]._poll_task
    assert old_poll_task is not None

    # One tick: the poll task runs, the upstream raises immediately, and the finally
    # block executes. Cancelling the ack-timeout task must not make the poll yield
    # before it has evicted the key and broadcast the sentinel.
    await asyncio.sleep(0)
    assert key not in manager._groups, (
        "the failing ack-mode poll must evict its group synchronously — awaiting the "
        "cancelled ack-timeout task must not reopen the late-subscriber window"
    )

    # A subscriber arriving inside that same tick must get a fresh group.
    stream2 = manager.subscribe(trigger_id=2, trigger=trigger, key=key)
    try:
        events = await asyncio.wait_for(_collect_ack_stream(trigger, stream2, n=1), timeout=1.0)
        assert events[0].payload == {"region": "fresh"}
    finally:
        # The original subscriber still has the failure sentinel waiting for it.
        with pytest.raises(RuntimeError, match="boom"):
            await asyncio.wait_for(
                _collect(trigger.filter_shared_stream(stream1), n=1),
                timeout=1.0,
            )
        await old_poll_task
        await manager.unsubscribe(1, key)
        await manager.unsubscribe(2, key)

    assert invocations == [0, 1]


@pytest.mark.asyncio
async def test_directory_file_delete_trigger_path_unchanged():
    """Triggers that do NOT override create_shared_stream_producer use the fast path.

    Subscribers receive raw events (not tuples) — identical to pre-ack-mode behavior.
    """
    cls = _make_trigger_class(_events_then_block([{"filename": "flag.txt"}]))
    # Confirm create_shared_stream_producer is NOT overridden (fast path).
    # We check that no class in the MRO before BaseEventTrigger defines it.
    assert "create_shared_stream_producer" not in cls.__dict__, (
        "create_shared_stream_producer must NOT be in the class's own __dict__ for fast-path detection"
    )

    trigger = cls()
    manager = SharedStreamManager()
    key = trigger.shared_stream_key()
    try:
        stream = manager.subscribe(trigger_id=1, trigger=trigger, key=key)
        events = await _collect(trigger.filter_shared_stream(stream), n=1)
        assert len(events) == 1
        # The event is a raw dict, not a tuple.
        assert isinstance(events[0].payload, dict)
        assert events[0].payload == {"filename": "flag.txt"}
    finally:
        await manager.unsubscribe(1, key)


@pytest.mark.asyncio
async def test_out_of_order_resolve_still_advances_in_fanout_order():
    """Events resolved out of order are advanced strictly in fan-out order."""
    cls = _make_ack_required_trigger_class(
        [
            ({"n": 1}, "p1"),
            ({"n": 2}, "p2"),
            ({"n": 3}, "p3"),
        ]
    )
    cls.advanced.clear()
    cls.outcomes.clear()

    t1 = cls()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()
    next_task = None
    try:
        s1 = manager.subscribe(trigger_id=1, trigger=t1, key=key)

        # Consume all three events, binding one persist seq to each so the
        # confirmation order — not the consumption order — decides when each
        # event resolves.
        it = s1.__aiter__()
        seqs: list[int] = []
        for _ in range(3):
            await _pull_raw(it)
            seq = manager.bind_pending_event(trigger_id=1, key=key)
            assert seq is not None
            seqs.append(seq)
        next_task = await _resume_past_yield(it)

        # Resolve events 2 and 3 first — nothing may advance while event 1 is pending.
        manager.confirm_persisted([seqs[1], seqs[2]])
        await asyncio.sleep(0)
        assert cls.advanced == [], "no advance may run while the head event is unresolved"

        manager.confirm_persisted([seqs[0]])
        await asyncio.sleep(0)
        assert cls.advanced == ["p1", "p2", "p3"], "advances must run in fan-out order"
    finally:
        await _cancel_quietly(next_task)
        await manager.unsubscribe(1, key)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("lane_for", "payloads", "expected_advanced", "ordered"),
    [
        pytest.param(None, ["p1", "p2"], ["p1", "p2"], True, id="single-lane-full-order"),
        pytest.param(lambda payload: payload[0], ["a1", "b1"], ["a1", "b1"], False, id="across-lanes"),
    ],
)
async def test_pump_serializes_advance_calls(lane_for, payloads, expected_advanced, ordered):
    """Events resolved together never produce overlapping advance calls.

    This holds within a single lane (where the full fan-out order is also
    preserved) and across lanes (where the relative order is not guaranteed,
    but at most one advance call is ever in flight globally).
    """
    release = asyncio.Event()
    state = {"in_flight": 0, "max_in_flight": 0}
    advanced: list = []

    def make_blocking_trigger():
        class _BlockingAdvanceProducer(SharedStreamProducer):
            async def open_stream(self):
                for payload in payloads:
                    yield {"n": payload}, payload
                await asyncio.Event().wait()

            def get_advance_lane(self, broker_payload):
                if lane_for is None:
                    return super().get_advance_lane(broker_payload)
                return lane_for(broker_payload)

            async def advance(self, batch):
                state["in_flight"] += 1
                state["max_in_flight"] = max(state["max_in_flight"], state["in_flight"])
                await release.wait()
                advanced.extend(item.broker_payload for item in batch)
                state["in_flight"] -= 1

        class _BlockingAdvanceTrigger(_ProgrammableSharedStreamTrigger):
            @classmethod
            def create_shared_stream_producer(cls, kwargs):
                return _BlockingAdvanceProducer()

            async def filter_shared_stream(self, shared_stream):
                async for raw in shared_stream:
                    yield TriggerEvent(raw)

        return _BlockingAdvanceTrigger()

    t1 = make_blocking_trigger()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()
    try:
        s1 = manager.subscribe(trigger_id=1, trigger=t1, key=key)

        async with _consume_in_background(s1) as collector:
            # Consuming both events resolves them; the pump starts the first
            # advance, which blocks. No second call may start while one is in
            # flight.
            await collector.wait_for(2)
            for _ in range(5):
                await asyncio.sleep(0)
            assert state["in_flight"] == 1, "exactly one advance may be in flight"
            assert advanced == []

            release.set()
            deadline = asyncio.get_event_loop().time() + 1.0
            while asyncio.get_event_loop().time() < deadline:
                if len(advanced) >= 2:
                    break
                await asyncio.sleep(0)

            if ordered:
                assert advanced == expected_advanced
            else:
                assert sorted(advanced) == expected_advanced
            assert state["max_in_flight"] == 1, "advance calls must never overlap, even across lanes"
    finally:
        await manager.unsubscribe(1, key)


@pytest.mark.asyncio
async def test_lanes_advance_independently():
    """A lane whose events are resolved does not wait for another lane's unresolved head."""
    cls = _make_ack_required_trigger_class(
        [
            ({"n": "a1"}, "a1"),
            ({"n": "b1"}, "b1"),
            ({"n": "a2"}, "a2"),
            ({"n": "b2"}, "b2"),
        ],
        lane_for=lambda payload: payload[0],
    )
    cls.advanced.clear()
    cls.outcomes.clear()

    t1 = cls()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()
    next_task = None
    try:
        s1 = manager.subscribe(trigger_id=1, trigger=t1, key=key)
        group = manager._groups[key]

        # Consume all four events, binding a persist seq to each lane-"a"
        # event so lane "a" stays unresolved while lane "b" resolves from
        # consumption alone.
        it = s1.__aiter__()
        seqs: dict[str, int] = {}
        for _ in range(4):
            raw = await _pull_raw(it)
            if raw["n"].startswith("a"):
                seq = manager.bind_pending_event(trigger_id=1, key=key)
                assert seq is not None
                seqs[raw["n"]] = seq
        next_task = await _resume_past_yield(it)

        for _ in range(5):
            await asyncio.sleep(0)
        assert cls.advanced == ["b1", "b2"], "lane b must advance without waiting for lane a's head"

        manager.confirm_persisted([seqs["a1"]])
        manager.confirm_persisted([seqs["a2"]])
        for _ in range(5):
            await asyncio.sleep(0)

        # Contract: full per-lane projections in fan-out order. The global
        # interleaving across lanes is a pump implementation detail.
        assert [p for p in cls.advanced if p[0] == "a"] == ["a1", "a2"]
        assert [p for p in cls.advanced if p[0] == "b"] == ["b1", "b2"]
        assert len(cls.advanced) == 4
        # All lanes drained → the per-lane index is garbage-collected.
        assert group._lane_queues == {}
    finally:
        await _cancel_quietly(next_task)
        await manager.unsubscribe(1, key)


@pytest.mark.asyncio
async def test_lane_internal_fifo_with_out_of_order_resolve():
    """Out-of-order resolves within a lane still advance in fan-out order, per lane."""
    cls = _make_ack_required_trigger_class(
        [
            ({"n": "a1"}, "a1"),
            ({"n": "b1"}, "b1"),
            ({"n": "a2"}, "a2"),
            ({"n": "a3"}, "a3"),
        ],
        lane_for=lambda payload: payload[0],
    )
    cls.advanced.clear()
    cls.outcomes.clear()

    t1 = cls()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()
    next_task = None
    try:
        s1 = manager.subscribe(trigger_id=1, trigger=t1, key=key)

        # Consume all four events; only a1 gets a persist seq, so a2, a3, and
        # b1 resolve from consumption while head a1 stays pending.
        it = s1.__aiter__()
        a1_seq: int | None = None
        for _ in range(4):
            raw = await _pull_raw(it)
            if raw["n"] == "a1":
                a1_seq = manager.bind_pending_event(trigger_id=1, key=key)
        next_task = await _resume_past_yield(it)

        for _ in range(5):
            await asyncio.sleep(0)
        assert cls.advanced == ["b1"], "lane a may not advance while its own head is unresolved"

        assert a1_seq is not None
        manager.confirm_persisted([a1_seq])
        for _ in range(5):
            await asyncio.sleep(0)

        assert [p for p in cls.advanced if p[0] == "a"] == ["a1", "a2", "a3"]
        assert [p for p in cls.advanced if p[0] == "b"] == ["b1"]
        assert len(cls.advanced) == 4
    finally:
        await _cancel_quietly(next_task)
        await manager.unsubscribe(1, key)


@pytest.mark.asyncio
async def test_get_advance_lane_raise_terminates_poll():
    """A get_advance_lane that raises fails the poll like any other poll failure."""
    proceed_flag = asyncio.Event()
    aclose_calls: list[int] = []
    advanced: list = []

    class _RaisingLaneProducer(SharedStreamProducer):
        async def open_stream(self):
            yield {"n": 1}, "p1"
            await proceed_flag.wait()
            yield {"n": 2}, "p2"
            await asyncio.Event().wait()  # pragma: no cover

        def get_advance_lane(self, broker_payload):
            if broker_payload == "p2":
                raise RuntimeError("lane boom")
            return None

        async def advance(self, batch):
            advanced.extend(item.broker_payload for item in batch)

        async def aclose(self):
            aclose_calls.append(1)

    class _RaisingLaneTrigger(_ProgrammableSharedStreamTrigger):
        @classmethod
        def create_shared_stream_producer(cls, kwargs):
            return _RaisingLaneProducer()

        async def filter_shared_stream(self, shared_stream):
            async for raw in shared_stream:
                yield TriggerEvent(raw)

    t1 = _RaisingLaneTrigger()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()

    s1 = manager.subscribe(trigger_id=1, trigger=t1, key=key)
    poll_task = manager._groups[key]._poll_task
    assert poll_task is not None

    # Consume the first event, then release the event whose
    # get_advance_lane raises.
    events1 = await asyncio.wait_for(_collect_ack_stream(t1, s1, n=1), timeout=1.0)
    assert len(events1) == 1
    proceed_flag.set()

    async def consume():
        async for _ in s1:
            pass

    with pytest.raises(RuntimeError, match="lane boom"):
        await asyncio.wait_for(consume(), timeout=1.0)

    assert key not in manager._groups, "the group must be evicted when get_advance_lane raises"
    await asyncio.wait_for(poll_task, timeout=1.0)
    assert sum(aclose_calls) == 1
    # The first event was resolved before the failure → drained on the stop path.
    assert advanced == ["p1"]
    await manager.unsubscribe(1, key)


@pytest.mark.asyncio
async def test_stop_drains_all_lanes():
    """On stop the pump keeps draining until every resolved event in every lane is advanced."""
    release = asyncio.Event()
    advanced: list = []

    class _SlowFirstAdvanceProducer(SharedStreamProducer):
        async def open_stream(self):
            yield {"n": "a1"}, "a1"
            yield {"n": "a2"}, "a2"
            yield {"n": "b1"}, "b1"
            yield {"n": "b2"}, "b2"
            await asyncio.Event().wait()

        def get_advance_lane(self, broker_payload):
            return broker_payload[0]

        async def advance(self, batch):
            if batch[0].broker_payload == "a1":
                await release.wait()
            advanced.extend(item.broker_payload for item in batch)

    class _SlowFirstAdvanceTrigger(_ProgrammableSharedStreamTrigger):
        @classmethod
        def create_shared_stream_producer(cls, kwargs):
            return _SlowFirstAdvanceProducer()

        async def filter_shared_stream(self, shared_stream):
            async for raw in shared_stream:
                yield TriggerEvent(raw)

    t1 = _SlowFirstAdvanceTrigger()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()

    s1 = manager.subscribe(trigger_id=1, trigger=t1, key=key)

    # Consume all four events; every event except a1 gets a persist seq, so
    # only a1 resolves from consumption alone.
    it = s1.__aiter__()
    seqs: dict[str, int] = {}
    for _ in range(4):
        raw = await _pull_raw(it)
        if raw["n"] != "a1":
            seq = manager.bind_pending_event(trigger_id=1, key=key)
            assert seq is not None
            seqs[raw["n"]] = seq

    # Start a1's advance and let it block in flight.
    for _ in range(5):
        await asyncio.sleep(0)

    # Resolve everything else while a1's advance is still in flight (b2's
    # window closes on unsubscribe), then stop the group before the pump has
    # a chance to dispatch them.
    manager.confirm_persisted(seqs.values())
    stop_task = asyncio.create_task(manager.unsubscribe(1, key))
    for _ in range(5):
        await asyncio.sleep(0)
    release.set()
    await asyncio.wait_for(stop_task, timeout=1.0)

    # Drain-on-stop: a single harvesting pass would miss a2 and b2, which
    # only become dispatchable after the first pass already visited their lanes.
    assert [p for p in advanced if p[0] == "a"] == ["a1", "a2"]
    assert [p for p in advanced if p[0] == "b"] == ["b1", "b2"]
    assert len(advanced) == 4


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("second_action", "expected", "expected_clean"),
    [
        pytest.param("consume", AdvanceOutcome(acked=2, failed=0), True, id="all-consume"),
        pytest.param(
            "unsubscribe",
            AdvanceOutcome(acked=2, failed=0),
            True,
            id="unsubscribe-counts-as-acked",
        ),
    ],
)
async def test_outcome_classification(second_action, expected, expected_clean):
    """Each subscriber online at broadcast time is counted in exactly one outcome field."""
    cls = _make_ack_required_trigger_class([({"msg": "outcome"}, "receipt-outcome")])
    cls.advanced.clear()
    cls.outcomes.clear()

    t1, t2 = cls(), cls()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()
    next_task = None
    try:
        s1 = manager.subscribe(trigger_id=1, trigger=t1, key=key)
        s2 = manager.subscribe(trigger_id=2, trigger=t2, key=key)

        it2 = s2.__aiter__()
        async with _consume_in_background(s1) as c1:
            await c1.wait_for(1)
            await _pull_raw(it2)
            if second_action == "consume":
                next_task = await _resume_past_yield(it2)
            else:
                await manager.unsubscribe(2, key)
            await asyncio.sleep(0)

            assert cls.advanced == ["receipt-outcome"]
            assert cls.outcomes == [expected]
            assert cls.outcomes[0].is_clean is expected_clean
            # Invariant: every subscriber in the broadcast snapshot is counted exactly once.
            outcome = cls.outcomes[0]
            assert outcome.acked + outcome.failed == 2
    finally:
        await _cancel_quietly(next_task)
        await manager.unsubscribe(1, key)
        if second_action != "unsubscribe":
            await manager.unsubscribe(2, key)


@pytest.mark.asyncio
async def test_outcome_counts_timeout_as_failed():
    """A subscriber force-failed by the ack timeout is counted as failed in the outcome."""
    cls = _make_ack_required_trigger_class([({"msg": "timeout-outcome"}, "receipt-to")])
    cls.advanced.clear()
    cls.outcomes.clear()

    t_fast, t_slow = cls(), cls()
    key = t_fast.shared_stream_key()

    fake_clock: list[float] = [0.0]

    def now() -> float:
        return fake_clock[0]

    manager = SharedStreamManager(ack_timeout=0.1, _now=now)
    try:
        s_fast = manager.subscribe(trigger_id=1, trigger=t_fast, key=key)
        s_slow = manager.subscribe(trigger_id=2, trigger=t_slow, key=key)

        # The fast subscriber consumes and resolves; the slow one pulls the
        # raw event and stays on it.
        it_slow = s_slow.__aiter__()
        async with _consume_in_background(s_fast) as c_fast:
            await c_fast.wait_for(1)
            await _pull_raw(it_slow)

            fake_clock[0] = 0.2
            # Let the timeout loop tick (cadence = 0.01s real) and the pump dispatch.
            await asyncio.sleep(0.05)

            assert cls.advanced == ["receipt-to"]
            assert cls.outcomes == [AdvanceOutcome(acked=1, failed=1)]
            assert not cls.outcomes[0].is_clean
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_reject_counts_as_rejected_not_acked_or_failed():
    """A single subscriber rejecting an event resolves it as rejected, not acked or failed."""
    cls = _make_reject_trigger_class([({"reject": True}, "receipt-reject")])
    cls.advanced.clear()
    cls.outcomes.clear()

    t1 = cls()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()
    try:
        s1 = manager.subscribe(trigger_id=1, trigger=t1, key=key)
        async with _consume_in_background(t1.filter_shared_stream(s1)) as collector:
            # The filter rejects the only event, so nothing is ever yielded;
            # give the drain loop and the advance pump a tick to resolve and
            # dispatch.
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            assert cls.advanced == ["receipt-reject"]
            assert cls.outcomes == [AdvanceOutcome(acked=0, failed=0, rejected=1)]
            assert collector.items == []
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_reject_resolves_immediately_without_persist_gate():
    """A reject advances at once, without waiting on any persist confirmation."""
    cls = _make_reject_trigger_class([({"reject": True}, "receipt-now")])
    cls.advanced.clear()
    cls.outcomes.clear()

    # A fake clock pinned at 0 proves the advance does not come from the ack
    # timeout path: no time elapses, yet the reject still advances.
    fake_clock: list[float] = [0.0]

    def now() -> float:
        return fake_clock[0]

    t1 = cls()
    key = t1.shared_stream_key()
    manager = SharedStreamManager(ack_timeout=0.1, _now=now)
    try:
        s1 = manager.subscribe(trigger_id=1, trigger=t1, key=key)
        async with _consume_in_background(t1.filter_shared_stream(s1)):
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            # No persist confirmation sent and the clock never moved, yet the
            # rejected event is already advanced.
            assert cls.advanced == ["receipt-now"]
            assert cls.outcomes == [AdvanceOutcome(acked=0, failed=0, rejected=1)]
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_fan_out_mixed_ack_reject_fail_outcome():
    """Three subscribers ack / reject / fail the same event; the outcome counts each once."""
    fake_clock: list[float] = [0.0]

    def now() -> float:
        return fake_clock[0]

    # One producer broadcasts a single event to three subscribers; subscriber
    # behaviour is decided per trigger class.
    ack_cls = _make_ack_required_trigger_class([({"msg": "mixed"}, "receipt-mixed")])
    ack_cls.advanced.clear()
    ack_cls.outcomes.clear()

    t_ack, t_reject, t_slow = ack_cls(), ack_cls(), ack_cls()
    key = t_ack.shared_stream_key()
    manager = SharedStreamManager(ack_timeout=0.1, _now=now)
    next_task = None
    try:
        s_ack = manager.subscribe(trigger_id=1, trigger=t_ack, key=key)
        s_reject = manager.subscribe(trigger_id=2, trigger=t_reject, key=key)
        s_slow = manager.subscribe(trigger_id=3, trigger=t_slow, key=key)

        it_reject = s_reject.__aiter__()
        it_slow = s_slow.__aiter__()
        async with _consume_in_background(s_ack) as c_ack:
            # Subscriber 1 consumes the event normally (acked).
            await c_ack.wait_for(1)
            # Subscriber 2 pulls the event and rejects it directly through the
            # group, mirroring what reject_shared_stream_event would do while
            # this subscriber's window is open.
            await _pull_raw(it_reject)
            manager._groups[key]._reject_pending_event(trigger_id=2, event_id=0)
            # Subscriber 3 pulls the event and stalls, to be failed by the timeout.
            await _pull_raw(it_slow)

            fake_clock[0] = 0.2
            await asyncio.sleep(0.05)

            assert ack_cls.advanced == ["receipt-mixed"]
            assert ack_cls.outcomes == [AdvanceOutcome(acked=1, failed=1, rejected=1)]
            outcome = ack_cls.outcomes[0]
            assert outcome.acked + outcome.rejected + outcome.failed == 3
    finally:
        await _cancel_quietly(next_task)
        await manager.stop_all()


@pytest.mark.asyncio
async def test_reject_does_not_block_later_event_in_same_lane():
    """A rejected event and a fully acked event both advance, in fan-out order."""
    cls = _make_reject_trigger_class(
        [
            ({"reject": True}, "receipt-a"),
            ({"reject": False, "msg": "b"}, "receipt-b"),
        ]
    )
    cls.advanced.clear()
    cls.outcomes.clear()

    t1 = cls()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()
    try:
        s1 = manager.subscribe(trigger_id=1, trigger=t1, key=key)
        async with _consume_in_background(t1.filter_shared_stream(s1)) as collector:
            # Event A is rejected (no TriggerEvent), event B yields one.
            await collector.wait_for(1)
            await asyncio.sleep(0)
            await asyncio.sleep(0)

            assert cls.advanced == ["receipt-a", "receipt-b"]
            assert cls.outcomes == [
                AdvanceOutcome(acked=0, failed=0, rejected=1),
                AdvanceOutcome(acked=1, failed=0, rejected=0),
            ]
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_reject_on_fast_path_is_no_op_and_event_still_fires():
    """On the fast path the reject signal is ignored and the event fires normally."""

    # The fast path has no producer: build a fast-mode trigger whose filter
    # still calls reject_shared_stream_event for one event.
    class _FastRejectTrigger(_ProgrammableSharedStreamTrigger):
        async def filter_shared_stream(self, shared_stream):
            async for raw in shared_stream:
                if raw.get("reject"):
                    reject_shared_stream_event()
                yield TriggerEvent(raw)

    events = [{"reject": True, "msg": "still-fires"}]

    async def _open(cls_, kwargs):
        for event in events:
            yield event
        await asyncio.Event().wait()

    _FastRejectTrigger.open_shared_stream = classmethod(_open)

    t1 = _FastRejectTrigger()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()
    mock_log = MagicMock()
    try:
        s1 = manager.subscribe(trigger_id=1, trigger=t1, key=key)
        with mock.patch("airflow.triggers.shared_stream.log", mock_log):
            collected = await asyncio.wait_for(_collect(t1.filter_shared_stream(s1), n=1), timeout=1.0)
        # The event fired despite the reject call, and the warning was logged.
        assert [e.payload for e in collected] == [{"reject": True, "msg": "still-fires"}]
        assert mock_log.warning.mock_calls == [
            mock.call("reject_shared_stream_event called outside an ack-mode binding window; ignored")
        ]
    finally:
        await manager.unsubscribe(1, key)


@pytest.mark.asyncio
async def test_reject_outside_any_window_is_no_op():
    """Calling the free function with no open window logs a warning and does not raise."""
    mock_log = MagicMock()
    with mock.patch("airflow.triggers.shared_stream.log", mock_log):
        reject_shared_stream_event()  # must not raise
    assert mock_log.warning.mock_calls == [
        mock.call("reject_shared_stream_event called outside an ack-mode binding window; ignored")
    ]


@pytest.mark.parametrize(
    ("outcome", "expected_clean"),
    [
        pytest.param(AdvanceOutcome(acked=2, failed=0, rejected=0), True, id="no-reject-no-fail"),
        pytest.param(AdvanceOutcome(acked=2, failed=0, rejected=1), False, id="reject-makes-unclean"),
        pytest.param(AdvanceOutcome(acked=2, failed=1, rejected=0), False, id="fail-makes-unclean"),
    ],
)
def test_is_clean_requires_no_reject_and_no_fail(outcome, expected_clean):
    """``is_clean`` is True only when both ``failed`` and ``rejected`` are zero."""
    assert outcome.is_clean is expected_clean


@pytest.mark.asyncio
async def test_reject_then_yield_real_event_fires_but_does_not_bind():
    """A subscriber that rejects an event and then yields one anyway: the late event fires but binds nothing."""

    class _RejectThenYieldTrigger(_ProgrammableSharedStreamTrigger):
        advanced: list = []
        outcomes: list[AdvanceOutcome] = []
        batches: list[list] = []
        aclose_calls: int = 0

        @classmethod
        def create_shared_stream_producer(cls, kwargs):
            return _RecordingProducer(cls, [({"reject": True}, "receipt-x")], lane_for=None)

        async def filter_shared_stream(self, shared_stream):
            async for raw in shared_stream:
                reject_shared_stream_event()
                # Subscriber-side logic error: still yield after rejecting.
                yield TriggerEvent(raw)

    cls = _RejectThenYieldTrigger
    cls.advanced.clear()
    cls.outcomes.clear()

    t1 = cls()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()
    try:
        s1 = manager.subscribe(trigger_id=1, trigger=t1, key=key)
        async with _consume_in_background(t1.filter_shared_stream(s1)) as collector:
            # The trigger event still fires despite the preceding reject.
            fired = await collector.wait_for(1)
            assert [e.payload for e in fired] == [{"reject": True}]

            # The reject already popped the binding, so the late event binds
            # nothing — bind_pending_event returns None and draws no seq.
            assert manager.bind_pending_event(trigger_id=1, key=key) is None

            await asyncio.sleep(0)
            assert cls.advanced == ["receipt-x"]
            assert cls.outcomes == [AdvanceOutcome(acked=0, failed=0, rejected=1)]
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_aclose_called_once_on_stop_path():
    """The producer is closed exactly once when the last subscriber leaves."""
    cls = _make_ack_required_trigger_class([({"msg": "bye"}, "receipt-bye")])
    cls.advanced.clear()
    cls.outcomes.clear()

    t1 = cls()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()

    s1 = manager.subscribe(trigger_id=1, trigger=t1, key=key)
    events = await asyncio.wait_for(_collect_ack_stream(t1, s1, n=1), timeout=1.0)
    assert len(events) == 1

    await manager.unsubscribe(1, key)  # last subscriber → group stop
    assert cls.aclose_calls == 1


@pytest.mark.asyncio
async def test_aclose_called_once_on_terminal_path():
    """The producer is closed exactly once when its open_stream raises."""
    aclose_calls: list[int] = []

    class _FailingOpenProducer(SharedStreamProducer):
        async def open_stream(self):
            raise RuntimeError("open boom")
            yield  # pragma: no cover

        async def advance(self, batch):  # pragma: no cover
            pass

        async def aclose(self):
            aclose_calls.append(1)

    class _FailingOpenTrigger(_ProgrammableSharedStreamTrigger):
        @classmethod
        def create_shared_stream_producer(cls, kwargs):
            return _FailingOpenProducer()

        async def filter_shared_stream(self, shared_stream):
            async for raw in shared_stream:  # pragma: no cover
                yield TriggerEvent(raw)

    t1 = _FailingOpenTrigger()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()

    stream = manager.subscribe(trigger_id=1, trigger=t1, key=key)
    poll_task = manager._groups[key]._poll_task
    assert poll_task is not None

    async def consume():
        async for _ in stream:
            pass

    with pytest.raises(RuntimeError, match="open boom"):
        await asyncio.wait_for(consume(), timeout=1.0)

    assert key not in manager._groups
    # The poll swallows the terminal exception after broadcasting it; once the
    # task completes, the producer must have been closed exactly once.
    await asyncio.wait_for(poll_task, timeout=1.0)
    assert sum(aclose_calls) == 1
    await manager.unsubscribe(1, key)


@pytest.mark.asyncio
async def test_pump_continues_after_advance_raises():
    """An advance that raises is logged; the pump still advances the next event."""
    attempted: list = []

    class _FirstRaiseProducer(SharedStreamProducer):
        async def open_stream(self):
            yield {"n": 1}, "p1"
            yield {"n": 2}, "p2"
            await asyncio.Event().wait()

        async def advance(self, batch):
            attempted.extend(item.broker_payload for item in batch)
            if any(item.broker_payload == "p1" for item in batch):
                raise RuntimeError("p1 boom")

    class _FirstRaiseTrigger(_ProgrammableSharedStreamTrigger):
        @classmethod
        def create_shared_stream_producer(cls, kwargs):
            return _FirstRaiseProducer()

        async def filter_shared_stream(self, shared_stream):
            async for raw in shared_stream:
                yield TriggerEvent(raw)

    t1 = _FirstRaiseTrigger()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()
    next_task = None
    try:
        s1 = manager.subscribe(trigger_id=1, trigger=t1, key=key)
        group = manager._groups[key]
        group.log = MagicMock()

        # Move past event 1; its advance dispatches alone and raises.
        it = s1.__aiter__()
        await _pull_raw(it)
        pull2 = await _resume_past_yield(it)
        for _ in range(5):
            await asyncio.sleep(0)
        assert attempted == ["p1"]

        # Move past event 2; the pump must still dispatch it.
        await asyncio.wait_for(pull2, timeout=1.0)
        next_task = await _resume_past_yield(it)
        for _ in range(5):
            await asyncio.sleep(0)

        assert attempted == ["p1", "p2"], "the pump must move on to event 2 after event 1's advance raised"
        error_calls = group.log.error.mock_calls
        assert len(error_calls) == 1
        assert "broker advance failed" in error_calls[0].args[0]
    finally:
        await _cancel_quietly(next_task)
        await manager.unsubscribe(1, key)


@pytest.mark.asyncio
async def test_force_fail_clears_subscriber_from_all_outstanding_entries():
    """A force-failed subscriber stops head-blocking earlier events' advances."""
    gate = asyncio.Event()

    class _GatedProducer(SharedStreamProducer):
        def __init__(self, trigger_cls):
            self._trigger_cls = trigger_cls

        async def open_stream(self):
            yield {"n": 1}, "p1"
            yield {"n": 2}, "p2"
            await gate.wait()
            yield {"n": 3}, "p3"
            await asyncio.Event().wait()

        async def advance(self, batch):
            self._trigger_cls.advanced.extend(item.broker_payload for item in batch)
            self._trigger_cls.outcomes.extend(item.outcome for item in batch)

    class _GatedTrigger(_ProgrammableSharedStreamTrigger):
        advanced: list = []
        outcomes: list[AdvanceOutcome] = []

        @classmethod
        def create_shared_stream_producer(cls, kwargs):
            return _GatedProducer(cls)

        async def filter_shared_stream(self, shared_stream):
            async for raw in shared_stream:
                yield TriggerEvent(raw)

    t_dead, t_live = _GatedTrigger(), _GatedTrigger()
    key = t_dead.shared_stream_key()
    # maxsize=2: the dead subscriber's queue fills with events 1-2 and overflows
    # at event 3's fan-out. The default 300s ack timeout would stall the test
    # if force-fail did not clear the dead subscriber from all entries.
    manager = SharedStreamManager(max_subscriber_queue=2)
    try:
        manager.subscribe(trigger_id=1, trigger=t_dead, key=key)
        s_live = manager.subscribe(trigger_id=2, trigger=t_live, key=key)

        # Live subscriber consumes events 1 and 2; the dead subscriber never consumes.
        async with _consume_in_background(s_live) as collector:
            await collector.wait_for(2)
            await asyncio.sleep(0)
            assert _GatedTrigger.advanced == [], "events 1-2 still wait on the dead subscriber"

            # Event 3 overflows the dead subscriber's queue → force-fail clears it
            # from ALL outstanding entries, so events 1-2 advance without waiting
            # for their ack timeouts.
            gate.set()
            deadline = asyncio.get_event_loop().time() + 1.0
            while asyncio.get_event_loop().time() < deadline:
                if len(_GatedTrigger.advanced) >= 2:
                    break
                await asyncio.sleep(0)
            assert _GatedTrigger.advanced == ["p1", "p2"]
            assert _GatedTrigger.outcomes == [AdvanceOutcome(acked=1, failed=1)] * 2

            # The live subscriber consumes event 3 and it advances too.
            await collector.wait_for(3)
            await asyncio.sleep(0)
            assert _GatedTrigger.advanced == ["p1", "p2", "p3"]
            assert _GatedTrigger.outcomes[2] == AdvanceOutcome(acked=1, failed=1)
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_producer_factory_failure_is_terminal():
    """create_shared_stream_producer raising fails subscribers and evicts the group."""

    class _FactoryBoomTrigger(_ProgrammableSharedStreamTrigger):
        @classmethod
        def create_shared_stream_producer(cls, kwargs):
            raise RuntimeError("factory boom")

        async def filter_shared_stream(self, shared_stream):
            async for raw in shared_stream:  # pragma: no cover
                yield TriggerEvent(raw)

    t1 = _FactoryBoomTrigger()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()

    stream = manager.subscribe(trigger_id=1, trigger=t1, key=key)

    async def consume():
        async for _ in stream:
            pass

    with pytest.raises(RuntimeError, match="factory boom"):
        await asyncio.wait_for(consume(), timeout=1.0)

    assert key not in manager._groups
    await manager.unsubscribe(1, key)


@pytest.mark.asyncio
async def test_stop_drains_resolved_prefix_and_abandons_unresolved():
    """stop() waits for the resolved prefix's advances; unresolved events are abandoned."""
    advance_started = asyncio.Event()
    advance_release = asyncio.Event()
    advanced: list = []
    aclose_calls: list[int] = []

    class _PrefixProducer(SharedStreamProducer):
        async def open_stream(self):
            yield {"n": 1}, "p1"
            yield {"n": 2}, "p2"
            await asyncio.Event().wait()

        async def advance(self, batch):
            advance_started.set()
            await advance_release.wait()
            advanced.extend(item.broker_payload for item in batch)

        async def aclose(self):
            aclose_calls.append(1)

    class _PrefixTrigger(_ProgrammableSharedStreamTrigger):
        @classmethod
        def create_shared_stream_producer(cls, kwargs):
            return _PrefixProducer()

        async def filter_shared_stream(self, shared_stream):
            async for raw in shared_stream:
                yield TriggerEvent(raw)

    t1 = _PrefixTrigger()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()

    s1 = manager.subscribe(trigger_id=1, trigger=t1, key=key)

    # Move past event 1 (resolving it) onto event 2, which stays unresolved
    # because the subscriber never resumes past its yield.
    it = s1.__aiter__()
    await _pull_raw(it)
    pull2 = await _resume_past_yield(it)
    await asyncio.wait_for(advance_started.wait(), timeout=1.0)
    await asyncio.wait_for(pull2, timeout=1.0)

    # stop must wait for the in-flight advance of the resolved event...
    stop_task = asyncio.create_task(manager.stop_all())
    await asyncio.sleep(0)
    assert not stop_task.done(), "stop must wait for the in-flight advance"

    advance_release.set()
    await asyncio.wait_for(stop_task, timeout=1.0)

    # ...and must not advance the unresolved event 2 (left to broker redelivery).
    assert advanced == ["p1"]
    assert sum(aclose_calls) == 1, "the producer must still be closed after the drain"


async def _pull_raw(stream_iter):
    """Pull one raw event off an ack-mode stream iterator."""
    return await asyncio.wait_for(stream_iter.__anext__(), timeout=1.0)


async def _resume_past_yield(stream_iter) -> asyncio.Task:
    """
    Resume the drain generator past its yield and leave it waiting.

    Resuming closes the previous event's binding window — the exact moment a
    production filter loops back to pull the next raw event. The returned
    task is still waiting on the queue; the test must cancel it (or expect
    its failure) during cleanup.
    """
    task = asyncio.create_task(stream_iter.__anext__())
    await asyncio.sleep(0)
    return task


async def _cancel_quietly(task: asyncio.Task | None) -> None:
    """Cancel a pending stream pull, swallowing its cancellation or failure."""
    if task is None:
        return
    task.cancel()
    with suppress(asyncio.CancelledError, AckTimeout):
        await task


@pytest.mark.asyncio
async def test_multi_event_binding_gates_advance_on_every_seq():
    """Two trigger events bound to one raw event both need confirmation before advance."""
    cls = _make_ack_required_trigger_class([({"n": 1}, "p1")])
    cls.advanced.clear()
    cls.outcomes.clear()

    t1 = cls()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()
    next_task = None
    try:
        stream = manager.subscribe(trigger_id=1, trigger=t1, key=key)
        it = stream.__aiter__()
        await _pull_raw(it)

        # The subscriber derives two trigger events from the same raw event.
        seq1 = manager.bind_pending_event(trigger_id=1, key=key)
        seq2 = manager.bind_pending_event(trigger_id=1, key=key)
        assert seq1 is not None
        assert seq2 is not None
        assert seq1 != seq2

        next_task = await _resume_past_yield(it)
        assert cls.advanced == [], "advance must wait for both persist confirmations"

        manager.confirm_persisted([seq1])
        await asyncio.sleep(0)
        assert cls.advanced == [], "one of two confirmations is not enough"

        manager.confirm_persisted([seq2])
        await asyncio.sleep(0)
        assert cls.advanced == ["p1"]
        assert cls.outcomes == [AdvanceOutcome(acked=1, failed=0)]
    finally:
        await _cancel_quietly(next_task)
        await manager.unsubscribe(1, key)


@pytest.mark.asyncio
async def test_filtered_out_event_resolves_on_next_pull_without_confirmation():
    """An event with no bound trigger events resolves as soon as the filter pulls the next raw event."""
    cls = _make_ack_required_trigger_class([({"n": 1}, "p1")])
    cls.advanced.clear()
    cls.outcomes.clear()

    t1 = cls()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()
    next_task = None
    try:
        stream = manager.subscribe(trigger_id=1, trigger=t1, key=key)
        it = stream.__aiter__()
        await _pull_raw(it)

        # Filtered out: derive nothing, loop back for the next raw event.
        next_task = await _resume_past_yield(it)
        await asyncio.sleep(0)

        assert cls.advanced == ["p1"], "a filtered-out event must resolve without any confirmation"
        assert cls.outcomes == [AdvanceOutcome(acked=1, failed=0)]
    finally:
        await _cancel_quietly(next_task)
        await manager.unsubscribe(1, key)


@pytest.mark.asyncio
async def test_unsubscribe_with_unconfirmed_seq_waits_for_confirmation():
    """Guard: a subscriber leaving right after producing an event must not bypass the persist gate."""
    cls = _make_ack_required_trigger_class([({"n": 1}, "p1")])
    cls.advanced.clear()
    cls.outcomes.clear()

    t1, t2 = cls(), cls()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()
    next_task = None
    try:
        s1 = manager.subscribe(trigger_id=1, trigger=t1, key=key)
        s2 = manager.subscribe(trigger_id=2, trigger=t2, key=key)
        it1 = s1.__aiter__()
        it2 = s2.__aiter__()

        # Subscriber 1 fires (binds a trigger event) and immediately leaves.
        await _pull_raw(it1)
        seq = manager.bind_pending_event(trigger_id=1, key=key)
        assert seq is not None
        await manager.unsubscribe(1, key)

        # Subscriber 2 resolves cleanly.
        await _pull_raw(it2)
        next_task = await _resume_past_yield(it2)
        await asyncio.sleep(0)

        assert cls.advanced == [], "the departed subscriber's unconfirmed event must hold the advance"
        assert len(manager._groups[key]._outstanding) == 1

        manager.confirm_persisted([seq])
        await asyncio.sleep(0)
        assert cls.advanced == ["p1"]
        # The departed subscriber still counts as acked (implicit resolution).
        assert cls.outcomes == [AdvanceOutcome(acked=2, failed=0)]
    finally:
        await _cancel_quietly(next_task)
        await manager.unsubscribe(2, key)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "clock_value",
        "advanced_before_confirm",
        "outcomes_before_confirm",
        "outstanding_before_confirm",
        "outcomes_after_confirm",
    ),
    [
        pytest.param(
            0.099,
            [],
            [],
            1,
            [AdvanceOutcome(acked=1, failed=0)],
            id="below-timeout-confirm-recovers",
        ),
        pytest.param(
            0.2,
            ["p1"],
            [AdvanceOutcome(acked=0, failed=1)],
            0,
            [AdvanceOutcome(acked=0, failed=1)],
            id="late-confirm-after-timeout-is-noop",
        ),
    ],
)
async def test_unconfirmed_persist_confirm_timing_around_timeout(
    clock_value,
    advanced_before_confirm,
    outcomes_before_confirm,
    outstanding_before_confirm,
    outcomes_after_confirm,
):
    """Confirmation timing against a 100 ms ack timeout.

    Strictly inside the window the unconfirmed event stays outstanding and a
    confirmation recovers it cleanly; once the timeout has already failed the
    event, a late confirmation changes nothing.
    """
    cls = _make_ack_required_trigger_class([({"n": 1}, "p1")])
    cls.advanced.clear()
    cls.outcomes.clear()

    t1 = cls()
    key = t1.shared_stream_key()

    fake_clock: list[float] = [0.0]

    def now() -> float:
        return fake_clock[0]

    manager = SharedStreamManager(ack_timeout=0.1, _now=now)
    next_task = None
    try:
        stream = manager.subscribe(trigger_id=1, trigger=t1, key=key)
        it = stream.__aiter__()
        await _pull_raw(it)

        seq = manager.bind_pending_event(trigger_id=1, key=key)
        assert seq is not None
        next_task = await _resume_past_yield(it)

        fake_clock[0] = clock_value
        await asyncio.sleep(0.05)
        assert cls.advanced == advanced_before_confirm
        assert cls.outcomes == outcomes_before_confirm
        assert len(manager._groups[key]._outstanding) == outstanding_before_confirm

        manager.confirm_persisted([seq])
        await asyncio.sleep(0)
        assert cls.advanced == ["p1"]
        assert cls.outcomes == outcomes_after_confirm
    finally:
        await _cancel_quietly(next_task)
        await manager.stop_all()


@pytest.mark.asyncio
async def test_unconfirmed_persist_at_timeout_fails_subscriber():
    """A confirmation that never arrives fails the event at the ack timeout (elapsed >= timeout)."""
    cls = _make_ack_required_trigger_class([({"n": 1}, "p1")])
    cls.advanced.clear()
    cls.outcomes.clear()

    t1 = cls()
    key = t1.shared_stream_key()

    fake_clock: list[float] = [0.0]

    def now() -> float:
        return fake_clock[0]

    manager = SharedStreamManager(ack_timeout=0.1, _now=now)
    next_task = None
    try:
        stream = manager.subscribe(trigger_id=1, trigger=t1, key=key)
        it = stream.__aiter__()
        await _pull_raw(it)

        seq = manager.bind_pending_event(trigger_id=1, key=key)
        assert seq is not None
        next_task = await _resume_past_yield(it)

        # Exactly at the timeout: the unconfirmed event is force-failed.
        fake_clock[0] = 0.1
        await asyncio.sleep(0.05)
        assert cls.advanced == ["p1"]
        assert cls.outcomes == [AdvanceOutcome(acked=0, failed=1)]

        # The still-online subscriber sees the AckTimeout; the confirmation
        # for the bound sequence number never arrives.
        with pytest.raises(AckTimeout):
            await asyncio.wait_for(next_task, timeout=1.0)
        next_task = None
    finally:
        await _cancel_quietly(next_task)
        await manager.stop_all()


@pytest.mark.asyncio
async def test_group_stop_abandons_unconfirmed_advance():
    """The last subscriber leaving with an unconfirmed event stops the group without advancing."""
    cls = _make_ack_required_trigger_class([({"n": 1}, "p1")])
    cls.advanced.clear()
    cls.outcomes.clear()

    t1 = cls()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()

    stream = manager.subscribe(trigger_id=1, trigger=t1, key=key)
    it = stream.__aiter__()
    await _pull_raw(it)

    seq = manager.bind_pending_event(trigger_id=1, key=key)
    assert seq is not None

    # Last subscriber leaves while the confirmation is still outstanding:
    # the group stops and the advance is abandoned — the broker redelivers.
    await manager.unsubscribe(1, key)
    assert manager._groups == {}
    assert cls.advanced == [], "an unconfirmed advance must be abandoned at group stop"
    assert cls.aclose_calls == 1


@pytest.mark.asyncio
async def test_pump_batches_contiguous_resolved_prefix():
    """Events that resolve together are dispatched as one batch, in fan-out order."""
    cls = _make_ack_required_trigger_class([({"n": 1}, "p1"), ({"n": 2}, "p2"), ({"n": 3}, "p3")])

    t1 = cls()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()
    next_task = None
    try:
        stream = manager.subscribe(trigger_id=1, trigger=t1, key=key)

        # Consume all three events, binding one persist seq to each so
        # nothing resolves until the confirmations land.
        it = stream.__aiter__()
        seqs: list[int] = []
        for _ in range(3):
            await _pull_raw(it)
            seq = manager.bind_pending_event(trigger_id=1, key=key)
            assert seq is not None
            seqs.append(seq)
        next_task = await _resume_past_yield(it)
        for _ in range(5):
            await asyncio.sleep(0)
        assert cls.batches == [], "nothing may advance while every event is unconfirmed"

        # All three confirmations land in one synchronous call; the pump sees
        # the whole lane resolved at once and dispatches a single batch.
        manager.confirm_persisted(seqs)
        for _ in range(5):
            await asyncio.sleep(0)
        assert cls.batches == [["p1", "p2", "p3"]], (
            "one call must carry the lane's resolved prefix in fan-out order"
        )
    finally:
        await _cancel_quietly(next_task)
        await manager.unsubscribe(1, key)


@pytest.mark.asyncio
async def test_pump_batches_per_lane_independently():
    """A batch never crosses a lane boundary; each lane's resolved prefix is its own call."""
    cls = _make_ack_required_trigger_class(
        [
            ({"n": "a1"}, "a1"),
            ({"n": "b1"}, "b1"),
            ({"n": "a2"}, "a2"),
            ({"n": "b2"}, "b2"),
        ],
        lane_for=lambda payload: payload[0],
    )

    t1 = cls()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()
    next_task = None
    try:
        stream = manager.subscribe(trigger_id=1, trigger=t1, key=key)

        it = stream.__aiter__()
        seqs: list[int] = []
        for _ in range(4):
            await _pull_raw(it)
            seq = manager.bind_pending_event(trigger_id=1, key=key)
            assert seq is not None
            seqs.append(seq)
        next_task = await _resume_past_yield(it)
        for _ in range(5):
            await asyncio.sleep(0)
        assert cls.batches == []

        # Everything resolves at once, but the batches stay per lane.
        manager.confirm_persisted(seqs)
        for _ in range(5):
            await asyncio.sleep(0)
        assert sorted(cls.batches) == [["a1", "a2"], ["b1", "b2"]], (
            "each lane's resolved prefix must be dispatched as its own batch"
        )
    finally:
        await _cancel_quietly(next_task)
        await manager.unsubscribe(1, key)


@pytest.mark.asyncio
async def test_partial_prefix_waits_for_lane_head():
    """Resolved events behind an unresolved head wait, then ship in one batch with it."""
    cls = _make_ack_required_trigger_class([({"n": 1}, "p1"), ({"n": 2}, "p2"), ({"n": 3}, "p3")])

    t1 = cls()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()
    next_task = None
    try:
        stream = manager.subscribe(trigger_id=1, trigger=t1, key=key)

        it = stream.__aiter__()
        seqs: list[int] = []
        for _ in range(3):
            await _pull_raw(it)
            seq = manager.bind_pending_event(trigger_id=1, key=key)
            assert seq is not None
            seqs.append(seq)
        next_task = await _resume_past_yield(it)

        # Events 2 and 3 resolve out of order; the lane head is still pending.
        manager.confirm_persisted([seqs[1], seqs[2]])
        for _ in range(5):
            await asyncio.sleep(0)
        assert cls.batches == [], "no batch may ship while the lane head is unresolved"

        # The head resolves; the whole prefix ships as one batch in fan-out order.
        manager.confirm_persisted([seqs[0]])
        for _ in range(5):
            await asyncio.sleep(0)
        assert cls.batches == [["p1", "p2", "p3"]]
    finally:
        await _cancel_quietly(next_task)
        await manager.unsubscribe(1, key)


@pytest.mark.asyncio
async def test_advance_raise_abandons_whole_batch():
    """A batch whose advance raises is logged and abandoned; later events form fresh batches."""
    calls: list[list] = []
    proceed = asyncio.Event()

    class _RaiseOnFirstBatchProducer(SharedStreamProducer):
        async def open_stream(self):
            yield {"n": 1}, "p1"
            yield {"n": 2}, "p2"
            await proceed.wait()
            yield {"n": 3}, "p3"
            await asyncio.Event().wait()

        async def advance(self, batch):
            calls.append([item.broker_payload for item in batch])
            if len(calls) == 1:
                raise RuntimeError("batch boom")

    class _RaiseOnFirstBatchTrigger(_ProgrammableSharedStreamTrigger):
        @classmethod
        def create_shared_stream_producer(cls, kwargs):
            return _RaiseOnFirstBatchProducer()

    t1 = _RaiseOnFirstBatchTrigger()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()
    next_task = None
    try:
        s1 = manager.subscribe(trigger_id=1, trigger=t1, key=key)
        group = manager._groups[key]
        group.log = MagicMock()

        # Resolve events 1 and 2 together so they form one two-event batch.
        it = s1.__aiter__()
        seqs: list[int] = []
        for _ in range(2):
            await _pull_raw(it)
            seq = manager.bind_pending_event(trigger_id=1, key=key)
            assert seq is not None
            seqs.append(seq)
        next_task = await _resume_past_yield(it)
        manager.confirm_persisted(seqs)
        for _ in range(5):
            await asyncio.sleep(0)

        # The batch raised: it is logged, already off the books, and never
        # dispatched again — the broker is left to redeliver.
        assert calls == [["p1", "p2"]]
        assert group._outstanding == {}
        assert len(group.log.error.mock_calls) == 1

        # The pump keeps running; a later event forms a fresh batch.
        proceed.set()
        await asyncio.wait_for(next_task, timeout=1.0)
        next_task = await _resume_past_yield(it)
        for _ in range(5):
            await asyncio.sleep(0)
        assert calls == [["p1", "p2"], ["p3"]]
    finally:
        await _cancel_quietly(next_task)
        await manager.unsubscribe(1, key)


@pytest.mark.asyncio
async def test_trickle_resolution_yields_singleton_batches():
    """Events that resolve one at a time degenerate to singleton batches (the old behavior)."""
    cls = _make_ack_required_trigger_class([({"n": 1}, "p1"), ({"n": 2}, "p2"), ({"n": 3}, "p3")])

    t1 = cls()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()
    next_task = None
    try:
        stream = manager.subscribe(trigger_id=1, trigger=t1, key=key)

        it = stream.__aiter__()
        seqs: list[int] = []
        for _ in range(3):
            await _pull_raw(it)
            seq = manager.bind_pending_event(trigger_id=1, key=key)
            assert seq is not None
            seqs.append(seq)
        next_task = await _resume_past_yield(it)

        # Resolve one event per pump pass.
        for seq in seqs:
            manager.confirm_persisted([seq])
            for _ in range(5):
                await asyncio.sleep(0)

        assert cls.batches == [["p1"], ["p2"], ["p3"]], (
            "trickled resolutions must each ship as their own batch"
        )
    finally:
        await _cancel_quietly(next_task)
        await manager.unsubscribe(1, key)
