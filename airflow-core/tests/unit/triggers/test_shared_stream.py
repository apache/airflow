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
import weakref
from contextlib import suppress

import pytest

from airflow.triggers.base import BaseEventTrigger, TriggerEvent
from airflow.triggers.shared_stream import (
    AckTimeout,
    AckToken,
    SharedStreamManager,
    _PollFailure,
    _SharedStreamGroup,
    _SubscriberOverflow,
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
    assert 1 in manager._groups[key]._overflowed

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
    import structlog

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
    assert trigger_id in group._overflowed, "trigger_id must be recorded in _overflowed"


# ---------------------------------------------------------------------------
# Ack-mode fixtures and helpers
# ---------------------------------------------------------------------------


def _make_ack_required_trigger_class(events_with_payloads: list[tuple]):
    """
    Return a fresh trigger class whose ``open_shared_stream`` yields
    ``(event, broker_payload)`` tuples and whose ``advance_shared_stream``
    records each advance call into a class-level list.
    """

    class _AckRequiredTrigger(_ProgrammableSharedStreamTrigger):
        advanced: list[tuple] = []

        @classmethod
        async def open_shared_stream(cls, kwargs):
            for event, broker_payload in events_with_payloads:
                yield event, broker_payload
            await asyncio.Event().wait()

        @classmethod
        async def advance_shared_stream(cls, kwargs, broker_payload):
            cls.advanced.append(broker_payload)

        async def filter_shared_stream(self, shared_stream):
            async for raw, token in shared_stream:
                await token.ack()
                yield TriggerEvent(raw)

    return _AckRequiredTrigger


async def _collect_ack_stream(trigger, stream, *, n: int, timeout: float = 1.0) -> list:
    """Collect ``n`` TriggerEvent items from an ack-aware filter_shared_stream."""
    out = []
    async for event in trigger.filter_shared_stream(stream):
        out.append(event)
        if len(out) >= n:
            break
    return out


# ---------------------------------------------------------------------------
# Test 1
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ack_required_producer_advances_after_all_subscribers_ack():
    """Producer's advance hook fires exactly once after both subscribers ack."""
    cls = _make_ack_required_trigger_class(
        [
            ({"msg": "hello"}, "receipt-1"),
        ]
    )
    cls.advanced.clear()

    t1, t2 = cls(), cls()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()
    try:
        s1 = manager.subscribe(trigger_id=1, trigger=t1, key=key)
        s2 = manager.subscribe(trigger_id=2, trigger=t2, key=key)

        tokens: list[AckToken] = []

        async def collect_token(stream, trigger):
            async for _raw, token in stream:
                tokens.append(token)
                break

        # Collect the (event, token) tuples without acking yet.
        await asyncio.gather(
            asyncio.wait_for(collect_token(s1, t1), timeout=1.0),
            asyncio.wait_for(collect_token(s2, t2), timeout=1.0),
        )

        assert len(cls.advanced) == 0, "advance must not fire before all acks"

        await tokens[0].ack()
        assert len(cls.advanced) == 0, "advance must not fire after only one ack"

        await tokens[1].ack()
        await asyncio.sleep(0)  # let the scheduled advance task run
        assert cls.advanced == ["receipt-1"], "advance must fire exactly once after all acks"
    finally:
        await manager.unsubscribe(1, key)
        await manager.unsubscribe(2, key)


# ---------------------------------------------------------------------------
# Test 2
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ack_required_producer_does_not_advance_on_partial_ack():
    """Producer does not advance when only one of two subscribers has acknowledged."""
    cls = _make_ack_required_trigger_class(
        [
            ({"msg": "partial"}, "receipt-partial"),
        ]
    )
    cls.advanced.clear()

    t1, t2 = cls(), cls()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()
    try:
        s1 = manager.subscribe(trigger_id=1, trigger=t1, key=key)
        s2 = manager.subscribe(trigger_id=2, trigger=t2, key=key)

        token1 = None
        got_s2 = asyncio.Event()

        async def grab_token1(stream):
            nonlocal token1
            async for _raw, token in stream:
                token1 = token
                break

        async def drain_s2(stream):
            async for _ in stream:  # pragma: no branch
                got_s2.set()
                break

        await asyncio.gather(
            asyncio.wait_for(grab_token1(s1), timeout=1.0),
            asyncio.wait_for(drain_s2(s2), timeout=1.0),
        )

        await token1.ack()
        await asyncio.sleep(0)
        assert cls.advanced == [], "producer must not advance with only one of two acks"
    finally:
        await manager.unsubscribe(1, key)
        await manager.unsubscribe(2, key)


# ---------------------------------------------------------------------------
# Test 3
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ack_timeout_force_fails_slow_subscriber_only():
    """A slow subscriber is force-failed after timeout; the fast subscriber and advance are unaffected."""
    cls = _make_ack_required_trigger_class(
        [
            ({"msg": "timeout-test"}, "receipt-timeout"),
        ]
    )
    cls.advanced.clear()

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

        token_fast = None

        async def grab_fast(stream):
            nonlocal token_fast
            async for _raw, token in stream:
                token_fast = token
                break

        async def drain_slow(stream):
            # Collect from slow stream — we expect AckTimeout to appear.
            async for _ in stream:
                break

        await asyncio.gather(
            asyncio.wait_for(grab_fast(s_fast), timeout=1.0),
            asyncio.wait_for(drain_slow(s_slow), timeout=1.0),
        )

        # fast acks; slow doesn't.
        await token_fast.ack()

        # Advance fake clock past ack_timeout; the timeout loop sees now() - created_at >= 0.1.
        fake_clock[0] = 0.2
        slow_queue = manager._groups[key]._subscribers.get(2)
        # Let _run_ack_timeout_loop tick through a few cadence cycles (cadence=0.01s real).
        await asyncio.sleep(0.05)

        assert slow_queue is not None
        sentinel = slow_queue.get_nowait()
        assert isinstance(sentinel, _PollFailure)
        assert isinstance(sentinel.exc, AckTimeout)

        # After timeout the advance should have fired (pending emptied).
        await asyncio.sleep(0)
        assert cls.advanced == ["receipt-timeout"]
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_ack_timeout_at_cap_boundary_does_not_timeout():
    """Subscriber acking inside the window (< ack_timeout) must NOT be force-failed."""
    cls = _make_ack_required_trigger_class(
        [
            ({"msg": "boundary-test"}, "receipt-boundary"),
        ]
    )
    cls.advanced.clear()

    t1 = cls()
    key = t1.shared_stream_key()

    # Inject a fake clock: created_at will be recorded at fake_clock[0] = 0.0.
    fake_clock: list[float] = [0.0]

    def now() -> float:
        return fake_clock[0]

    manager = SharedStreamManager(ack_timeout=0.1, _now=now)
    try:
        stream = manager.subscribe(trigger_id=1, trigger=t1, key=key)

        token_ref = None

        async def grab_token(s):
            nonlocal token_ref
            async for _raw, token in s:
                token_ref = token
                break

        await asyncio.wait_for(grab_token(stream), timeout=1.0)

        # Advance fake clock to 99 ms — strictly inside the 100 ms ack_timeout window.
        # now() - created_at = 0.099 < 0.1, so the timeout loop must NOT fire.
        fake_clock[0] = 0.099
        # Let the timeout loop tick a few cadence cycles (cadence = max(0.01, 0.1/10) = 0.01s real).
        await asyncio.sleep(0.05)

        subscriber_queue = manager._groups[key]._subscribers.get(1)
        assert subscriber_queue is not None
        assert subscriber_queue.empty(), "subscriber must not have been force-failed at t=99ms"

        # Now ack — advance should fire.
        await token_ref.ack()
        await asyncio.sleep(0)
        assert cls.advanced == ["receipt-boundary"]
    finally:
        await manager.stop_all()


# ---------------------------------------------------------------------------
# Test 4
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_late_subscriber_does_not_block_advance_of_earlier_event():
    """A subscriber joining after event N is broadcast is not in event N's ack set."""
    event_broadcast = asyncio.Event()
    proceed = asyncio.Event()

    class _LateTrigger(_ProgrammableSharedStreamTrigger):
        advanced: list = []

        @classmethod
        async def open_shared_stream(cls, kwargs):
            event_broadcast.set()
            await proceed.wait()
            yield {"msg": "late-test"}, "receipt-late"
            await asyncio.Event().wait()

        @classmethod
        async def advance_shared_stream(cls, kwargs, broker_payload):
            cls.advanced.append(broker_payload)

        async def filter_shared_stream(self, shared_stream):
            async for raw, token in shared_stream:
                await token.ack()
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
        # (event, token) tuple into the early subscriber's queue.
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

        # Collect from early subscriber (filter acks immediately).
        events = await asyncio.wait_for(_collect_ack_stream(t_early, s_early, n=1), timeout=1.0)
        assert len(events) == 1

        await asyncio.sleep(0)
        # The late subscriber did not participate in ack set; advance must have fired.
        assert _LateTrigger.advanced == ["receipt-late"]
    finally:
        await manager.unsubscribe(1, key)
        await manager.unsubscribe(2, key)


# ---------------------------------------------------------------------------
# Test 5
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_nack_removes_subscriber_from_ack_set_without_redeliver():
    """nack() removes the subscriber from the pending set; no redeliver happens."""
    cls = _make_ack_required_trigger_class(
        [
            ({"msg": "nack-test"}, "receipt-nack"),
        ]
    )
    cls.advanced.clear()

    t1, t2 = cls(), cls()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()
    try:
        s1 = manager.subscribe(trigger_id=1, trigger=t1, key=key)
        s2 = manager.subscribe(trigger_id=2, trigger=t2, key=key)

        tokens: list[AckToken] = []

        async def grab_token(stream):
            async for _raw, token in stream:
                tokens.append(token)
                break

        await asyncio.gather(
            asyncio.wait_for(grab_token(s1), timeout=1.0),
            asyncio.wait_for(grab_token(s2), timeout=1.0),
        )

        # t1 acks, t2 nacks.
        await tokens[0].ack()
        await tokens[1].nack()
        await asyncio.sleep(0)

        assert cls.advanced == ["receipt-nack"], "advance must fire after ack + nack"

        # No redelivery — the manager has no redeliver mechanism.
        group = manager._groups.get(key)
        if group:
            assert len(group._outstanding) == 0
    finally:
        await manager.unsubscribe(1, key)
        await manager.unsubscribe(2, key)


# ---------------------------------------------------------------------------
# Test 6
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_double_ack_is_noop():
    """Calling ack() twice on the same token must not raise or advance twice."""
    cls = _make_ack_required_trigger_class(
        [
            ({"msg": "double-ack"}, "receipt-double"),
        ]
    )
    cls.advanced.clear()

    t1 = cls()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()
    try:
        s1 = manager.subscribe(trigger_id=1, trigger=t1, key=key)

        token = None

        async def grab_token(stream):
            nonlocal token
            async for _raw, tk in stream:
                token = tk
                break

        await asyncio.wait_for(grab_token(s1), timeout=1.0)
        assert token is not None

        await token.ack()
        await asyncio.sleep(0)
        assert cls.advanced == ["receipt-double"]

        # Second ack — must be a no-op, not raise, not double-advance.
        await token.ack()
        await asyncio.sleep(0)
        assert cls.advanced == ["receipt-double"], "second ack must not trigger a second advance"
    finally:
        await manager.unsubscribe(1, key)


# ---------------------------------------------------------------------------
# Test 7
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_subscriber_unsubscribe_during_outstanding_ack():
    """When a subscriber leaves while its ack is pending, the group advances without it."""
    cls = _make_ack_required_trigger_class(
        [
            ({"msg": "unsub-during-ack"}, "receipt-unsub"),
        ]
    )
    cls.advanced.clear()

    t1, t2 = cls(), cls()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()
    try:
        s1 = manager.subscribe(trigger_id=1, trigger=t1, key=key)
        s2 = manager.subscribe(trigger_id=2, trigger=t2, key=key)

        tokens: list[AckToken] = []

        async def grab_token(stream):
            async for _raw, token in stream:
                tokens.append(token)
                break

        await asyncio.gather(
            asyncio.wait_for(grab_token(s1), timeout=1.0),
            asyncio.wait_for(grab_token(s2), timeout=1.0),
        )

        assert len(tokens) == 2
        # t1 acks; t2 leaves without acking (implicit ack-out via unsubscribe).
        await tokens[0].ack()
        await manager.unsubscribe(2, key)
        await asyncio.sleep(0)

        assert cls.advanced == ["receipt-unsub"], (
            "unsubscribe must trigger implicit ack-out so producer can advance"
        )
    finally:
        await manager.unsubscribe(1, key)


# ---------------------------------------------------------------------------
# Test — regression: QueueFull during fan-out must not break iteration of remaining subscribers
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ack_mode_queue_full_during_fanout_does_not_break_iteration():
    """A full queue at fan-out time must not break the iteration of remaining subscribers."""
    cls = _make_ack_required_trigger_class([({"msg": "burst"}, "receipt-burst")])
    cls.advanced.clear()

    t_full, t_ok = cls(), cls()
    key = t_full.shared_stream_key()
    # Queue of size 1 — pre-filling it will force QueueFull on fan-out for trigger 1.
    manager = SharedStreamManager(max_subscriber_queue=1)
    try:
        s_full = manager.subscribe(trigger_id=1, trigger=t_full, key=key)
        s_ok = manager.subscribe(trigger_id=2, trigger=t_ok, key=key)

        # Pre-fill trigger 1's queue so put_nowait raises QueueFull at fan-out.
        group = manager._groups[key]
        group._subscribers[1].put_nowait(("filler", AckToken(-1, 1, weakref.ref(group))))

        # t_ok: drive its ack stream and collect the real event.
        ok_result: list = []

        async def collect_ok():
            events = await _collect_ack_stream(t_ok, s_ok, n=1)
            ok_result.extend(events)

        # t_full: its queue will be drained + replaced by _PollFailure(_SubscriberOverflow).
        # _drain() raises the inner exception, so we expect _SubscriberOverflow here.
        full_exc: list[BaseException] = []

        async def collect_full():
            try:
                async for _ in s_full:
                    pass
            except Exception as exc:
                full_exc.append(exc)

        await asyncio.gather(
            asyncio.wait_for(collect_full(), timeout=2.0),
            asyncio.wait_for(collect_ok(), timeout=2.0),
        )

        # Give _schedule_advance a tick to run.
        await asyncio.sleep(0)

        # t_full got an overflow failure (not a RuntimeError from set mutation).
        assert len(full_exc) == 1
        assert isinstance(full_exc[0], _SubscriberOverflow)

        # t_ok got the real event and acknowledged it → advance was triggered.
        assert len(ok_result) == 1
        assert ok_result[0].payload == {"msg": "burst"}
        assert cls.advanced == ["receipt-burst"]
    finally:
        await manager.stop_all()


# ---------------------------------------------------------------------------
# Test — must-fix #1: no-subscriber snapshot uses _schedule_advance (fire-and-forget)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_subscriber_snapshot_advances_immediately():
    """When no subscribers are online at broadcast time, advance hook is still called."""
    cls = _make_ack_required_trigger_class(
        [
            ({"msg": "no-sub"}, "receipt-no-sub"),
        ]
    )
    cls.advanced.clear()

    t1 = cls()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()

    # Subscribe then immediately unsubscribe so the group exists but has zero subscribers
    # when the event arrives.
    s1 = manager.subscribe(trigger_id=1, trigger=t1, key=key)
    manager._groups[key].unsubscribe(1)  # remove without stopping the poll task

    # The poll task is still running; give it a few ticks to broadcast.
    deadline = asyncio.get_event_loop().time() + 1.0
    while asyncio.get_event_loop().time() < deadline:
        if cls.advanced:
            break
        await asyncio.sleep(0.01)

    assert cls.advanced == ["receipt-no-sub"], (
        "advance hook must be called even when no subscribers are present at broadcast time"
    )

    del s1  # silence unused-variable warning
    await manager.stop_all()


# ---------------------------------------------------------------------------
# Test — must-fix #3: advance_shared_stream exception is logged, poll survives
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_advance_shared_stream_exception_is_logged():
    """When advance_shared_stream raises, the error is logged and the poll group stays alive."""
    from unittest.mock import MagicMock

    proceed_flag = asyncio.Event()

    class _RaisingAdvanceTrigger(_ProgrammableSharedStreamTrigger):
        @classmethod
        async def open_shared_stream(cls, kwargs):
            yield ({"msg": "event-1"}, "payload-1")
            await proceed_flag.wait()
            yield ({"msg": "event-2"}, "payload-2")
            await asyncio.Event().wait()

        @classmethod
        async def advance_shared_stream(cls, kwargs, broker_payload):
            raise RuntimeError("simulated broker failure")

        async def filter_shared_stream(self, shared_stream):
            async for raw, token in shared_stream:
                await token.ack()
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

        # Consume the first event — this triggers an ack and schedules advance.
        events1 = await asyncio.wait_for(_collect_ack_stream(t1, s1, n=1), timeout=1.0)
        assert len(events1) == 1

        # Give the advance task a tick to run and raise.
        await asyncio.sleep(0.05)

        # log.error must have been called with a message about advance_shared_stream.
        assert mock_log_error.called, "log.error must be called when advance_shared_stream raises"
        call_args = mock_log_error.call_args
        assert "advance_shared_stream raised" in call_args[0][0], (
            f"log.error message must mention 'advance_shared_stream raised', got: {call_args[0][0]}"
        )

        # The poll group is still alive — the exception must not have killed it.
        assert key in manager._groups, "group must survive an advance_shared_stream exception"

        # A second event can still be produced and consumed.
        proceed_flag.set()
        events2 = await asyncio.wait_for(_collect_ack_stream(t1, s1, n=1), timeout=1.0)
        assert len(events2) == 1
    finally:
        await manager.stop_all()


# ---------------------------------------------------------------------------
# Test — must-fix #3 regression: stop() must await in-flight advance tasks
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stop_drains_in_flight_advance_tasks():
    """stop() must await advance hooks already in flight so graceful shutdown does not drop them."""
    advance_started = asyncio.Event()
    advance_may_finish = asyncio.Event()
    advance_done_at: list[float] = []

    class _SlowAdvanceTrigger(_ProgrammableSharedStreamTrigger):
        advanced: list = []

        @classmethod
        async def open_shared_stream(cls, kwargs):
            yield ({"msg": "slow"}, "receipt-slow")
            await asyncio.Event().wait()

        @classmethod
        async def advance_shared_stream(cls, kwargs, broker_payload):
            advance_started.set()
            await advance_may_finish.wait()
            await asyncio.sleep(0.05)  # ensure stop() must actually wait for us
            cls.advanced.append(broker_payload)
            advance_done_at.append(time.monotonic())

        async def filter_shared_stream(self, shared_stream):
            async for raw, token in shared_stream:
                await token.ack()
                yield TriggerEvent(raw)

    _SlowAdvanceTrigger.advanced.clear()

    t1 = _SlowAdvanceTrigger()
    key = t1.shared_stream_key()
    manager = SharedStreamManager()

    s1 = manager.subscribe(trigger_id=1, trigger=t1, key=key)

    # Drive the one event through so the advance task is scheduled.
    events = await asyncio.wait_for(_collect_ack_stream(t1, s1, n=1), timeout=1.0)
    assert len(events) == 1

    # Wait for advance_shared_stream to actually start (confirms the task is in-flight).
    await asyncio.wait_for(advance_started.wait(), timeout=1.0)

    # Unblock the advance hook and stop; advance still has a 50 ms sleep so stop()
    # must genuinely await the task rather than racing past it.
    advance_may_finish.set()
    await manager.stop_all()
    stop_returned_at = time.monotonic()

    # stop_all() must have waited for the advance task to complete.
    assert _SlowAdvanceTrigger.advanced == ["receipt-slow"], (
        "stop() must drain in-flight advance tasks before returning"
    )
    assert advance_done_at, "advance task must have completed"
    assert advance_done_at[0] <= stop_returned_at, (
        "stop() must wait for in-flight advance to finish before returning"
    )


# ---------------------------------------------------------------------------
# Test 8 — regression: fast path for triggers without advance_shared_stream
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_directory_file_delete_trigger_path_unchanged():
    """Triggers that do NOT override advance_shared_stream use the fast path.

    Subscribers receive raw events (not tuples) — identical to pre-ack-mode behavior.
    """
    cls = _make_trigger_class(_events_then_block([{"filename": "flag.txt"}]))
    # Confirm advance_shared_stream is NOT overridden (fast path).
    # We check that no class in the MRO before BaseEventTrigger defines it.
    assert "advance_shared_stream" not in cls.__dict__, (
        "advance_shared_stream must NOT be in the class's own __dict__ for fast-path detection"
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
