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
from contextlib import suppress

import pytest

from airflow.triggers.base import BaseEventTrigger, TriggerEvent
from airflow.triggers.shared_stream import (
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
