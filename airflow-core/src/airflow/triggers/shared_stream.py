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
Shared underlying I/O between :class:`BaseEventTrigger` instances in the triggerer.

When multiple triggers declare the same non-``None``
:meth:`~airflow.triggers.base.BaseEventTrigger.shared_stream_key`, the
triggerer routes them through :class:`SharedStreamManager` so that one
underlying poll loop produces raw events that are broadcast to every
participating trigger. Each trigger then runs
:meth:`~airflow.triggers.base.BaseEventTrigger.filter_shared_stream` to
convert the broadcast into its own :class:`~airflow.triggers.base.TriggerEvent`
instances. Triggers that opt out (the default) keep their independent
``run()``-based poll loops untouched.

Producer-side ack channel
-------------------------

When a trigger overrides
:meth:`~airflow.triggers.base.BaseEventTrigger.advance_shared_stream`,
the manager switches to **ack mode** for that stream.

``open_shared_stream`` must then yield ``(raw_event, broker_payload)``
tuples. The manager hands ``(raw_event, :class:`AckToken`)`` to each
subscriber's queue instead of the raw event alone. A subscriber calls
``await token.ack()`` once it has accepted the event, or
``await token.nack()`` to opt out without triggering broker-side redeliver.
When every subscriber that was online at broadcast time has called
``ack()`` (or ``nack()``, or timed out), the manager calls
``await cls.advance_shared_stream(kwargs, broker_payload)`` exactly once —
this is where the producer trigger commits / deletes / acks on the broker.

**Snapshot-at-fan-out**: the set of subscribers that must ack an event is
frozen at broadcast time. A subscriber that joins after the event was
broadcast is not added to that event's pending set.

**Per-event ack timeout**: a background task scans outstanding events.
Any subscriber that has not acknowledged within ``ack_timeout`` seconds is
force-failed via the existing :class:`_PollFailure` path (exception type
:class:`AckTimeout`). Other subscribers are not affected; once the
remaining acks arrive the producer advances normally.

**Triggerer restart**: outstanding acks are in-memory only. After a
triggerer restart, the broker will redeliver unacknowledged events.
Subscribers must therefore be idempotent.

**``shared_stream_subscriber_queue_size`` in ack mode**: the bound is
still "unprocessed raw events per subscriber". In ack mode the producer
waits for acks before yielding the next event, so the queue rarely
approaches the bound; it mainly protects against burst delivery before a
subscriber's filter has had a chance to run.

Triggers that do **not** override ``advance_shared_stream`` run the
**fast path**: no event IDs, no ack table, no AckToken — subscribers
receive raw events as before (backward-compatible).

Lifecycle invariants
--------------------

The manager and groups cooperate to keep a single invariant true at every
``await``-point:

    A key is present in :attr:`SharedStreamManager._groups` only while its
    group's poll task is alive and accepting new subscribers.

This rules out the late-subscriber races that the naive design admits — a
new subscriber for a key whose poll has died or is in the middle of being
torn down always falls through to "create a fresh group" rather than
attaching to a dead one and hanging on an empty queue. The invariant is
maintained synchronously:

* When ``_poll`` ends for any reason other than cancellation (the upstream
  iterator raised, or returned), the group's ``finally`` block evicts the
  key from ``_groups`` and broadcasts a terminal sentinel to current
  subscribers — all without yielding, so no other coroutine can interleave.
* When the last subscriber leaves, :meth:`SharedStreamManager.unsubscribe`
  evicts the key from ``_groups`` *before* awaiting ``group.stop()``, so a
  new subscriber arriving while we wait for cancellation creates a fresh
  group.
* :meth:`SharedStreamManager.stop_all` clears ``_groups`` in one synchronous
  step before awaiting any stop, applying the same rule to shutdown.
"""

from __future__ import annotations

import asyncio
import time
import weakref
from collections.abc import AsyncGenerator, AsyncIterator, Callable, Hashable
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import structlog

from airflow.triggers.base import BaseEventTrigger

if TYPE_CHECKING:
    from structlog.stdlib import BoundLogger

log = structlog.get_logger(__name__)

__all__ = ["AckToken", "AckTimeout", "SharedStreamManager"]

DEFAULT_SUBSCRIBER_QUEUE_MAX = 1024
"""Default per-subscriber queue size for shared streams.

The :class:`SharedStreamManager` admits up to this many unconsumed raw events
per subscriber before treating the subscriber as too slow to keep up — at
which point the subscriber's trigger is failed with
:class:`_SubscriberOverflow` rather than the queue growing without bound.

Used as the fallback when no value is passed to ``SharedStreamManager``;
in the triggerer this is overridden from the
``[triggerer] shared_stream_subscriber_queue_size`` config option.
"""

DEFAULT_ACK_TIMEOUT = 300.0
"""Default per-event ack timeout in seconds (5 minutes).

When ack mode is active, a subscriber that has not called ``token.ack()`` or
``token.nack()`` within this window is force-failed via :class:`AckTimeout`.
Override per-manager with ``SharedStreamManager(ack_timeout=...)``.
"""


class _PollTerminated(Exception):
    """
    Raised inside subscribers when ``open_shared_stream`` returns without yielding more events.

    Implementations are expected to run for the lifetime of the group; an
    early return would otherwise leave subscribers waiting forever on an
    empty queue.
    """


class _SubscriberOverflow(Exception):
    """
    Raised in a subscriber whose queue exceeded its maxsize.

    Surfaces the slow subscriber loudly through the standard trigger-failure
    path (rather than silently dropping events) so Airflow's retry / failure
    semantics apply. Other subscribers in the same group are unaffected.
    """


class _PollFailure:
    """Sentinel propagated through subscriber queues when the shared poll ends."""

    __slots__ = ("exc",)

    def __init__(self, exc: BaseException) -> None:
        self.exc = exc


class AckTimeout(Exception):
    """
    Raised in a subscriber whose ack did not arrive within the per-event timeout.

    Treated the same as :class:`_SubscriberOverflow` — the subscriber's trigger
    fails through the standard trigger-failure path. Other subscribers in the same
    group are unaffected; their acks still advance the producer normally.
    """


@dataclass
class _OutstandingEntry:
    """State for one outstanding (broadcast-but-not-yet-advanced) event."""

    pending: set[int]  # trigger_ids still awaiting ack
    created_at: float  # time.monotonic() at broadcast
    broker_payload: Any


class AckToken:
    """
    Handed to subscribers alongside each shared-stream event in ack mode.

    Call ``await token.ack()`` once the event has been accepted, or
    ``await token.nack()`` to opt out without triggering broker-side redeliver.
    Repeated calls are no-ops. After the group stops, calls silently do nothing.
    """

    __slots__ = ("_event_id", "_trigger_id", "_group_ref", "_resolved")

    def __init__(
        self,
        event_id: int,
        trigger_id: int,
        group_ref: weakref.ref[_SharedStreamGroup],
    ) -> None:
        self._event_id = event_id
        self._trigger_id = trigger_id
        self._group_ref = group_ref
        self._resolved = False

    async def ack(self) -> None:
        """
        Notify the producer that this subscriber has accepted the event.

        Declared ``async`` for forward-compatibility: a future broker-native
        implementation may need to ``await`` an I/O call here. The current
        in-process implementation is synchronous.
        """
        if self._resolved:
            return
        self._resolved = True
        group = self._group_ref()
        if group is not None:
            group._on_ack(self._event_id, self._trigger_id, nack=False)

    async def nack(self) -> None:
        """
        Opt out of this event without triggering broker-side redeliver.

        Declared ``async`` for forward-compatibility: a future broker-native
        implementation may need to ``await`` an I/O call here. The current
        in-process implementation is synchronous.
        """
        if self._resolved:
            return
        self._resolved = True
        group = self._group_ref()
        if group is not None:
            group._on_ack(self._event_id, self._trigger_id, nack=True)


async def _drain(queue: asyncio.Queue) -> AsyncGenerator[Any, None]:
    """
    Yield items from ``queue`` until a poll termination sentinel arrives.

    Subscribers exit either by their consuming task being cancelled
    (Airflow's standard idiom — :class:`CancelledError` propagates through
    ``queue.get()``) or by the shared poll ending, in which case the
    :class:`_PollFailure` sentinel re-raises here.
    """
    while True:
        item = await queue.get()
        if isinstance(item, _PollFailure):
            raise item.exc
        yield item


class _SharedStreamGroup:
    """One shared poll loop broadcasting raw events to N subscriber queues."""

    def __init__(
        self,
        *,
        key: Hashable,
        trigger_class: type[BaseEventTrigger],
        kwargs: dict[str, Any],
        on_poll_terminate: Callable[[_SharedStreamGroup], None],
        max_subscriber_queue: int,
        ack_timeout: float,
        log: BoundLogger,
        _now: Callable[[], float] = time.monotonic,
    ) -> None:
        self.key = key
        self.trigger_class = trigger_class
        self.kwargs = kwargs
        self.log = log
        self._on_poll_terminate = on_poll_terminate
        self._max_subscriber_queue = max_subscriber_queue
        self._ack_timeout = ack_timeout
        self._now = _now
        self._subscribers: dict[int, asyncio.Queue] = {}
        self._overflowed: set[int] = set()
        self._poll_task: asyncio.Task | None = None
        # Ack mode state — populated only when advance_shared_stream is overridden.
        self._outstanding: dict[int, _OutstandingEntry] = {}
        self._next_event_id: int = 0
        self._ack_timeout_task: asyncio.Task | None = None
        # Fire-and-forget advance tasks; kept in a set so GC doesn't collect them
        # before they finish (standard asyncio fire-and-forget pattern).
        self._advance_tasks: set[asyncio.Task] = set()

    def start(self) -> None:
        """Start the underlying poll loop. Call exactly once per group."""
        if self._poll_task is not None:
            raise RuntimeError(f"Shared stream group {self.key!r} already started")
        self._poll_task = asyncio.create_task(
            self._poll(),
            name=f"shared-stream-poll[{self.key!r}]",
        )

    def _on_advance_done(self, task: asyncio.Task) -> None:
        """Done callback for advance tasks: discard from set and log any exception."""
        self._advance_tasks.discard(task)
        if not task.cancelled() and (exc := task.exception()):
            self.log.error(
                "advance_shared_stream raised; broker advance failed",
                key=self.key,
                exc_info=exc,
            )

    def _schedule_advance(self, broker_payload: Any) -> None:
        """
        Fan out an advance call to the producer.

        Fire-and-forget: the resulting task's failure is logged via
        :meth:`_on_advance_done` and not re-raised. Subscribers must be
        idempotent.
        """
        task = asyncio.create_task(self.trigger_class.advance_shared_stream(self.kwargs, broker_payload))
        self._advance_tasks.add(task)
        task.add_done_callback(self._on_advance_done)

    def _is_ack_required(self) -> bool:
        # Check whether any class in the MRO (before BaseEventTrigger) defines
        # advance_shared_stream — i.e. the subclass has overridden it.
        for klass in self.trigger_class.__mro__:
            if klass is BaseEventTrigger:
                # Reached the base without finding an override — fast path.
                return False
            if "advance_shared_stream" in klass.__dict__:
                return True
        return False

    async def _poll(self) -> None:
        ack_required = self._is_ack_required()
        if ack_required:
            self._ack_timeout_task = asyncio.create_task(
                self._run_ack_timeout_loop(),
                name=f"shared-stream-ack-timeout[{self.key!r}]",
            )
        terminal_exc: BaseException | None = None
        try:
            async for item in self.trigger_class.open_shared_stream(self.kwargs):
                if ack_required:
                    raw_event, broker_payload = item
                    # Snapshot the subscriber set at fan-out time.
                    snapshot = set(self._subscribers.keys()) - self._overflowed
                    if not snapshot:
                        # No subscribers to ack — fire-and-forget, consistent with the
                        # subscriber path (both use _schedule_advance).
                        self._schedule_advance(broker_payload)
                        continue
                    event_id = self._next_event_id
                    self._next_event_id += 1
                    self._outstanding[event_id] = _OutstandingEntry(
                        pending=snapshot.copy(),
                        created_at=self._now(),
                        broker_payload=broker_payload,
                    )
                    group_ref: weakref.ref[_SharedStreamGroup] = weakref.ref(self)
                    for trigger_id in snapshot:
                        queue = self._subscribers[trigger_id]
                        token = AckToken(event_id, trigger_id, group_ref)
                        try:
                            queue.put_nowait((raw_event, token))
                        except asyncio.QueueFull:
                            # Remove this subscriber from the outstanding entry
                            # before force-failing it so the entry does not wait
                            # for an ack that will never arrive.
                            entry = self._outstanding.get(event_id)
                            if entry is not None:
                                entry.pending.discard(trigger_id)
                            self._fail_overflowed_subscriber(trigger_id, queue)
                            if entry is not None and not entry.pending:
                                del self._outstanding[event_id]
                                self._schedule_advance(entry.broker_payload)
                else:
                    raw_event = item
                    for trigger_id, queue in self._subscribers.items():
                        if trigger_id in self._overflowed:
                            continue
                        try:
                            queue.put_nowait(raw_event)
                        except asyncio.QueueFull:
                            self._fail_overflowed_subscriber(trigger_id, queue)
            terminal_exc = _PollTerminated(
                f"open_shared_stream for {self.key!r} returned without raising; "
                "shared streams are expected to run for the lifetime of the group"
            )
        except asyncio.CancelledError:
            # ``stop()`` initiated this; the manager has already evicted the
            # group and is awaiting our exit. Do not run the terminate path.
            raise
        except Exception as exc:
            terminal_exc = exc
            self.log.exception("Shared stream poll failed; propagating to subscribers", key=self.key)
        finally:
            if self._ack_timeout_task is not None and not self._ack_timeout_task.done():
                self._ack_timeout_task.cancel()
                with suppress(asyncio.CancelledError):
                    await self._ack_timeout_task
            if terminal_exc is not None:
                # Synchronous: evict from the manager and broadcast the
                # sentinel before returning to the loop, so no coroutine can
                # observe ``_groups[key]`` pointing at a dead poll.
                self._on_poll_terminate(self)
                failure = _PollFailure(terminal_exc)
                for queue in self._subscribers.values():
                    # Drain stale events then put the failure sentinel so every
                    # subscriber wakes up even if its queue was at capacity.
                    self._drain_and_offer_failure(queue, failure)

    def _on_ack(self, event_id: int, trigger_id: int, *, nack: bool = False) -> None:
        """
        Record an ack (or nack) from a subscriber.

        Removes ``trigger_id`` from the pending set of ``event_id``. If the
        set empties, fires ``advance_shared_stream`` and clears the entry.
        Duplicate calls for the same (event_id, trigger_id) are no-ops.
        """
        entry = self._outstanding.get(event_id)
        if entry is None:
            return  # already advanced or never existed
        entry.pending.discard(trigger_id)
        if not entry.pending:
            del self._outstanding[event_id]
            self._schedule_advance(entry.broker_payload)

    async def _run_ack_timeout_loop(self) -> None:
        """
        Background task: force-fail subscribers whose ack is overdue.

        Cadence is ``max(0.01, ack_timeout / 10)`` — runs ten times per timeout
        window, floored at 10 ms to avoid burning CPU when ``ack_timeout`` is small.
        """
        cadence = max(0.01, self._ack_timeout / 10)
        while True:
            await asyncio.sleep(cadence)
            now = self._now()
            for event_id, entry in list(self._outstanding.items()):
                if now - entry.created_at < self._ack_timeout:
                    continue
                timed_out = set(entry.pending)
                for trigger_id in timed_out:
                    queue = self._subscribers.get(trigger_id)
                    if queue is not None:
                        self.log.warning(
                            "Ack timeout; force-failing subscriber",
                            key=self.key,
                            trigger_id=trigger_id,
                            event_id=event_id,
                        )
                        self._drain_and_offer_failure(
                            queue,
                            _PollFailure(
                                AckTimeout(
                                    f"shared stream {self.key!r} trigger {trigger_id} "
                                    f"did not ack event {event_id} within {self._ack_timeout}s"
                                )
                            ),
                        )
                        self._overflowed.add(trigger_id)
                    entry.pending.discard(trigger_id)
                if not entry.pending:
                    del self._outstanding[event_id]
                    self._schedule_advance(entry.broker_payload)

    def subscribe(self, trigger_id: int) -> AsyncIterator[Any]:
        """Register ``trigger_id`` as a subscriber and return its raw event stream."""
        if trigger_id in self._subscribers:
            raise RuntimeError(f"Trigger {trigger_id} already subscribed to shared stream {self.key!r}")
        queue: asyncio.Queue = asyncio.Queue(maxsize=self._max_subscriber_queue)
        self._subscribers[trigger_id] = queue
        return _drain(queue)

    def unsubscribe(self, trigger_id: int) -> None:
        # Active subscribers exit through their consuming task being cancelled
        # (Airflow's standard idiom); dropping the queue is enough here.
        self._subscribers.pop(trigger_id, None)
        self._overflowed.discard(trigger_id)
        # Remove from all outstanding entries — implicit ack-out to prevent
        # the producer waiting forever for a subscriber that has left.
        for event_id, entry in list(self._outstanding.items()):
            entry.pending.discard(trigger_id)
            if not entry.pending:
                del self._outstanding[event_id]
                self._schedule_advance(entry.broker_payload)

    def _fail_overflowed_subscriber(self, trigger_id: int, queue: asyncio.Queue) -> None:
        """
        Force a slow subscriber to fail with :class:`_SubscriberOverflow`.

        The broadcast hit ``QueueFull`` for this subscriber's queue, which
        means the subscriber's :meth:`filter_shared_stream` is falling behind
        the upstream cadence. Rather than dropping events silently — which
        would invisibly violate Asset event-driven semantics — we drain
        whatever stale events are pending and replace them with a
        :class:`_PollFailure` so the subscriber's ``run_trigger`` sees the
        error on its next ``__anext__``. Other subscribers in the same group
        are unaffected.
        """
        self.log.warning(
            "Shared stream subscriber overflowed; failing this trigger",
            key=self.key,
            trigger_id=trigger_id,
            queue_maxsize=queue.maxsize,
        )
        self._drain_and_offer_failure(
            queue,
            _PollFailure(
                _SubscriberOverflow(
                    f"shared stream {self.key!r} fell behind for trigger {trigger_id}: "
                    f"subscriber queue exceeded maxsize={queue.maxsize}"
                )
            ),
        )
        self._overflowed.add(trigger_id)

    def _drain_and_offer_failure(self, queue: asyncio.Queue, failure: _PollFailure) -> None:
        """
        Drain ``queue`` and put ``failure`` so the subscriber wakes on the failure.

        The drain releases capacity so the subsequent ``put_nowait`` cannot raise
        ``QueueFull``; this is the single point that both the terminal-broadcast
        and the per-subscriber overflow path go through.
        """
        while not queue.empty():
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        queue.put_nowait(failure)

    def is_empty(self) -> bool:
        return not self._subscribers

    async def stop(self) -> None:
        """Cancel poll and ack-timeout tasks if still running, then drain all in-flight advance tasks."""
        if self._ack_timeout_task is not None and not self._ack_timeout_task.done():
            self._ack_timeout_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._ack_timeout_task
        if self._poll_task is not None and not self._poll_task.done():
            self._poll_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._poll_task
        if self._advance_tasks:
            await asyncio.gather(*self._advance_tasks, return_exceptions=True)


class SharedStreamManager:
    """
    Coordinate :class:`BaseEventTrigger` instances that share underlying I/O.

    The manager owns one :class:`_SharedStreamGroup` per distinct
    ``shared_stream_key``. Each group runs a single async task that drives
    ``open_shared_stream``; subscribers receive raw events through their own
    asyncio queues and convert them to :class:`TriggerEvent` instances
    independently.

    The manager is single-event-loop and not thread-safe. The triggerer's
    ``TriggerRunner`` is its sole owner.
    """

    def __init__(
        self,
        *,
        log: BoundLogger | None = None,
        max_subscriber_queue: int = DEFAULT_SUBSCRIBER_QUEUE_MAX,
        ack_timeout: float = DEFAULT_ACK_TIMEOUT,
        _now: Callable[[], float] = time.monotonic,
    ) -> None:
        self.log = log or structlog.get_logger(__name__)
        self._max_subscriber_queue = max_subscriber_queue
        self._ack_timeout = ack_timeout
        self._now = _now
        self._groups: dict[Hashable, _SharedStreamGroup] = {}

    def subscribe(
        self,
        *,
        trigger_id: int,
        trigger: BaseEventTrigger,
        key: Hashable,
    ) -> AsyncIterator[Any]:
        """
        Subscribe a trigger to the shared stream identified by ``key``.

        On first subscriber for a given key the group is created and the
        underlying poll loop is started. Returns an async iterator of raw
        events the trigger should feed into ``filter_shared_stream``.
        """
        if key is None:
            raise ValueError("shared stream key must not be None")
        if (group := self._groups.get(key)) is None:
            _, kwargs = trigger.serialize()
            group = _SharedStreamGroup(
                key=key,
                trigger_class=type(trigger),
                kwargs=kwargs,
                on_poll_terminate=self._handle_poll_terminate,
                max_subscriber_queue=self._max_subscriber_queue,
                ack_timeout=self._ack_timeout,
                log=self.log,
                _now=self._now,
            )
            self._groups[key] = group
            group.start()
            self.log.debug("Shared stream group started", key=key)
        return group.subscribe(trigger_id)

    async def unsubscribe(self, trigger_id: int, key: Hashable) -> None:
        """
        Remove a subscriber.

        When the last subscriber for ``key`` leaves, the key is evicted from
        ``_groups`` synchronously and the underlying poll task is cancelled.
        Eviction happens *before* awaiting ``stop()`` so that a new subscriber
        arriving while we wait for cancellation builds a fresh group rather
        than attaching to the dying one.
        """
        group = self._groups.get(key)
        if group is None:
            return
        group.unsubscribe(trigger_id)
        if group.is_empty():
            del self._groups[key]
            await group.stop()
            self.log.debug("Shared stream group stopped", key=key)

    async def stop_all(self) -> None:
        """Cancel every active group; used during triggerer shutdown."""
        groups = list(self._groups.values())
        self._groups.clear()
        for group in groups:
            await group.stop()

    def _handle_poll_terminate(self, group: _SharedStreamGroup) -> None:
        """
        Evict a group synchronously when its poll task ends on its own.

        Invoked from ``_SharedStreamGroup._poll``'s ``finally`` before any
        ``await`` hands control to another coroutine, so the eviction races no
        ``subscribe`` call. The ``is`` check is defensive — under normal flow
        a group only enters this path while it is still the live entry for
        its key.
        """
        if self._groups.get(group.key) is group:
            del self._groups[group.key]
