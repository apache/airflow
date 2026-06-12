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
:meth:`~airflow.triggers.base.BaseEventTrigger.create_shared_stream_producer`,
the manager switches to **ack mode** for that stream.

The factory is called once per group and returns a
:class:`SharedStreamProducer` that owns the broker connection for the
lifetime of one poll. The manager drives the producer's ``open_stream``,
which yields ``(raw_event, broker_payload)`` tuples, and hands
``(raw_event, token)`` pairs — where ``token`` is an :class:`AckToken` — to
each subscriber's queue instead of the raw event alone. A subscriber calls
``await token.ack()`` once it has accepted the event, or
``await token.nack()`` to opt out. An ack completes the subscriber's
resolution only after the subscriber has moved past the event (pulled the
next raw event, or unsubscribed) and every
:class:`~airflow.triggers.base.TriggerEvent` it derived from the event has
been confirmed persisted to the metadata database; ``nack()`` and
force-failure resolve immediately. When every subscriber that was online
at broadcast time has resolved the event, the manager calls
``await producer.advance(broker_payload, outcome)`` exactly once — this is
where the producer commits / deletes / acks on the broker. The
:class:`AdvanceOutcome` carries per-event counts of how the subscribers
resolved. Advance calls are dispatched by a single pump task: the
producer's :meth:`~SharedStreamProducer.get_advance_lane` assigns each
event to a lane, events in the same lane are advanced strictly in fan-out
order, and events in different lanes do not wait for one another. At any
moment at most one ``advance`` call is awaited globally; the default lane
assignment (every event in the same lane) preserves the original single
global order. When the poll ends, the manager awaits ``producer.aclose()``
once, best-effort.

**Snapshot-at-fan-out**: the set of subscribers that must ack an event is
frozen at broadcast time. A subscriber that joins after the event was
broadcast is not added to that event's pending set.

**Persistence-gated advance**: the trigger events a subscriber derives
from a raw event are assigned sequence numbers as they leave the runner;
the supervisor confirms each one after the event is stored in the
metadata database, and the confirmation reaches the runner on the next
state sync (typically one sync round, a second or two — well within the
ack timeout). The subscriber's ack only completes once all of its
sequence numbers for the event are confirmed. If a confirmation never arrives — the persist
failed, or the triggerer crashed in between — the ack timeout fails the
event, the producer does not commit, and the broker redelivers. The
binding between a trigger event and the raw event it came from relies on
the filter yielding each derived event before pulling the next raw event
from the shared stream (the natural way to write a filter).

**Per-event ack timeout**: a background task scans outstanding events.
Any subscriber that has not acknowledged within ``ack_timeout`` seconds is
force-failed via the existing :class:`_PollFailure` path (exception type
:class:`AckTimeout`). The same timeout backstops persist confirmations
that never arrive. Other subscribers are not affected; once the
remaining acks arrive the producer advances normally.

**Triggerer restart**: outstanding acks are in-memory only. After a
triggerer restart, the broker will redeliver unacknowledged events.
Subscribers must therefore be idempotent. The same applies when a group
stops while events are still awaiting persist confirmation (for example,
the last subscriber unsubscribes right after producing an event): the
pending advances are abandoned and the broker redelivers those events.

**``shared_stream_subscriber_queue_size`` in ack mode**: the bound is
still "unprocessed raw events per subscriber". The manager does **not**
wait for outstanding acks before pulling the next event from the producer's
stream; back-pressure is purely queue-bound — a subscriber whose queue is
full is force-failed via :class:`_SubscriberOverflow`. The queue mainly
protects against burst delivery before a subscriber's filter has had a
chance to run. Broker advances, however, are dispatched by a single pump
task in per-lane order: while a lane's head event still has pending
subscribers, every later advance in that same lane waits (the ack timeout
is the backstop that bounds this per-lane head-of-line wait).

Triggers that do **not** override ``create_shared_stream_producer`` run the
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
import itertools
import time
import weakref
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import AsyncGenerator, AsyncIterator, Callable, Hashable, Iterable, Iterator
from contextlib import suppress
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

import structlog

from airflow.triggers.base import BaseEventTrigger

if TYPE_CHECKING:
    from structlog.stdlib import BoundLogger

log = structlog.get_logger(__name__)

__all__ = ["AckTimeout", "AckToken", "AdvanceOutcome", "SharedStreamManager", "SharedStreamProducer"]

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


@dataclass(frozen=True, slots=True)
class AdvanceOutcome:
    """
    Per-event resolution counts handed to :meth:`SharedStreamProducer.advance`.

    Every subscriber that was online when the event was broadcast is counted
    in exactly one field:

    * ``acked`` — called ``token.ack()`` (or unsubscribed while the event
      was outstanding — implicit ack-out) and every trigger event derived
      from the event was confirmed persisted.
    * ``nacked`` — called ``token.nack()``.
    * ``failed`` — force-failed by the manager (ack timeout — including a
      persist confirmation that never arrived — or queue overflow).

    An event broadcast while no subscribers were online carries all-zero
    counts and is clean.
    """

    acked: int
    nacked: int
    failed: int

    @property
    def is_clean(self) -> bool:
        """Whether every subscriber accepted the event (``nacked`` and ``failed`` are both zero)."""
        return self.nacked == 0 and self.failed == 0


class SharedStreamProducer(ABC):
    """
    Broker-side half of a shared stream running in ack mode.

    Returned by
    :meth:`~airflow.triggers.base.BaseEventTrigger.create_shared_stream_producer`;
    one instance owns the broker connection for the lifetime of one poll.
    """

    @abstractmethod
    def open_stream(self) -> AsyncIterator[tuple[Any, Any]]:
        """
        Open the broker connection and yield ``(raw_event, broker_payload)`` pairs.

        Implement as an async generator. ``broker_payload`` is any opaque
        object this producer needs later to advance the broker (e.g. a Kafka
        offset, SQS receipt handle, Pub/Sub ack ID). Called once per poll;
        open the broker connection here, not in the factory or ``__init__``.

        Implementations are expected to run for the lifetime of the group —
        returning without raising is treated as an error and propagated to
        every subscriber, so the contract is "yield forever, or raise".
        """

    @abstractmethod
    async def advance(self, broker_payload: Any, outcome: AdvanceOutcome) -> None:
        """
        Advance the broker for one fully resolved event.

        Within a lane — events for which :meth:`get_advance_lane` returned
        equal values — calls arrive strictly in fan-out order: the call for
        event N is awaited only after the call for event N-1 returned (or
        raised and was logged). Across lanes the relative order is not
        guaranteed, but at any moment at most one ``advance`` call is
        awaited globally. This makes cumulative schemes such as a Kafka
        offset commit safe within a lane. ``outcome`` carries the per-event
        resolution counts; use it to decide whether to commit, skip, or
        trigger a broker-side redeliver.
        """

    def get_advance_lane(self, broker_payload: Any) -> Hashable:
        """
        Return the advance lane for one event.

        Events whose lane values compare equal are advanced strictly in
        fan-out order relative to each other; events in different lanes do
        not wait for one another. At any moment at most one ``advance`` call
        is awaited globally, regardless of how many lanes exist. The default
        implementation puts every event in the same lane, which preserves the
        single global fan-out order.

        Called synchronously once per event before fan-out, so it must be
        cheap (O(1)) and must not block. If it raises, the whole poll is
        treated as failed: the group terminates and the error propagates to
        every subscriber.

        Example: a Kafka producer can return ``(topic, partition)`` here —
        a cumulative offset commit only needs ordering within a partition,
        so a slow partition no longer delays commits on the others.
        """
        return None

    async def aclose(self) -> None:
        """
        Release broker resources; called once when the poll ends.

        Best-effort: a raised exception is logged and not propagated. The
        default implementation does nothing.
        """


@dataclass(slots=True)
class _TokenBinding:
    """
    Per-(event, subscriber) resolution bookkeeping for one :class:`AckToken`.

    A subscriber resolves as ``acked`` only when all three hold:
    1. ``ack()`` was called (explicitly or implicitly on unsubscribe)
    2. the binding window is closed (the subscriber moved past the event)
    3. every trigger event seq the subscriber derived from the event has been confirmed persisted
    """

    acked: bool = False
    window_closed: bool = False
    unconfirmed_seqs: set[int] = field(default_factory=set)


@dataclass
class _OutstandingEntry:
    """State for one outstanding (broadcast-but-not-yet-advanced) event."""

    pending: set[int]  # trigger_ids still awaiting ack
    created_at: float  # monotonic clock at broadcast (via injectable _now)
    broker_payload: Any
    lane: Hashable  # producer.get_advance_lane(broker_payload), taken at fan-out
    # Per-subscriber ack bookkeeping, created at fan-out alongside ``pending``
    # and removed by ``_resolve_subscriber`` when the subscriber resolves.
    bindings: dict[int, _TokenBinding] = field(default_factory=dict)
    # Resolution counts; together they form the event's AdvanceOutcome.
    acked: int = 0
    nacked: int = 0
    failed: int = 0


class AckToken:
    """
    Handed to subscribers alongside each shared-stream event in ack mode.

    Call ``await token.ack()`` once the event has been accepted, or
    ``await token.nack()`` to opt out — the resolution is reported to the
    producer through the event's :class:`AdvanceOutcome`. Repeated calls are
    no-ops. After the group stops, calls silently do nothing.

    An ``ack()`` does not complete the subscriber's resolution on its own:
    the event counts as ``acked`` only after the subscriber has moved past it
    (pulled the next raw event or unsubscribed) and every trigger event the
    subscriber derived from it has been confirmed persisted to the metadata
    database. ``nack()`` resolves immediately.
    """

    __slots__ = ("_event_id", "_trigger_id", "_group_ref", "_resolved")

    def __init__(
        self,
        *,
        event_id: int,
        trigger_id: int,
        group_ref: weakref.ref[_SharedStreamGroup],
    ) -> None:
        self._event_id = event_id
        self._trigger_id = trigger_id
        self._group_ref = group_ref
        self._resolved = False

    def _resolve(self, resolution: Literal["acked", "nacked"]) -> None:
        """Mark the token resolved and report it to the owning group, once."""
        if self._resolved:
            return
        self._resolved = True
        group = self._group_ref()
        if group is None:
            return
        if resolution == "acked":
            group._record_ack(event_id=self._event_id, trigger_id=self._trigger_id)
        else:
            group._record_nack(event_id=self._event_id, trigger_id=self._trigger_id)

    async def ack(self) -> None:
        """
        Notify the producer that this subscriber has accepted the event.

        Declared ``async`` so the token API leaves room for awaitable
        implementations. The broker advance the ack feeds into may happen
        later: it waits until the trigger events derived from this event are
        persisted to the metadata database.
        """
        self._resolve("acked")

    async def nack(self) -> None:
        """
        Opt out of this event; counted as ``nacked`` in the event's :class:`AdvanceOutcome`.

        How the broker reacts is the producer's decision inside
        :meth:`SharedStreamProducer.advance` — commit anyway, skip, or
        trigger a redeliver. Declared ``async`` so the token API leaves room
        for awaitable implementations. A ``nack()`` resolves the subscriber
        immediately, without waiting for any persistence confirmation.
        """
        self._resolve("nacked")


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
        # Subscribers already force-failed (queue overflow or ack timeout);
        # excluded from subsequent broadcasts until they unsubscribe.
        self._failed_subscribers: set[int] = set()
        self._poll_task: asyncio.Task | None = None
        # Constant for the group's lifetime: trigger_class never changes.
        self._ack_required: bool = self._is_ack_required()
        # Ack mode state — populated only when create_shared_stream_producer
        # is overridden.
        self._outstanding: dict[int, _OutstandingEntry] = {}
        # Per-lane FIFO index over _outstanding: event ids in fan-out order,
        # keyed by the lane each event's producer assigned at fan-out.
        self._lane_queues: dict[Hashable, deque[int]] = {}
        # seq -> (event_id, trigger_id) for trigger events awaiting persist
        # confirmation; entries are removed on confirmation or when the
        # owning binding resolves (late confirmations then no-op).
        self._seq_index: dict[int, tuple[int, int]] = {}
        # Per subscriber: the token whose binding window is currently open —
        # trigger events the subscriber emits now bind to this token.
        self._current_token: dict[int, AckToken] = {}
        self._next_event_id: int = 0
        self._ack_timeout_task: asyncio.Task | None = None
        # Advance pump: a single task dispatches broker advances in per-lane
        # fan-out order, woken through this event whenever an entry resolves.
        self._advance_wakeup: asyncio.Event = asyncio.Event()
        self._pump_stopping: bool = False
        self._pump_task: asyncio.Task | None = None

    def start(self) -> None:
        """Start the underlying poll loop. Call exactly once per group."""
        if self._poll_task is not None:
            raise RuntimeError(f"Shared stream group {self.key!r} already started")
        self._poll_task = asyncio.create_task(
            self._poll(),
            name=f"shared-stream-poll[{self.key!r}]",
        )

    def _request_pump_stop(self) -> None:
        """
        Ask the advance pump to drain the already-resolved lane heads and exit.

        Synchronous (no await) so it can run inside ``_poll``'s terminal
        section without yielding.
        """
        self._pump_stopping = True
        self._advance_wakeup.set()

    async def _run_advance_pump(self, producer: SharedStreamProducer) -> None:
        """
        Dispatch broker advances in per-lane fan-out order, one at a time.

        A single task scans the lanes round-robin: each pass dispatches at
        most one resolved head per lane, and passes repeat until one makes
        no progress. An advance that raises is logged and the pump moves on.
        On stop the pump keeps passing until every already-resolved
        dispatchable event is drained, abandons unresolved entries to broker
        redelivery, and exits.
        """
        while True:
            if not self._pump_stopping:
                await self._advance_wakeup.wait()
            self._advance_wakeup.clear()
            progress = True
            while progress:
                progress = False
                # Snapshot: lanes are added (by _poll) while we await below.
                # A lane inserted during an await is invisible to this pass,
                # but its appearance always comes with a wakeup.set() (born
                # resolved) or a later resolve, so the next progress pass or
                # the next wakeup picks it up — nothing is lost. A no-progress
                # pass costs O(#lanes) and falls back to wait().
                for lane in list(self._lane_queues):
                    lane_queue = self._lane_queues[lane]
                    # The deque head is always present in _outstanding: entry
                    # deletion belongs to the pump alone, and it removes the
                    # deque slot and the dict entry together below — a
                    # KeyError here would be a bug.
                    head_id = lane_queue[0]
                    entry = self._outstanding[head_id]
                    if entry.pending:
                        continue
                    lane_queue.popleft()
                    del self._outstanding[head_id]
                    if not lane_queue:
                        # Sole lane GC point; _poll recreates the lane on demand.
                        del self._lane_queues[lane]
                    outcome = AdvanceOutcome(acked=entry.acked, nacked=entry.nacked, failed=entry.failed)
                    try:
                        await producer.advance(entry.broker_payload, outcome)
                    except Exception as exc:
                        self.log.error(
                            "Producer advance raised; broker advance failed",
                            key=self.key,
                            lane=lane,
                            exc_info=exc,
                        )
                    progress = True
            if self._pump_stopping:
                return

    def _is_ack_required(self) -> bool:
        # Check whether any class in the MRO (before BaseEventTrigger) defines
        # create_shared_stream_producer — i.e. the subclass has overridden it.
        for klass in self.trigger_class.__mro__:
            if klass is BaseEventTrigger:
                # Reached the base without finding an override — fast path.
                return False
            if "create_shared_stream_producer" in klass.__dict__:
                return True
        return False

    async def _poll(self) -> None:
        ack_required = self._ack_required
        producer: SharedStreamProducer | None = None
        terminal_exc: BaseException | None = None
        try:
            if ack_required:
                # A factory failure flows through the terminal broadcast
                # path below, like any other poll failure.
                producer = self.trigger_class.create_shared_stream_producer(self.kwargs)
                # Non-Optional alias for the fan-out section below; ``producer``
                # itself stays Optional for the ``finally`` aclose.
                ack_producer: SharedStreamProducer = producer
                self._pump_task = asyncio.create_task(
                    self._run_advance_pump(producer),
                    name=f"shared-stream-advance-pump[{self.key!r}]",
                )
                self._ack_timeout_task = asyncio.create_task(
                    self._run_ack_timeout_loop(),
                    name=f"shared-stream-ack-timeout[{self.key!r}]",
                )
                event_source: AsyncIterator[Any] = producer.open_stream()
            else:
                event_source = self.trigger_class.open_shared_stream(self.kwargs)
            async for item in event_source:
                if ack_required:
                    raw_event, broker_payload = item
                    # If get_advance_lane raises — or returns an unhashable
                    # lane, which the setdefault below trips on — the event
                    # has no entry yet and the exception flows through the
                    # terminal broadcast path below, like any other poll
                    # failure.
                    lane = ack_producer.get_advance_lane(broker_payload)
                    lane_queue = self._lane_queues.setdefault(lane, deque())
                    # Snapshot the subscriber set at fan-out time.
                    snapshot = set(self._subscribers.keys()) - self._failed_subscribers
                    event_id = self._next_event_id
                    self._next_event_id += 1
                    self._outstanding[event_id] = _OutstandingEntry(
                        pending=snapshot.copy(),
                        created_at=self._now(),
                        broker_payload=broker_payload,
                        lane=lane,
                        bindings={trigger_id: _TokenBinding() for trigger_id in snapshot},
                    )
                    lane_queue.append(event_id)
                    if not snapshot:
                        # No subscribers to ack — the entry is born resolved;
                        # route it through the pump so broker advances stay
                        # in per-lane fan-out order.
                        self._advance_wakeup.set()
                        continue
                    group_ref: weakref.ref[_SharedStreamGroup] = weakref.ref(self)
                    for trigger_id in snapshot:
                        queue = self._subscribers[trigger_id]
                        token = AckToken(event_id=event_id, trigger_id=trigger_id, group_ref=group_ref)
                        try:
                            queue.put_nowait((raw_event, token))
                        except asyncio.QueueFull:
                            self._fail_overflowed_subscriber(trigger_id, queue)
                            # The dead subscriber will never resolve anything
                            # it still owes; fail it out of every outstanding
                            # entry (including this one) so it cannot
                            # head-block the ordered advance pump.
                            self._fail_subscriber_in_outstanding(trigger_id)
                else:
                    raw_event = item
                    for trigger_id, queue in self._subscribers.items():
                        if trigger_id in self._failed_subscribers:
                            continue
                        try:
                            queue.put_nowait(raw_event)
                        except asyncio.QueueFull:
                            self._fail_overflowed_subscriber(trigger_id, queue)
            terminal_exc = _PollTerminated(
                f"shared stream for {self.key!r} returned without raising; "
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
            # Synchronous section — no yield until the terminal broadcast is
            # done (see "Lifecycle invariants" in the module docstring), so
            # no late subscriber can attach to the dead group. Awaiting the
            # cancelled ack-timeout task, the pump drain, and the producer
            # close are all deferred below the broadcast for the same reason.
            cancelled_ack_task: asyncio.Task | None = None
            if self._ack_timeout_task is not None and not self._ack_timeout_task.done():
                self._ack_timeout_task.cancel()
                cancelled_ack_task = self._ack_timeout_task
            # Synchronous: flags the pump to drain its resolved lane heads and exit.
            self._request_pump_stop()
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
            # End of synchronous section; yields are safe from here on.
            if cancelled_ack_task is not None:
                with suppress(asyncio.CancelledError):
                    await cancelled_ack_task
            if self._pump_task is not None:
                # The pump exits by itself once it has dispatched everything
                # already resolved and dispatchable; the suppress is defensive.
                with suppress(asyncio.CancelledError):
                    await self._pump_task
            if producer is not None:
                try:
                    await producer.aclose()
                except Exception as exc:
                    self.log.warning("Producer aclose failed", key=self.key, exc_info=exc)

    def _resolve_subscriber(
        self,
        *,
        event_id: int,
        trigger_id: int,
        resolution: Literal["acked", "nacked", "failed"],
    ) -> None:
        """
        Record one subscriber's resolution of one event.

        Removes ``trigger_id`` from the pending set of ``event_id`` and adds
        it to the matching outcome count. If the set empties, wakes the
        advance pump — deleting the entry and dispatching the broker advance
        are the pump's job, so advances stay in per-lane fan-out order. Duplicate
        calls for the same (event_id, trigger_id) are no-ops, which keeps
        every subscriber counted exactly once per event.

        This is the single cleanup point for the subscriber's ack
        bookkeeping: the binding is popped and its unconfirmed seqs leave
        ``_seq_index``, so a persist confirmation arriving after the fact
        (e.g. after an ack timeout already failed the subscriber) finds
        nothing and no-ops.
        """
        entry = self._outstanding.get(event_id)
        if entry is None or trigger_id not in entry.pending:
            return  # already resolved, already advanced, or never existed
        entry.pending.discard(trigger_id)
        binding = entry.bindings.pop(trigger_id, None)
        if binding is not None:
            for seq in binding.unconfirmed_seqs:
                self._seq_index.pop(seq, None)
        if resolution == "acked":
            entry.acked += 1
        elif resolution == "nacked":
            entry.nacked += 1
        else:
            entry.failed += 1
        if not entry.pending:
            self._advance_wakeup.set()

    def _record_ack(self, *, event_id: int, trigger_id: int) -> None:
        """
        Record that the subscriber called ``ack()`` for one event.

        The subscriber resolves as acked only once the binding window is
        closed and every bound seq is confirmed persisted; until then the
        event stays outstanding (the ack timeout is the backstop).
        """
        entry = self._outstanding.get(event_id)
        if entry is None:
            return
        binding = entry.bindings.get(trigger_id)
        if binding is None:
            return  # already resolved (e.g. force-failed before the ack landed)
        binding.acked = True
        self._maybe_complete(event_id=event_id, trigger_id=trigger_id)

    def _record_nack(self, *, event_id: int, trigger_id: int) -> None:
        """Resolve the subscriber as nacked immediately; no persistence gating applies."""
        self._resolve_subscriber(event_id=event_id, trigger_id=trigger_id, resolution="nacked")

    def _maybe_complete(self, *, event_id: int, trigger_id: int) -> None:
        """Resolve the subscriber as acked once ack, window close, and all persist confirmations are in."""
        entry = self._outstanding.get(event_id)
        if entry is None:
            return
        binding = entry.bindings.get(trigger_id)
        if binding is None:
            return
        if binding.acked and binding.window_closed and not binding.unconfirmed_seqs:
            self._resolve_subscriber(event_id=event_id, trigger_id=trigger_id, resolution="acked")

    def _close_binding_window(self, token: AckToken) -> None:
        """
        Close the binding window of one yielded token.

        Called by ``_ack_drain`` the moment the subscriber pulls the next
        item — trigger events the subscriber emits from here on belong to
        the next token, and an ack recorded for this one can now complete
        (subject to persist confirmations).
        """
        trigger_id = token._trigger_id
        if self._current_token.get(trigger_id) is token:
            del self._current_token[trigger_id]
        entry = self._outstanding.get(token._event_id)
        if entry is None:
            return
        binding = entry.bindings.get(trigger_id)
        if binding is None:
            return
        binding.window_closed = True
        self._maybe_complete(event_id=token._event_id, trigger_id=trigger_id)

    def bind_pending_event(self, *, trigger_id: int, seq_counter: Iterator[int]) -> int | None:
        """
        Bind one just-emitted trigger event to the subscriber's open binding window.

        Returns the seq the broker advance must wait on, or ``None`` when
        there is nothing to bind to — no window open (not in ack mode, or
        the event was emitted outside any raw event's window) or the
        binding already resolved (nacked or force-failed). A seq is only
        drawn from ``seq_counter`` when the event actually binds.
        """
        token = self._current_token.get(trigger_id)
        if token is None:
            return None
        entry = self._outstanding.get(token._event_id)
        if entry is None:
            return None
        binding = entry.bindings.get(trigger_id)
        if binding is None:
            return None
        seq = next(seq_counter)
        binding.unconfirmed_seqs.add(seq)
        self._seq_index[seq] = (token._event_id, trigger_id)
        return seq

    def confirm_persisted(self, seqs: Iterable[int]) -> None:
        """
        Record persist confirmations for trigger event seqs bound in this group.

        Seqs that are not (or no longer) in ``_seq_index`` — another group's
        seqs, or bindings that already resolved through nack / timeout —
        are ignored.
        """
        for seq in seqs:
            bound = self._seq_index.pop(seq, None)
            if bound is None:
                continue
            event_id, trigger_id = bound
            entry = self._outstanding.get(event_id)
            if entry is None:
                continue
            binding = entry.bindings.get(trigger_id)
            if binding is None:
                continue
            binding.unconfirmed_seqs.discard(seq)
            self._maybe_complete(event_id=event_id, trigger_id=trigger_id)

    def _fail_subscriber_in_outstanding(self, trigger_id: int) -> None:
        """
        Resolve ``trigger_id`` as failed in every outstanding entry.

        A force-failed subscriber (queue overflow or ack timeout) will never
        resolve the events it still owes; leaving it pending in older entries
        would head-block the ordered advance pump until each entry's own ack
        timeout. This is the retroactive counterpart of excluding the
        subscriber from future broadcasts via ``_failed_subscribers``.
        """
        for event_id in list(self._outstanding):
            self._resolve_subscriber(event_id=event_id, trigger_id=trigger_id, resolution="failed")

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
                        self._failed_subscribers.add(trigger_id)
                    # Fail the subscriber out of every outstanding entry (not
                    # just this one); the resolution wakes the pump, which
                    # owns entry deletion and the ordered broker advance.
                    self._fail_subscriber_in_outstanding(trigger_id)

    async def _ack_drain(self, trigger_id: int, queue: asyncio.Queue) -> AsyncGenerator[Any, None]:
        """
        Ack-mode counterpart of :func:`_drain`, tracking the binding window.

        Between yielding ``(raw_event, token)`` and the subscriber pulling
        the next item, the token's binding window is open: trigger events
        the subscriber emits bind to it via :meth:`bind_pending_event`. The
        window closes the moment the subscriber resumes this generator —
        before we wait on the queue again — so a filtered-out event (acked,
        nothing yielded) resolves as soon as the filter loops back for the
        next raw event.
        """
        while True:
            item = await queue.get()
            if isinstance(item, _PollFailure):
                raise item.exc
            _, token = item
            self._current_token[trigger_id] = token
            yield item
            self._close_binding_window(token)

    def subscribe(self, trigger_id: int) -> AsyncIterator[Any]:
        """Register ``trigger_id`` as a subscriber and return its raw event stream."""
        if trigger_id in self._subscribers:
            raise RuntimeError(f"Trigger {trigger_id} already subscribed to shared stream {self.key!r}")
        queue: asyncio.Queue = asyncio.Queue(maxsize=self._max_subscriber_queue)
        self._subscribers[trigger_id] = queue
        if self._ack_required:
            return self._ack_drain(trigger_id, queue)
        return _drain(queue)

    def unsubscribe(self, trigger_id: int) -> None:
        # Active subscribers exit through their consuming task being cancelled
        # (Airflow's standard idiom); dropping the queue is enough here.
        self._subscribers.pop(trigger_id, None)
        self._failed_subscribers.discard(trigger_id)
        self._current_token.pop(trigger_id, None)
        # Implicit ack-out: leaving counts as accepting every outstanding
        # event, so the producer never waits forever for a subscriber that
        # has left. The broker advance still waits for persist confirmation
        # of any trigger events this subscriber derived from the event; with
        # nothing unconfirmed the subscriber resolves immediately.
        for event_id, entry in list(self._outstanding.items()):
            binding = entry.bindings.get(trigger_id)
            if binding is None:
                # No live binding (already resolved, or pre-ack-mode entry);
                # _resolve_subscriber no-ops unless still pending.
                self._resolve_subscriber(event_id=event_id, trigger_id=trigger_id, resolution="acked")
                continue
            binding.acked = True
            binding.window_closed = True
            self._maybe_complete(event_id=event_id, trigger_id=trigger_id)

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
        self._failed_subscribers.add(trigger_id)

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
        """
        Cancel the poll and ack-timeout tasks if still running.

        The poll's ``finally`` drains the advance pump (in-flight and
        already-resolved advances complete; unresolved events are abandoned
        to broker redelivery) and closes the producer.
        """
        if self._ack_timeout_task is not None and not self._ack_timeout_task.done():
            self._ack_timeout_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._ack_timeout_task
        if self._poll_task is not None and not self._poll_task.done():
            self._poll_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._poll_task
        # Defensive: if the poll never reached its finally, make sure the
        # pump still drains and exits.
        if self._pump_task is not None and not self._pump_task.done():
            self._request_pump_stop()
            with suppress(asyncio.CancelledError):
                await self._pump_task


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
        # Allocator for trigger-event persist-confirmation seqs; unique per
        # manager (= per runner process), shared across groups. Gaps are fine.
        self._seq_counter: Iterator[int] = itertools.count()

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

    def bind_pending_event(self, *, trigger_id: int, key: Hashable) -> int | None:
        """
        Bind a trigger event the subscriber just emitted to its shared-stream ack state.

        Returns the persist-confirmation seq the runner must report through
        :meth:`confirm_persisted` once the event is stored, or ``None`` when
        there is nothing to gate — the group is gone, the stream is not in
        ack mode, or the subscriber has no open binding window. Synchronous
        and O(1); call it between taking the event off the trigger and
        queueing it outbound, with no ``await`` in between.
        """
        if (group := self._groups.get(key)) is None:
            return None
        return group.bind_pending_event(trigger_id=trigger_id, seq_counter=self._seq_counter)

    def confirm_persisted(self, seqs: Iterable[int]) -> None:
        """
        Record that the trigger events behind ``seqs`` were persisted.

        Broadcast to every live group; each group resolves the sequence
        numbers it owns and ignores the rest. Sequence numbers whose binding
        already resolved (``nack()``, timeout) or whose group has stopped
        are ignored.
        """
        # Materialize so a one-shot iterator is not exhausted by the first group.
        seq_list = list(seqs)
        for group in self._groups.values():
            group.confirm_persisted(seq_list)

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
