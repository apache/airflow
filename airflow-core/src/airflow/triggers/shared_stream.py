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

Scope and the missing ack channel
---------------------------------

The shared-stream channel is **one-way**: events flow from
``open_shared_stream`` out to each subscriber's ``filter_shared_stream``,
with no path back. Subscribers cannot tell the producer "I accepted this
event; please advance / commit / ack". The pattern is therefore only safe
for upstreams whose consumption does not need a producer-side side effect
tied to a subscriber's accept / reject decision:

* Idempotent / read-only reads (filesystem listings, polling REST APIs).
* Auto-commit Kafka consumers (``enable.auto.commit=true``).
* Subscriber-side-effect cleanup (``unlink``, local marking, …) where the
  per-event action goes through APIs the subscriber owns independently.

Kafka manual-commit consumers, SQS delete-on-process / visibility
extension, and similar message-broker patterns where progress is per-message
and tied to the subscriber's decision are **not** in scope here today. A
producer-side ack channel to cover them is a follow-up that should be
designed against a concrete Kafka or SQS consumer rather than against an
abstract API. See :class:`~airflow.triggers.base.BaseEventTrigger` for the
matching subclass-facing notes.

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
from collections.abc import AsyncGenerator, AsyncIterator, Callable, Hashable
from contextlib import suppress
from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    from structlog.stdlib import BoundLogger

    from airflow.triggers.base import BaseEventTrigger

log = structlog.get_logger(__name__)

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
        log: BoundLogger,
    ) -> None:
        self.key = key
        self.trigger_class = trigger_class
        self.kwargs = kwargs
        self.log = log
        self._on_poll_terminate = on_poll_terminate
        self._max_subscriber_queue = max_subscriber_queue
        self._subscribers: dict[int, asyncio.Queue] = {}
        self._overflowed: set[int] = set()
        self._poll_task: asyncio.Task | None = None

    def start(self) -> None:
        """Start the underlying poll loop. Call exactly once per group."""
        if self._poll_task is not None:
            raise RuntimeError(f"Shared stream group {self.key!r} already started")
        self._poll_task = asyncio.create_task(
            self._poll(),
            name=f"shared-stream-poll[{self.key!r}]",
        )

    async def _poll(self) -> None:
        terminal_exc: BaseException | None = None
        try:
            async for raw_event in self.trigger_class.open_shared_stream(self.kwargs):
                for trigger_id, queue in self._subscribers.items():
                    if trigger_id in self._overflowed:
                        # Subscriber has been force-failed on a previous
                        # overflow; the failure sentinel is already in its
                        # queue and unsubscribe will drop it on next pass.
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
        """Cancel the poll task if it is still running and wait for it to exit."""
        if self._poll_task is None or self._poll_task.done():
            return
        self._poll_task.cancel()
        with suppress(asyncio.CancelledError):
            await self._poll_task


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
    ) -> None:
        self.log = log or structlog.get_logger(__name__)
        self._max_subscriber_queue = max_subscriber_queue
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
                log=self.log,
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
