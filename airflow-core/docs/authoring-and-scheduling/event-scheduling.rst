 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Event-driven scheduling
=======================

.. versionadded:: 3.0

Apache Airflow allows for event-driven scheduling, enabling Dags to be triggered based on external events rather than
predefined time-based schedules.
This is particularly useful in modern data architectures where workflows need to react to real-time data changes,
messages, or system signals.

By using assets, as described in :doc:`asset-scheduling`, you can configure Dags to start execution when specific external events
occur. Assets provide a mechanism to establish dependencies between external events and Dag execution, ensuring that
workflows react dynamically to changes in the external environment.

The ``AssetWatcher`` class plays a crucial role in this mechanism. It monitors an external event source, such as a
message queue, and triggers an asset update when a relevant event occurs.
The ``watchers`` parameter in the ``Asset`` definition allows you to associate multiple ``AssetWatcher`` instances with an
asset, enabling it to respond to various event sources.

See the :doc:`common.messaging provider docs <apache-airflow-providers-common-messaging:triggers>` for more information and examples.

Supported triggers for event-driven scheduling
----------------------------------------------
Not all :doc:`triggers <deferring>` in Airflow can be used for event-driven scheduling. As opposed to all triggers that
inherit from ``BaseTrigger``, only a subset that inherit from ``BaseEventTrigger`` are compatible.
The reason for this restriction is that some triggers are not designed for event-driven scheduling, and using them to
schedule Dags could lead to unintended results.

``BaseEventTrigger`` ensures that triggers used for scheduling adhere to an event-driven paradigm, reacting appropriately
to external event changes without causing unexpected Dag behavior.

Writing event-driven compatible triggers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To make a trigger compatible with event-driven scheduling, it must inherit from ``BaseEventTrigger``. There are three
main scenarios for working with triggers in this context:

1. **Creating a new event-driven trigger**: If you need a new trigger for an unsupported event source, you should create
a new class inheriting from ``BaseEventTrigger`` and implement its logic.

2. **Adapting an existing compatible trigger**: If an existing trigger (inheriting from ``BaseTrigger``) is proven to be
already compatible with event-driven scheduling, then you just need to change the base class from ``BaseTrigger`` to
``BaseEventTrigger``.

3. **Adapting an existing incompatible trigger**: If an existing trigger does not appear to be compatible with
event-driven scheduling, then a new trigger must be created.
This new trigger must inherit ``BaseEventTrigger`` and ensure it properly works with event-driven scheduling.
It might inherit from the existing trigger as well if both triggers share some common code.

Sharing one poll across sibling triggers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. versionadded:: 3.3

When several ``AssetWatcher`` instances on different assets back triggers that read from the **same upstream resource**
— a directory of flag files, a polling REST endpoint, and similar idempotent or
subscriber-side-effect sources — the triggerer would otherwise spin up one independent poll loop per trigger. For a
shared source with twenty subscribers that means twenty poll loops, twenty connections, twenty sets of API calls per
cadence. See "Suitable upstreams" below for the precise scope.

``BaseEventTrigger`` supports an opt-in path so that sibling triggers share a single underlying poll, while each
trigger keeps its own DB row, its own ``run_trigger`` task, and its own per-instance filtering. To participate, a
subclass overrides three hooks:

* :meth:`~airflow.triggers.base.BaseEventTrigger.shared_stream_key` — return a key identifying the shared
  upstream (typically a tuple of strings). Triggers whose key compares equal will share one poll. Returning ``None``
  (the default) opts out — the trigger runs its own independent ``run()`` loop, exactly as before. The return value
  is read **once** when the triggerer starts this trigger; changing it mid-lifetime has no effect on group
  membership, so siblings that should share a poll must return the same key from the outset.
  The key must be deterministic — derive it from configuration fields, never from per-call values such as
  ``time.time()`` or ``uuid.uuid4()``, because the comparison must be stable across the lifetime of the group.

* :meth:`~airflow.triggers.base.BaseEventTrigger.open_shared_stream` — a ``@classmethod`` coroutine the triggerer
  drives **once per shared-stream group** to yield raw events from the upstream. Because the triggerer reuses one
  trigger's kwargs to drive the shared poll, only rely on fields whose values participate in ``shared_stream_key``.

* :meth:`~airflow.triggers.base.BaseEventTrigger.filter_shared_stream` — an instance method that consumes the
  broadcast raw stream and yields the ``TriggerEvent`` instances this trigger should fire. Per-trigger filtering
  (e.g. only events matching this instance's ``filename``) lives here.

Example: a ``DirectoryFileDeleteTrigger`` that fires when a per-asset flag file appears in a shared inbox directory:

.. code-block:: python

    from collections.abc import AsyncIterator, Hashable
    from typing import Any

    from airflow.triggers.base import BaseEventTrigger, TriggerEvent


    class DirectoryFileDeleteTrigger(BaseEventTrigger):
        def __init__(self, *, directory, filename, poke_interval=5.0):
            super().__init__()
            self.directory = directory
            self.filename = filename
            self.poke_interval = poke_interval

        def shared_stream_key(self) -> Hashable | None:
            # All triggers on the same directory + cadence share one scan.
            return ("directory-scan", self.directory, self.poke_interval)

        @classmethod
        async def open_shared_stream(cls, kwargs: dict[str, Any]) -> AsyncIterator[Any]:
            # Drives one directory listing loop per group.
            ...

        async def filter_shared_stream(self, shared_stream: AsyncIterator[Any]) -> AsyncIterator[TriggerEvent]:
            # Each instance fires only for its own filename.
            async for snapshot in shared_stream:
                if self.filename in snapshot["names"]:
                    yield TriggerEvent(...)
                    return

A complete example using this trigger ships in
``airflow.example_dags.example_asset_with_watchers``, where two sibling
``DirectoryFileDeleteTrigger`` watchers share one directory scan alongside
a standalone ``FileDeleteTrigger`` watcher in the same Dag.

What is and isn't shared
^^^^^^^^^^^^^^^^^^^^^^^^

The sharing is narrower than the name might suggest:

* **Shared** (one per ``shared_stream_key``): the ``open_shared_stream`` async generator and its upstream I/O — for
  example, the actual ``iterdir`` calls on the directory or polling REST API calls.

* **Not shared** (one per trigger): the ``Trigger`` DB row, the trigger instance, the ``run_trigger``
  asyncio task, and the ``filter_shared_stream`` async generator. Each ``AssetWatcher`` still appears as its own
  trigger in the UI and in the metadata database.

In other words, the savings is at the poll-loop and upstream-I/O layer, not at the persistence or scheduling layer.

Suitable upstreams
^^^^^^^^^^^^^^^^^^

Good fits for the shared-stream pattern:

* Idempotent / read-only reads — directory scans, polling REST APIs.
* Subscriber-side-effect cleanup, where the trigger's per-event action
  (``unlink``, local marking, …) goes through APIs the subscriber owns
  independently of the shared producer handle.
* Message-broker upstreams (Kafka, SQS, Pub/Sub, Azure Service Bus) where
  the producer must commit/delete/ack after all subscribers have processed
  the message — use the ack channel described below.

Producer-side ack channel
^^^^^^^^^^^^^^^^^^^^^^^^^

For upstreams where the producer must advance (commit, delete, or ack) only
after all subscribers have processed an event, override
:meth:`~airflow.triggers.base.BaseEventTrigger.create_shared_stream_producer`
to return a :class:`~airflow.triggers.shared_stream.SharedStreamProducer`.
When this factory is overridden, the manager enters **ack mode**:

1. The manager calls ``create_shared_stream_producer(kwargs)`` once per
   shared-stream group. The returned producer owns the broker connection for
   the lifetime of one poll; open the connection lazily inside
   ``open_stream``, not in the factory.
2. The producer's ``open_stream`` yields ``(event, broker_payload)`` tuples,
   where ``broker_payload`` is whatever the producer needs later (e.g. an
   SQS receipt handle, a Kafka offset, a Pub/Sub ack ID).
3. Each subscriber's ``filter_shared_stream`` receives ``(event, token)``
   pairs. The subscriber calls ``await token.ack()`` once it has accepted
   the event, or ``await token.nack()`` to opt out.
4. Once every subscriber in the fan-out set has resolved an event (by
   ``ack()``, ``nack()``, unsubscribing, timing out, or overflowing its
   queue) **and** every ``TriggerEvent`` the subscribers derived from it
   has been persisted to the metadata database, the manager calls
   ``await producer.advance(broker_payload, outcome)`` — commit the offset,
   delete the SQS message, etc. The ``outcome`` is an
   :class:`~airflow.triggers.shared_stream.AdvanceOutcome` carrying
   per-event counts of how the subscribers resolved.

**Ordering guarantee**: by default every event belongs to the same lane,
so advance calls are dispatched strictly in event order — ``advance`` for
event N is awaited only after event N-1's ``advance`` returned (or failed
and was logged). A producer can override ``get_advance_lane`` to narrow
that ordering to within a lane: events whose lane values compare equal
are advanced in event order relative to each other, while events in
different lanes do not wait for one another. Either way, at most one
``advance`` call is awaited at a time. When the poll ends, the manager
calls ``await producer.aclose()`` once, best-effort.

Example — SQS-like producer:

.. code-block:: python

    from collections.abc import AsyncIterator
    from typing import Any

    from airflow.triggers.base import BaseEventTrigger, TriggerEvent
    from airflow.triggers.shared_stream import AdvanceOutcome, SharedStreamProducer


    class SqsSharedStreamProducer(SharedStreamProducer):
        def __init__(self, queue_url: str):
            self.queue_url = queue_url
            self.client = None

        async def open_stream(self) -> AsyncIterator[tuple[Any, Any]]:
            # Open the connection here, not in the trigger's factory.
            self.client = await create_sqs_client()
            while True:
                messages = await poll_sqs(self.client, self.queue_url)
                for msg in messages:
                    yield msg["Body"], msg["ReceiptHandle"]

        async def advance(self, broker_payload: Any, outcome: AdvanceOutcome) -> None:
            # Called once all subscribers have resolved this message.
            if outcome.is_clean:
                await delete_sqs_message(self.client, self.queue_url, broker_payload)
            # Otherwise leave the message for the visibility timeout to redeliver.

        async def aclose(self) -> None:
            if self.client is not None:
                await self.client.close()


    class SqsSharedTrigger(BaseEventTrigger):
        def __init__(self, *, queue_url: str, region: str | None = None):
            super().__init__()
            self.queue_url = queue_url
            self.region = region

        def serialize(self):
            return (
                f"{type(self).__module__}.{type(self).__qualname__}",
                {"queue_url": self.queue_url, "region": self.region},
            )

        def shared_stream_key(self):
            return ("sqs", self.queue_url)

        @classmethod
        def create_shared_stream_producer(cls, kwargs) -> SqsSharedStreamProducer:
            return SqsSharedStreamProducer(kwargs["queue_url"])

        async def filter_shared_stream(self, shared_stream):
            async for raw, token in shared_stream:
                if self.region is None or raw.get("region") == self.region:
                    await token.ack()
                    yield TriggerEvent(raw)
                else:
                    await token.nack()

        async def run(self):  # pragma: no cover
            yield TriggerEvent({})

Example — Kafka cumulative commit across partitions. A Kafka commit
acknowledges every offset up to the committed one within a partition, so
it is only safe if no later event from the same partition can be committed
while an earlier one is still pending — events on other partitions do not
matter. Returning ``(topic, partition)`` from ``get_advance_lane`` narrows
the ordering guarantee to exactly that granularity: each partition's
commits stay in order, and a slow partition no longer delays commits on
the other partitions:

.. code-block:: python

    class KafkaSharedStreamProducer(SharedStreamProducer):
        def __init__(self, topics: list[str]):
            self.topics = topics
            self.consumer = None

        async def open_stream(self):
            self.consumer = await create_kafka_consumer(self.topics)
            async for message in self.consumer:
                yield message.value, (message.topic, message.partition, message.offset)

        def get_advance_lane(self, broker_payload):
            topic, partition, _offset = broker_payload
            return topic, partition

        async def advance(self, broker_payload, outcome):
            topic, partition, offset = broker_payload
            # Within one lane — here, one partition — advance calls arrive in
            # event order, so this cumulative commit can never skip past an
            # event that is still pending.
            await self.consumer.commit(topic, partition, offset + 1)

        async def aclose(self):
            if self.consumer is not None:
                await self.consumer.stop()

The order of ``ack()`` relative to ``yield`` does not affect correctness:
an ack completes only after the subscriber has moved past the event and
the trigger events it derived from it were persisted, so nothing depends
on code running after the ``yield``. The one constraint on filter authors
is the binding between raw events and the trigger events derived from
them: yield every ``TriggerEvent`` derived from a raw event before pulling
the next raw event from the shared stream — which is what a
straightforward filter loop does anyway.

**Snapshot-at-fan-out**: the set of subscribers that must ack a given event
is frozen at the moment the event is broadcast. A subscriber that joins
after the event was dispatched is not added to that event's pending set.

**Per-event ack timeout**: if a subscriber has not called ``ack()`` or
``nack()`` within the ack timeout (default 5 minutes, configurable via the
``[triggerer] shared_stream_ack_timeout`` config option), the manager force-fails that
subscriber's trigger. The same timeout covers an ack that is waiting on a
persistence confirmation that never arrives. Other subscribers are not
affected; once their acks arrive, the producer advances normally. The ack
timeout is a manager-level
safety net and does not replace any native broker session or visibility timeout.
From the subscriber's perspective the force-fail surfaces as an ``AckTimeout``
(importable from ``airflow.triggers.shared_stream``) raised by the
``shared_stream`` iterator inside ``filter_shared_stream``. Letting it propagate
is fine — the trigger fails through the standard trigger-failure path; catch it
only if the subscriber needs to run cleanup before failing.

**Triggerer restart**: outstanding acks live in memory only. After a
triggerer restart, the broker redelivers messages that were not yet
acknowledged. Subscribers must therefore be idempotent.

**Durability**: the broker advance is gated on persistence. A subscriber's
``ack()`` completes only after every ``TriggerEvent`` it derived from the
event has been stored in the metadata database; the confirmation reaches
the trigger runner on the next state sync, typically within a second or
two. If the confirmation never arrives — the triggerer crashed, or the
event could not be persisted — the ack timeout fails the event, the
producer does not commit, and the broker redelivers. A failure can
therefore cause duplicate delivery but never a lost event; idempotent
subscribers absorb the duplicates. The same trade-off applies when a
group stops while events are still awaiting confirmation (for example,
the last subscriber unsubscribes right after producing an event): the
pending advances are abandoned and the broker redelivers those events.

**nack() semantics**: ``nack()`` removes the subscriber from the pending
ack set and is counted in the ``nacked`` field of the event's
``AdvanceOutcome``. What that means for the broker is the producer's
decision inside ``advance`` — commit anyway, skip the commit, or trigger an
immediate redeliver (e.g. ``ChangeMessageVisibility(0)`` for SQS).

**``shared_stream_subscriber_queue_size`` in ack mode**: the config bound
still governs unprocessed raw events per subscriber. The manager does
**not** wait for outstanding acks before pulling the next upstream event;
back-pressure is queue-bound — a subscriber whose queue is full is
force-failed. The queue primarily guards against burst delivery before a
subscriber's filter runs. Broker advances, by contrast, are dispatched in
order within each lane: an event whose acks are still pending delays the
advance of every later event in the same lane, with the ack timeout
bounding that wait.

Verifying that sharing is active
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The triggerer logs the creation of each shared-stream group, and names the poll task after its key:

.. code-block:: text

    Shared stream group started key=('directory-scan', '/tmp/region-flags', 5.0)

.. code-block:: text

    asyncio task name: shared-stream-poll[('directory-scan', '/tmp/region-flags', 5.0)]

If sharing is active you should see exactly one ``Shared stream group started`` line per distinct key, regardless of
how many subscribers join it. If you see one log line per subscriber instead, the keys probably do not compare equal
— verify that ``shared_stream_key`` returns identical values across the siblings.

Slow-subscriber overflow
^^^^^^^^^^^^^^^^^^^^^^^^

Each subscriber in a shared-stream group has a bounded in-memory queue. If the poll loop
produces events faster than a subscriber's ``filter_shared_stream`` can consume them, the
queue fills and that trigger is failed with ``_SubscriberOverflow`` — a deliberate fail-fast
rather than unbounded memory growth.

If subscribers repeatedly overflow, there are two ways to address this:

* Raise ``[triggerer] shared_stream_subscriber_queue_size`` to give the
  filter more slack before the overflow threshold is reached.
* Redesign :meth:`~airflow.triggers.base.BaseEventTrigger.shared_stream_key` so fewer
  sibling triggers share a single group — a narrower group reduces the rate at which any
  one subscriber needs to consume events.

Both reduce the mismatch between producer throughput and per-subscriber consume rate.

Avoid infinite scheduling
~~~~~~~~~~~~~~~~~~~~~~~~~

The reason why some triggers are not compatible with event-driven scheduling is that they are waiting
for an external resource to reach a given state. Examples:

* Wait for a file to exist in a storage service
* Wait for a job to be in a success state
* Wait for a row to be present in a database

Scheduling under such conditions can lead to infinite rescheduling. This is because once the condition becomes true,
it is likely to remain true for an extended period.

For example, consider a Dag scheduled to run when a specific job reaches a "success" state.
Once the job succeeds, it will typically remain in that state. As a result, the Dag will be triggered repeatedly every
time the triggerer checks the condition.

Another example is the ``S3KeyTrigger``, which checks for the presence of a specific file in an S3 bucket.
Once the file is created, the trigger will continue to succeed on every check, since the condition
"is file X present in bucket Y" remains true.
This leads to the Dag being triggered indefinitely every time the trigger mechanism runs.

When creating custom triggers, be cautious about using conditions that remain permanently true once met.
This can unintentionally result in infinite Dag executions and overwhelm your system.

Use cases for event-driven Dags
-------------------------------

* **Data ingestion pipelines**: Trigger ETL workflows when new data arrives in a storage system.

* **Machine learning workflows**: Start training models when new datasets become available.

* **IoT and real-time analytics**: React to sensor data, logs, or application events in real-time.

* **Microservices and event-driven architectures**: Orchestrate workflows based on service-to-service messages.
